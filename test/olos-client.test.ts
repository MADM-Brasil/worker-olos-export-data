import sql from 'mssql';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { BlockedMssqlTransport, OlosProcedureClient, prepareCall, prepareMssqlRequest } from '../src/olos/client.js';
import { procedures, tableContracts, type Procedure } from '../src/olos/contracts.js';
import { OlosError, type ProcedureResponse } from '../src/olos/types.js';
import { signature, dataset, record, response, transport } from './support/olos.js';

const signal = () => new AbortController().signal;
const values = { FixtureTable: 'AttemptsRawData', FixtureAfter: '9007199254740993' };
afterEach(() => { vi.useRealTimers(); vi.restoreAllMocks(); });
describe('contratos e cliente tipado Olos com transporte sintético', () => {
  it.each(Object.keys(tableContracts) as (keyof typeof tableContracts)[])('contrato individual %s não inventa layout ou tipos', table => {
    const c = tableContracts[table];
    expect(c.version).toBe(`${table}/v1`);
    expect(c.approved).toBe(false);
    expect(c.fields[c.documentedKey]).toEqual({ nameConfirmed: true, sqlType: null, nullable: null });
    expect(Object.keys(c.fields)).not.toContain('Password');
  });
  it.each(Object.keys(procedures) as Procedure[])('bloqueia execução real de %s antes de conectar', async procedure => {
    const native = new BlockedMssqlTransport(), connect = vi.spyOn(native, 'connect');
    await expect(new OlosProcedureClient(native).call(signature(procedure), values, signal())).rejects.toMatchObject({ code: 'REAL_EXECUTION_BLOCKED', outcome: 'not_sent' });
    expect(connect).not.toHaveBeenCalled();
  });
  it.each(['DataAvailable','ResetReadData'] as Procedure[])('bloqueia %s inclusive no transporte de teste', async procedure => {
    const fake = transport();
    await expect(new OlosProcedureClient(fake).call(signature(procedure), values, signal())).rejects.toMatchObject({ code: 'PROCEDURE_BLOCKED' });
    expect(fake.connect).not.toHaveBeenCalled();
  });
  it.each(['ReadData','ReadDataCurrentDay','ReadDataPreviousDays','LogReadData','ReReadData'] as Procedure[])('captura datasets e OUTPUTs sintéticos de %s', async procedure => {
    const fake = transport(), client = new OlosProcedureClient(fake);
    const reply = await client.call(signature(procedure), values, signal());
    expect(reply.Message).toBe('synthetic message');
    expect(reply.ErrorMessage).toBeNull();
    expect(reply.datasets[0]?.rows[0]).toEqual(record());
    expect(fake.invoke.mock.calls[0]?.[0].procedure).toBe(procedure);
    await client.close();
  });
  it('prepara parâmetros explícitos mssql sem executar SQL ou perder BIGINT', () => {
    const request = new sql.Request(), input = vi.spyOn(request, 'input'), output = vi.spyOn(request, 'output'), execute = vi.spyOn(request, 'execute');
    prepareMssqlRequest(request, prepareCall(signature(), { FixtureTable: "'; DROP TABLE x;--", FixtureAfter: '9223372036854775807' }, 1000));
    expect(input).toHaveBeenCalledWith('FixtureAfter', sql.BigInt, '9223372036854775807');
    expect(input.mock.calls[0]?.[2]).toBe("'; DROP TABLE x;--");
    expect(output).toHaveBeenCalledTimes(2);
    expect(execute).not.toHaveBeenCalled();
    expect(request.arrayRowMode).toBe(true);
  });
  it.each([
    { FixtureTable: 'x', FixtureAfter: 9007199254740992 },
    { FixtureTable: 'x', FixtureAfter: '9223372036854775808' },
    { FixtureTable: 'x', FixtureAfter: '01' },
    { FixtureTable: 'x', FixtureAfter: undefined },
    { FixtureTable: 'x', FixtureAfter: null, Injected: 'x' },
  ])('rejeita parâmetros inexatos/ausentes/extras %#', values => {
    expect(() => prepareCall(signature(), values as never, 1000)).toThrow('INVALID_PARAMETER');
  });
  it('rejeita assinatura sem evidência, nomes SQL, duplicatas e OUTPUT sem tipo confirmado', () => {
    const s = signature();
    expect(() => prepareCall({ ...s, evidence: '' }, values, 1000)).toThrow('CONTRACT_UNCONFIRMED');
    for (const name of ['x;exec evil','FixtureTable','PASSWORD']) {
      expect(() => prepareCall({ ...s, parameters: [...s.parameters, { name, direction: 'output', type: { kind: 'int' }, nullable: true }] }, values, 1000)).toThrow();
    }
    expect(() => prepareCall({ ...s, errorOutput: 'Unknown' }, values, 1000)).toThrow('CONTRACT_UNCONFIRMED');
  });
  it('falha de conexão sanitizada, sem execução e reconexão com backoff controlado', async () => {
    let clock = 0;
    const fake = transport(); fake.connect.mockRejectedValueOnce(new Error('password=secret telephone=private'));
    const client = new OlosProcedureClient(fake, {}, () => clock);
    await expect(client.call(signature(), values, signal())).rejects.toEqual(new OlosError('CONNECTION_FAILED'));
    expect(fake.invoke).not.toHaveBeenCalled();
    await expect(client.call(signature(), values, signal())).rejects.toMatchObject({ code: 'RECONNECT_BACKOFF' });
    clock = 1000;
    await expect(client.call(signature(), values, signal())).resolves.toMatchObject({ ErrorMessage: null });
    expect(fake.connect).toHaveBeenCalledTimes(2);
  });
  it('timeout não libera transporte pendente nem dispara retry', async () => {
    vi.useFakeTimers();
    const fake = transport();
    let finish!: (r: ProcedureResponse) => void;
    fake.invoke.mockImplementationOnce(() => new Promise(resolve => { finish = resolve; }));
    const client = new OlosProcedureClient(fake);
    const pending = client.call(signature(), values, signal(), 100);
    const assertion = expect(pending).rejects.toMatchObject({ code: 'TIMEOUT', outcome: 'unknown' });
    await vi.advanceTimersByTimeAsync(101);
    expect(fake.invoke.mock.calls[0]?.[1].aborted).toBe(true);
    await expect(client.call(signature(), values, signal())).rejects.toMatchObject({ code: 'BUSY' });
    await expect(client.close()).rejects.toMatchObject({ code: 'BUSY' });
    expect(fake.close).not.toHaveBeenCalled();
    finish(response()); await assertion;
    expect(fake.invoke).toHaveBeenCalledTimes(1);
  });
  it('erro de execução pós-dispatch é incerto, sanitizado e não repetido', async () => {
    const fake = transport(); fake.invoke.mockRejectedValueOnce(new Error('PII and credentials'));
    await expect(new OlosProcedureClient(fake).call(signature(), values, signal())).rejects.toEqual(new OlosError('EXECUTION_FAILED','unknown'));
    expect(fake.invoke).toHaveBeenCalledTimes(1);
  });
  it('timeout nativo é sanitizado e reconexão posterior não repete automaticamente a chamada antiga', async () => {
    let clock = 0;
    const fake = transport(); fake.invoke.mockRejectedValueOnce({ code: 'ETIMEOUT', message: 'private timeout message' });
    const client = new OlosProcedureClient(fake, {}, () => clock);
    await expect(client.call(signature(), values, signal())).rejects.toEqual(new OlosError('TIMEOUT','unknown'));
    expect(fake.invoke).toHaveBeenCalledTimes(1); expect(fake.close).toHaveBeenCalledTimes(1);
    clock = 1000;
    await client.call(signature('LogReadData'), values, signal());
    expect(fake.connect).toHaveBeenCalledTimes(2);
    expect(fake.invoke.mock.calls[1]?.[0].procedure).toBe('LogReadData');
  });
  it('mudança da assinatura/valores do chamador durante conexão não altera a chamada preparada', async () => {
    const fake = transport(), s = signature(), parameters = { ...values };
    let connected!: () => void;
    fake.connect.mockImplementationOnce(() => new Promise(resolve => { connected = resolve; }));
    const pending = new OlosProcedureClient(fake).call(s, parameters, signal());
    parameters.FixtureAfter = '2';
    (s as { evidence: string }).evidence = 'changed after start';
    connected(); await pending;
    expect(fake.invoke.mock.calls[0]?.[0].values.FixtureAfter).toBe('9007199254740993');
    expect(fake.invoke.mock.calls[0]?.[0].signature.evidence).toBe('SYNTHETIC TEST ONLY');
  });
  it('captura erro OUTPUT em memória sem incorporar conteúdo em exceções', async () => {
    const value = response();
    const fake = transport({ ...value, output: { Message: 'private diagnostic', ErrorMessage: 'private SQL error', UnexpectedPassword: 'secret' } });
    const reply = await new OlosProcedureClient(fake).call(signature(), values, signal());
    expect(reply.ErrorMessage).toBe('private SQL error');
    expect(reply.output).not.toHaveProperty('UnexpectedPassword');
  });
  it('remove Password em metadados/linhas antes de publicar datasets', async () => {
    const data = dataset([{ ...record(), Password: 'must never escape' }]);
    const reply = await new OlosProcedureClient(transport(response({ ...data, columns: [...data.columns, { name: 'Password', type: 'nvarchar' }] }))).call(signature(), values, signal());
    expect(JSON.stringify(reply)).not.toContain('Password');
    expect(JSON.stringify(reply)).not.toContain('must never escape');
  });
  it('aceita resposta vazia preservando dataset e null', async () => {
    const reply = await new OlosProcedureClient(transport(response(dataset([])))).call(signature(), values, signal());
    expect(reply.datasets[0]?.rows).toEqual([]);
  });
  it('processa grande volume sintético dentro do limite e rejeita excesso', async () => {
    const rows = Array.from({ length: 10_000 }, (_, i) => record(String(i)));
    const fake = transport(response(dataset(rows)));
    const reply = await new OlosProcedureClient(fake, { maxRows: 10_000 }).call(signature(), values, signal());
    expect(reply.datasets[0]?.rows).toHaveLength(10_000);
    await expect(new OlosProcedureClient(fake, { maxRows: 9999 }).call(signature(), values, signal())).rejects.toMatchObject({ code: 'LIMIT_EXCEEDED' });
  });
  it('limita bytes e rejeita Date antes de structuredClone', async () => {
    for (const data of [dataset([{ ...record(), FixtureText: 'x'.repeat(1000) }]), dataset([{ ...record(), FixtureLocal: new Date() }])]) {
      await expect(new OlosProcedureClient(transport(response(data)), { maxBytes: 500 }).call(signature(), values, signal())).rejects.toBeInstanceOf(OlosError);
    }
  });
  it('cancelamento anterior à conexão não executa nada', async () => {
    const abort = new AbortController(); abort.abort(); const fake = transport();
    await expect(new OlosProcedureClient(fake).call(signature(), values, abort.signal)).rejects.toMatchObject({ code: 'ABORTED', outcome: 'not_sent' });
    expect(fake.connect).not.toHaveBeenCalled();
  });
});
