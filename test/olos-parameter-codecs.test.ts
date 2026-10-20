import sql from 'mssql';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { OlosProcedureClient, prepareCall, prepareMssqlRequest } from '../src/olos/client.js';
import { dateParameter, utcDriverDate } from '../src/olos/parameter-codecs.js';
import { reportedSignature } from '../src/olos/procedure-signatures.js';
import { fixtureSignature } from './support/olos-documented-fixture.js';
import { response, transport } from './support/olos.js';

afterEach(() => { vi.restoreAllMocks(); vi.unstubAllEnvs(); });
describe('parâmetros das assinaturas verificadas pela empresa', () => {
  it.each(['0001-01-01','2020-02-29','2020-06-03','9999-12-31'])('DATE %s conserva o dia civil em UTC somente na fronteira do driver', value => {
    expect(dateParameter(value)).toBe(value); expect(utcDriverDate(value).toISOString()).toBe(`${value}T00:00:00.000Z`);
  });
  it.each(['2020-02-30','1900-02-29','0000-01-01','2020/06/03','2020-06-03T00:00:00Z','2020-06-03-03:00','03/06/2020',new Date(),null,123])('rejeita DATE ambíguo ou inexato %#', value => {
    expect(() => dateParameter(value)).toThrow('INVALID_PARAMETER');
  });
  it.each(['America/Sao_Paulo','Pacific/Kiritimati','UTC'])('binding DATE com TZ=%s exige useUTC efetivo e preserva o ano 0001', timezone => {
    vi.stubEnv('TZ',timezone);
    const pool = new sql.ConnectionPool({ server: 'synthetic.invalid', options: { useUTC: true } });
    const request = pool.request(), connect = vi.spyOn(pool,'connect'), execute = vi.spyOn(request,'execute');
    const call = prepareCall(fixtureSignature('LogReadData'), { tablename: 'AttemptsRawData', DateExport: '0001-01-01' },1000);
    expect(call.values.DateExport).toBe('0001-01-01');
    prepareMssqlRequest(request,call);
    expect((request.parameters.DateExport?.value as Date).toISOString()).toBe('0001-01-01T00:00:00.000Z');
    expect(request.parameters.DateExport?.type).toBe(sql.Date);
    expect(request.parameters.tablename?.length).toBe(128);
    expect(connect).not.toHaveBeenCalled(); expect(execute).not.toHaveBeenCalled();
  });
  it('não prepara DATE quando useUTC está falso ou não há parent configurado', () => {
    const call = prepareCall(fixtureSignature('LogReadData'), { tablename: 'AttemptsRawData', DateExport: '2020-06-03' },1000);
    for (const request of [new sql.Request(),new sql.ConnectionPool({ server: 'synthetic.invalid', options: { useUTC: false } }).request()]) {
      expect(() => prepareMssqlRequest(request,call)).toThrow('CONTRACT_UNCONFIRMED');
      expect(Object.keys(request.parameters)).toHaveLength(0);
    }
  });
  it('nullable é política explícita: NULL de LogReadData não é data default nem string vazia', () => {
    const signature = reportedSignature('LogReadData',{ evidence: 'synthetic: explicit null means all dates as documented',
      nullable: { tablename: false, DateExport: true, Message: true, ErrorMessage: true } });
    const call = prepareCall(signature,{ tablename: 'AttemptsRawData', DateExport: null },1000);
    const request = new sql.ConnectionPool({ server: 'synthetic.invalid', options: { useUTC: true } }).request();
    prepareMssqlRequest(request,call); expect(request.parameters.DateExport?.value).toBeNull();
    expect(() => prepareCall(fixtureSignature('LogReadData'),{ tablename: 'AttemptsRawData', DateExport: null },1000)).toThrow('INVALID_PARAMETER');
    expect(() => reportedSignature('ReadData',{ evidence: 'missing policy', nullable: {} })).toThrow('CONTRACT_UNCONFIRMED');
  });
  it('VARCHAR(MAX) real relatado usa sql.MAX e recebe mensagem acima de 4000 sem truncar', async () => {
    const signature = fixtureSignature('ReadData'), call = prepareCall(signature,{ TableName: 'Campaign' },1000), request = new sql.Request();
    prepareMssqlRequest(request,call);
    expect(request.parameters.Message?.length).toBe(sql.MAX); expect(request.parameters.ErrorMessage?.length).toBe(sql.MAX);
    const message = 'x'.repeat(5000), fake = transport({ ...response(), output: { Message: message, ErrorMessage: null } });
    expect((await new OlosProcedureClient(fake).call(signature,{ TableName: 'Campaign' },new AbortController().signal)).Message).toBe(message);
    await expect(new OlosProcedureClient(fake).call(fixtureSignature('ReadDataCurrentDay'),{ TableName: 'AttemptsRawData' },new AbortController().signal)).rejects.toMatchObject({ code: 'LIMIT_EXCEEDED' });
  });
  it('limite de bytes inclui a soma dos OUTPUTs MAX, sem truncar ou registrar seu conteúdo', async () => {
    const fake = transport({ datasets: [], output: { Message: 'm'.repeat(600), ErrorMessage: 'e'.repeat(600) }, returnValue: 0 });
    await expect(new OlosProcedureClient(fake,{ maxBytes: 1000 }).call(fixtureSignature('ReadData'),{ TableName: 'Campaign' },new AbortController().signal)).rejects.toMatchObject({ code: 'LIMIT_EXCEEDED' });
  });
  it('política de OUTPUT não-null é aplicada sem preencher o null retornado', async () => {
    const signature = reportedSignature('ReadData',{ evidence: 'SYNTHETIC explicit nonnull output policy',
      nullable: { TableName: false, Message: true, ErrorMessage: false } });
    await expect(new OlosProcedureClient(transport()).call(signature,{ TableName: 'Campaign' },new AbortController().signal)).rejects.toMatchObject({ code: 'INVALID_RESPONSE' });
  });
  it('BIGINT do intervalo não é confundido com os tipos INT das PKs documentadas', () => {
    const call = prepareCall(fixtureSignature('ReReadData'),{ TableName: 'AttemptsRawData', DateExport: '2020-06-03',
      PKIDStart: '9007199254740993', PKIDEnd: '9223372036854775807' },1000);
    const request = new sql.ConnectionPool({ server: 'synthetic.invalid', options: { useUTC: true } }).request();
    prepareMssqlRequest(request,call);
    expect(request.parameters.PKIDStart?.value).toBe('9007199254740993'); expect(request.parameters.PKIDEnd?.value).toBe('9223372036854775807');
    expect(request.parameters.PKIDStart?.type).toBe(sql.BigInt);
  });
  it('conserva NVARCHAR em unidades UTF-16 e mantém VARCHAR ASCII até confirmar codepage', () => {
    const log = fixtureSignature('LogReadData');
    expect(prepareCall(log,{ tablename: 'Á'.repeat(128), DateExport: '2020-06-03' },1000).values.tablename).toHaveLength(128);
    expect(() => prepareCall(log,{ tablename: 'Á'.repeat(129), DateExport: '2020-06-03' },1000)).toThrow('INVALID_PARAMETER');
    expect(() => prepareCall(log,{ tablename: '😀'.repeat(65), DateExport: '2020-06-03' },1000)).toThrow('INVALID_PARAMETER');
    expect(() => prepareCall(fixtureSignature('ReadData'),{ TableName: 'Á' },1000)).toThrow('INVALID_PARAMETER');
  });
  it('DataAvailable e Reset continuam bloqueadas apesar das assinaturas conhecidas', () => {
    expect(() => reportedSignature('DataAvailable',{ evidence: 'synthetic', nullable: { TableName: false, DateExport: false } })).toThrow('PROCEDURE_BLOCKED');
    expect(() => prepareCall(fixtureSignature('ResetReadData'),{ TableName: 'AttemptsRawData', DateExport: '2020-06-03' },1000)).toThrow('PROCEDURE_BLOCKED');
  });
});
