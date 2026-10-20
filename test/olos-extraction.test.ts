import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import type pg from 'pg';
import { OlosProcedureClient } from '../src/olos/client.js';
import { OlosExtraction, type ReadPlan } from '../src/olos/extraction.js';
import { OlosReconciliation } from '../src/olos/recovery.js';
import { PostgresPersistence } from '../src/persistence/store.js';
import { PersistenceError, type PersistenceBatch, type RunHandle } from '../src/persistence/types.js';
import { isolatedPostgres } from './support/postgres.js';
import { dataset, readPlan, record, response, signature, tableContract, transport } from './support/olos.js';

let fixture: Awaited<ReturnType<typeof isolatedPostgres>>, pool: pg.Pool, store: PostgresPersistence;
const signal = () => new AbortController().signal;
beforeAll(async () => { fixture = await isolatedPostgres(); }, 150_000);
afterAll(async () => { await fixture?.stop(); }, 30_000);
beforeEach(async () => {
  await fixture.admin.query('TRUNCATE olos_export.source_contracts, olos_export.streams, olos_export.source_locks CASCADE');
  const c = tableContract().source;
  await fixture.admin.query(`INSERT INTO olos_export.source_contracts(source_table,version,approved,fields,key_fields,layout_evidence,key_evidence)
    VALUES ($1,$2,true,$3::jsonb,$4,$5,$6)`, [c.source_table,c.version,JSON.stringify(c.fields),c.key_fields,c.layout_evidence,c.key_evidence]);
  await fixture.admin.query("INSERT INTO olos_export.source_locks(source_table) VALUES ('AttemptsRawData')");
  pool = fixture.worker(); store = new PostgresPersistence(pool);
});
afterEach(async () => { vi.restoreAllMocks(); await pool?.end(); });
async function state() {
  const runs = (await fixture.admin.query('SELECT state FROM olos_export.runs')).rows;
  const cp = (await fixture.admin.query('SELECT version,cursor_value FROM olos_export.checkpoints')).rows;
  const rows = (await fixture.admin.query('SELECT payload FROM olos_export.attempts_raw_data')).rows;
  return { runs, cp, rows };
}
const engine = (fake = transport()) => new OlosExtraction(new OlosProcedureClient(fake), store);
const waitFor = async (predicate: () => boolean) => { for (let i = 0; i < 200 && !predicate(); i++) await new Promise(resolve => setTimeout(resolve, 2)); expect(predicate()).toBe(true); };

describe('coordenação de extração sintética com persistência real isolada', () => {
  it('intent antes da leitura; persiste valores exatos e checkpoint mantendo a projeção intacta', async () => {
    const fake = transport();
    fake.invoke.mockImplementationOnce(async () => {
      expect((await state()).runs).toEqual([{ state: 'read_intent' }]);
      return response();
    });
    const receipt = await engine(fake).run(readPlan(), signal());
    const saved = await state();
    expect(receipt.received).toBe(1);
    expect(saved.rows[0]?.payload).toEqual(record());
    expect(saved.cp).toEqual([{ version: '1', cursor_value: null }]);
    expect(saved.runs).toEqual([{ state: 'succeeded' }]);
    expect((await fixture.admin.query("SELECT to_regclass('raw.olos_calls') AS raw")).rows[0].raw).toBeNull();
    expect((await fixture.admin.query('SELECT version FROM olos_export.schema_versions ORDER BY version')).rows).toEqual([{ version: 1 },{ version: 2 }]);
  });
  it('lote vazio confirmado não presume paginação ou chama novamente', async () => {
    const fake = transport(response(dataset([])));
    const receipt = await engine(fake).run(readPlan(), signal());
    expect(receipt.received).toBe(0); expect(receipt.batches).toHaveLength(1);
    expect(fake.invoke).toHaveBeenCalledTimes(1);
    expect((await state()).cp[0]?.version).toBe('1');
  });
  it('falha de PostgreSQL antes do intent resulta em zero chamadas remotas', async () => {
    const fake = transport();
    vi.spyOn(store, 'startRun').mockRejectedValueOnce(new PersistenceError('PERSISTENCE_FAILED'));
    await expect(engine(fake).run(readPlan(), signal())).rejects.toMatchObject({ code: 'PERSISTENCE_FAILED' });
    expect(fake.invoke).not.toHaveBeenCalled(); expect((await state()).runs).toEqual([]);
  });
  it('falha de conexão Olos deixa intent para recuperação sem avançar checkpoint', async () => {
    const fake = transport(); fake.connect.mockRejectedValueOnce(new Error('connection private details'));
    await expect(engine(fake).run(readPlan(), signal())).rejects.toMatchObject({ code: 'CONNECTION_FAILED' });
    expect((await state()).cp[0]?.version).toBe('0');
    expect((await state()).runs).toEqual([{ state: 'read_intent' }]);
  });
  it('ErrorMessage e returnValue não aprovado bloqueiam persistência sem reler origem', async () => {
    const fake = transport({ ...response(), output: { Message: null, ErrorMessage: 'private error' } });
    await expect(engine(fake).run(readPlan(), signal())).rejects.toMatchObject({ code: 'OUTPUT_ERROR', outcome: 'unknown' });
    expect((await state()).rows).toEqual([]); expect(fake.invoke).toHaveBeenCalledTimes(1);
  });
  it('returnValue não aprovado não é interpretado como sucesso por ausência de erro', async () => {
    const fake = transport({ ...response(), returnValue: 99 });
    await expect(engine(fake).run(readPlan(), signal())).rejects.toMatchObject({ code: 'OUTPUT_ERROR' });
    expect((await state()).cp[0]?.version).toBe('0');
  });
  it('múltiplos datasets não são descartados silenciosamente', async () => {
    const fake = transport({ ...response(), datasets: [dataset(), dataset([])] });
    await expect(engine(fake).run(readPlan(), signal())).rejects.toMatchObject({ code: 'INVALID_RESPONSE' });
    expect((await state()).rows).toEqual([]);
  });
  it('interrupção entre leitura e persistência vira conciliação após reinicialização', async () => {
    const fake = transport(); vi.spyOn(store, 'observeBatch').mockRejectedValueOnce(new Error('simulated interruption'));
    await expect(engine(fake).run(readPlan(), signal())).rejects.toThrow('simulated interruption');
    expect((await state()).cp[0]?.version).toBe('0');
    await fixture.admin.query("UPDATE olos_export.source_locks SET lease_until=clock_timestamp()-interval '1 second'");
    const restarted = new OlosExtraction(new OlosProcedureClient(transport()), new PostgresPersistence(pool));
    const cases = await restarted.recoverInterrupted();
    expect(cases).toHaveLength(1);
    expect((await state()).runs).toEqual([{ state: 'needs_reconciliation' }]);
    await expect(restarted.run(readPlan(), signal())).rejects.toMatchObject({ code: 'RECOVERY_REQUIRED' });
    expect(fake.invoke).toHaveBeenCalledTimes(1);
  });
  it('lote retido pode ser repetido localmente após falha sem nova leitura', async () => {
    const fake = transport(), extraction = engine(fake);
    let retained!: PersistenceBatch, handle!: RunHandle;
    const originalObserve = store.observeBatch.bind(store);
    vi.spyOn(store, 'observeBatch').mockImplementation(async (h, batch) => { retained = batch; handle = h; await originalObserve(h,batch); });
    vi.spyOn(store, 'persistBatch').mockRejectedValueOnce(new PersistenceError('PERSISTENCE_FAILED'));
    await expect(extraction.run(readPlan(), signal())).rejects.toMatchObject({ code: 'PERSISTENCE_FAILED' });
    await extraction.retryRetainedBatch(handle, retained);
    await new OlosExtraction(new OlosProcedureClient(transport()), new PostgresPersistence(pool)).retryRetainedBatch(handle, retained);
    expect((await state()).rows).toHaveLength(1);
    expect((await state()).cp[0]?.version).toBe('1'); expect(fake.invoke).toHaveBeenCalledTimes(1);
    await store.finishRun(handle);
  });
  it('COMMIT sem confirmação impede próxima leitura e exige consulta/recuperação', async () => {
    const fake = transport(), persist = store.persistBatch.bind(store);
    vi.spyOn(store, 'persistBatch').mockImplementationOnce(async (h,b) => { await persist(h,b); throw new PersistenceError('COMMIT_UNKNOWN','unknown'); });
    await expect(engine(fake).run(readPlan(), signal())).rejects.toMatchObject({ code: 'COMMIT_UNKNOWN' });
    expect((await state()).rows).toHaveLength(1);
    expect((await state()).cp[0]?.version).toBe('1');
    expect((await state()).runs).toEqual([{ state: 'persisting' }]);
    expect(fake.invoke).toHaveBeenCalledTimes(1);
  });
  it('duas instâncias não leem a mesma tabela simultaneamente', async () => {
    const fake = transport(); let finish!: () => void;
    fake.invoke.mockImplementationOnce(async () => { await new Promise<void>(resolve => { finish = resolve; }); return response(); });
    const first = engine(fake), pending = first.run(readPlan(), signal());
    await waitFor(() => Boolean(finish));
    await expect(first.run(readPlan(), signal())).rejects.toMatchObject({ code: 'BUSY' });
    const secondTransport = transport();
    await expect(engine(secondTransport).run(readPlan(), signal())).rejects.toMatchObject({ code: 'BUSY' });
    expect(secondTransport.invoke).not.toHaveBeenCalled(); finish(); await pending;
  });
  it('lease expirada durante leitura impede persistência e conserva necessidade de conciliação', async () => {
    const fake = transport();
    fake.invoke.mockImplementationOnce(async () => {
      await fixture.admin.query("UPDATE olos_export.source_locks SET lease_until=clock_timestamp()-interval '1 second'"); return response();
    });
    await expect(engine(fake).run(readPlan(), signal())).rejects.toMatchObject({ code: 'LEASE_LOST' });
    expect((await state()).rows).toEqual([]); expect(fake.invoke).toHaveBeenCalledTimes(1);
  });
  it('repetição exige evidência e não avança antes do commit anterior', async () => {
    const fake = transport(); let call = 0;
    fake.invoke.mockImplementation(async () => {
      call++;
      expect((await state()).cp[0]?.version).toBe(String(call - 1));
      return response(dataset([record(String(call))]));
    });
    const base = readPlan();
    const plan: ReadPlan = { ...base, repetition: { evidence: 'SYNTHETIC explicit cursor and termination', maxCalls: 2,
      continuation: (_reply, _records, cp) => ({ done: cp.version === '1', nextCursor: cp.version === '0' ? 'synthetic:page2' : 'synthetic:done' }) } };
    await expect(engine(fake).run({ ...plan, repetition: { ...plan.repetition!, evidence: '' } }, signal())).rejects.toMatchObject({ code: 'REPETITION_UNCONFIRMED' });
    expect(fake.invoke).not.toHaveBeenCalled();
    expect((await engine(fake).run(plan, signal())).received).toBe(2);
    expect((await state()).cp).toEqual([{ version: '2', cursor_value: 'synthetic:done' }]);
  });
  it('sem associação de tipo de leitura comprovada não grava intent nem lê origem', async () => {
    const fake = transport();
    await expect(engine(fake).run({ ...readPlan(), readTypeEvidence: '' }, signal())).rejects.toMatchObject({ code: 'CONTRACT_UNCONFIRMED' });
    expect(fake.invoke).not.toHaveBeenCalled(); expect((await state()).runs).toEqual([]);
  });
  it('cursor sem progresso não autoriza nova chamada nem checkpoint falso', async () => {
    const fake = transport(), plan = readPlan();
    await expect(engine(fake).run({ ...plan, repetition: { evidence: 'SYNTHETIC', maxCalls: 2,
      continuation: () => ({ done: false, nextCursor: null }) } }, signal())).rejects.toMatchObject({ code: 'NO_PROGRESS' });
    expect(fake.invoke).toHaveBeenCalledTimes(1); expect((await state()).cp[0]?.version).toBe('0');
  });
  it('checkpoint inconsistente após commit interrompe sequência', async () => {
    const fake = transport(), cp = store.checkpoint.bind(store);
    vi.spyOn(store, 'checkpoint').mockImplementationOnce(cp).mockResolvedValueOnce({ version: '5', cursor: 'wrong' });
    await expect(engine(fake).run(readPlan(), signal())).rejects.toMatchObject({ code: 'INVALID_RESPONSE' });
    expect(fake.invoke).toHaveBeenCalledTimes(1);
  });
  it('recuperação consulta apenas LogReadData sintético, grava digest e nunca executa ReRead/Reset', async () => {
    vi.spyOn(store, 'observeBatch').mockRejectedValueOnce(new Error('crash'));
    await expect(engine().run(readPlan(), signal())).rejects.toThrow('crash');
    await fixture.admin.query("UPDATE olos_export.source_locks SET lease_until=clock_timestamp()-interval '1 second'");
    const [caseId] = await store.recoverExpired();
    const fake = transport(), client = new OlosProcedureClient(fake);
    const request = { caseId: caseId!, sourceTable: 'AttemptsRawData' as const, readType: 'investigation' as const, exportDate: '2026-10-15', pkRange: null };
    const recovery = new OlosReconciliation(client, store, { logSignature: signature('LogReadData'), rereadSignature: signature('ReReadData'),
      evidence: 'SYNTHETIC response classifier', parameters: () => ({ FixtureTable: 'AttemptsRawData', FixtureAfter: null }),
      classifyLog: () => 'remote_read_observed', operatorApprovalReference: 'SYNTHETIC plan only' });
    const evidence = await recovery.inspectLogReadData(request, signal());
    const planned = await recovery.planReReadData(request, evidence);
    expect(planned.operatorApprovalReference).toBe('SYNTHETIC plan only');
    expect(fake.invoke).toHaveBeenCalledTimes(1); expect(fake.invoke.mock.calls[0]?.[0].procedure).toBe('LogReadData');
    expect((await fixture.admin.query('SELECT action,result,evidence_digest FROM olos_export.recovery_attempts ORDER BY created_at')).rows).toEqual([
      { action: 'inspect_log', result: 'evidence_recorded', evidence_digest: evidence.digest },
      { action: 'plan_reread', result: 'evidence_recorded', evidence_digest: evidence.digest },
    ]);
    expect((await fixture.admin.query('SELECT state FROM olos_export.recovery_cases')).rows[0].state).toBe('open');
    await expect(recovery.planReReadData(request, { ...evidence, conclusion: 'inconclusive' })).rejects.toMatchObject({ code: 'CONTRACT_UNCONFIRMED' });
    await expect(recovery.planReReadData({ ...request, exportDate: '2026-10-14' }, evidence)).rejects.toMatchObject({ code: 'CONTRACT_UNCONFIRMED' });
  });
});
