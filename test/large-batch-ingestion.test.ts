import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { readFile, mkdtemp, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import type pg from 'pg';
import { LocalSpool } from '../adapters/local-spool.js';
import { spoolLimits } from '../src/large-batch/config.js';
import { LargeBatchCoordinator } from '../src/large-batch/coordinator.js';
import { PostgresLargeBatchPersistence } from '../src/persistence/large-batch.js';
import { PostgresPersistence } from '../src/persistence/store.js';
import { PersistenceError } from '../src/persistence/types.js';
import type { RawSource, SpoolManifest } from '../src/large-batch/types.js';
import { isolatedPostgres } from './support/postgres.js';
import { largePlan, rawSource } from './support/large-batch-fixture.js';
import { OlosReconciliation } from '../src/olos/recovery.js';
import { OlosProcedureClient } from '../src/olos/client.js';
import { fixtureSignature } from './support/olos-documented-fixture.js';
import { transport } from './support/olos.js';

let fixture: Awaited<ReturnType<typeof isolatedPostgres>>, pool: pg.Pool, root: string, repo: LocalSpool, core: PostgresPersistence, target: PostgresLargeBatchPersistence;
const limits = spoolLimits({ minFreeBytes: 0, chunkRows: 2, bufferBytes: 1024 });
const signal = () => new AbortController().signal;
beforeAll(async () => {
  fixture = await isolatedPostgres();
  await fixture.admin.query(await readFile(new URL('../migrations/005_large_batch_control.sql',import.meta.url),'utf8'));
  await fixture.admin.query(await readFile(new URL('../permissions/large-batch.sql',import.meta.url),'utf8'));
  await fixture.admin.query(await readFile(new URL('../migrations/003_projection_control.sql',import.meta.url),'utf8'));
  await fixture.admin.query(await readFile(new URL('../permissions/projection.sql',import.meta.url),'utf8'));
},150_000);
afterAll(async () => { await fixture?.stop(); },30_000);
beforeEach(async () => {
  await fixture.admin.query('TRUNCATE olos_export.source_contracts,olos_export.streams,olos_export.source_locks CASCADE');
  const c = largePlan().contract;
  await fixture.admin.query(`INSERT INTO olos_export.source_contracts(source_table,version,approved,fields,key_fields,layout_evidence,key_evidence)
    VALUES ($1,$2,true,$3::jsonb,$4,$5,$6)`,[c.source_table,c.version,JSON.stringify(c.fields),c.key_fields,c.layout_evidence,c.key_evidence]);
  await fixture.admin.query("INSERT INTO olos_export.source_locks(source_table) VALUES ('AttemptsRawData')");
  pool = fixture.worker(); core = new PostgresPersistence(pool); target = new PostgresLargeBatchPersistence(pool,limits);
  root = await mkdtemp(join(tmpdir(),'olos-large-pg-')); repo = await LocalSpool.open(root,limits);
});
afterEach(async () => { vi.restoreAllMocks(); await pool?.end(); await rm(root,{ recursive: true, force: true }); });
const coordinator = (r = repo,t = target,c = core) => new LargeBatchCoordinator(c,r,t,limits);
const rows = async () => (await fixture.admin.query('SELECT payload FROM olos_export.attempts_raw_data')).rows;
const cp = async () => (await fixture.admin.query('SELECT version FROM olos_export.checkpoints')).rows[0]?.version;
async function expire() { await fixture.admin.query("UPDATE olos_export.source_locks SET lease_until=clock_timestamp()-interval '1 second'"); }

describe('uma resposta selada, uma transação, uma identidade de recuperação', () => {
  it('confirma envelope exato, deduplicação global, recibo e checkpoint somente no final', async () => {
    const source = rawSource(7,{ duplicate: true });
    expect(await coordinator().receive(largePlan(),source,signal())).toEqual({ received: 7, persisted: 1, duplicates: 6, rejected: 0 });
    expect(source.calls).toBe(1); expect((await rows())[0].payload.StartDate).toBe('sql-datetime/v1:0000000001000000');
    expect(await cp()).toBe('1'); expect(await repo.active()).toBeNull();
    expect((await fixture.admin.query('SELECT count(*) FROM olos_export.large_batch_keys')).rows[0].count).toBe('1');
    expect((await fixture.admin.query('SELECT state FROM olos_export.runs')).rows[0].state).toBe('succeeded');
  });
  it('VPN indisponível durante leitura conserva intent/identidade e não grava resposta parcial', async () => {
    const source = rawSource(10,{ failAt: 3 });
    await expect(coordinator().receive(largePlan(),source,signal())).rejects.toMatchObject({ code: 'SOURCE_INTERRUPTED' });
    expect(await rows()).toHaveLength(0); expect(await cp()).toBe('0');
    const m = (await repo.active())!; expect(m.sealed).toBe(false); expect(m.identity.runId).toMatch(/^[a-f0-9-]+$/);
    await expect(coordinator().receive(largePlan(),rawSource(1),signal())).rejects.toMatchObject({ code: 'RECOVERY_REQUIRED' });
    await expire(); const recovered = await coordinator(await LocalSpool.open(root,limits)).recover(signal());
    expect(recovered.result).toBeNull(); expect(recovered.cases).toHaveLength(1); expect(recovered.manifest?.identity.batchId).toBe(m.identity.batchId);
  });
  it('fim do iterator sem marcador de conclusão não sela nem confirma checkpoint', async () => {
    await expect(coordinator().receive(largePlan(),rawSource(4,{ complete: false }),signal())).rejects.toMatchObject({ code: 'INCOMPLETE' });
    expect((await repo.active())?.sealed).toBe(false); expect(await cp()).toBe('0');
  });
  it('falha após leitura completa e antes de fsync do spool mantém o lote bloqueado', async () => {
    let armed = false;
    const disk = await LocalSpool.open(root,limits,{ async before(op) { if (armed && op === 'sync') throw Object.assign(new Error('disk private'),{ code: 'ENOSPC' }); } });
    const source = rawSource(1), read = source.read.bind(source);
    source.read = async function* (plan,s) { for await (const event of read(plan,s)) { if (event.kind === 'complete') armed = true; yield event; } };
    await expect(coordinator(disk).receive(largePlan(),source,signal())).rejects.toMatchObject({ code: 'DISK_FULL' });
    expect((await repo.active())?.sealed).toBe(false); expect(await rows()).toHaveLength(0); expect(await cp()).toBe('0');
  });
  it('falha após spool e antes do PostgreSQL é recuperável por nova instância sem nova leitura', async () => {
    const source = rawSource(5); vi.spyOn(target,'persist').mockRejectedValueOnce(new PersistenceError('PERSISTENCE_FAILED'));
    await expect(coordinator().receive(largePlan(),source,signal())).rejects.toMatchObject({ code: 'PERSISTENCE_FAILED' });
    const m = (await repo.active())!; expect(m.sealed).toBe(true); expect(await cp()).toBe('0');
    const restarted = coordinator(await LocalSpool.open(root,limits),new PostgresLargeBatchPersistence(pool,limits),new PostgresPersistence(pool));
    expect((await restarted.recover(signal())).result?.persisted).toBe(5); expect(source.calls).toBe(1); expect(await cp()).toBe('1');
  });
  it('commit efetuado sem ACK reconcilia pelo mesmo recibo sem reexecutar origem/escrita', async () => {
    const source = rawSource(5), persist = target.persist.bind(target);
    vi.spyOn(target,'persist').mockImplementationOnce(async (...args) => { await persist(...args); throw new PersistenceError('COMMIT_UNKNOWN','unknown'); });
    await expect(coordinator().receive(largePlan(),source,signal())).rejects.toMatchObject({ code: 'COMMIT_UNKNOWN' });
    expect(await cp()).toBe('1'); expect((await repo.active())?.state).toBe('uncertain');
    expect((await coordinator(await LocalSpool.open(root,limits)).recover(signal())).result?.persisted).toBe(5);
    expect(source.calls).toBe(1); expect(await cp()).toBe('1'); expect(await rows()).toHaveLength(5);
  });
  it('COMMIT sem ACK não efetuado deixa spool íntegro para retry local com mesmo ID', async () => {
    const original = pool.connect.bind(pool); let fail = false;
    vi.spyOn(pool,'connect').mockImplementation(async () => {
      const client = await original(), query = client.query.bind(client);
      client.query = (async (...args: unknown[]) => {
        if (args[0] === 'COMMIT' && fail) { fail = false; await query('ROLLBACK'); throw new Error('connection private'); }
        return (query as (...a: unknown[]) => Promise<unknown>)(...args);
      }) as typeof client.query;
      return client;
    });
    const p = target.persist.bind(target); vi.spyOn(target,'persist').mockImplementationOnce(async (...args) => { fail = true; return p(...args); });
    await expect(coordinator().receive(largePlan(),rawSource(5),signal())).rejects.toMatchObject({ code: 'COMMIT_UNKNOWN' });
    expect(await cp()).toBe('0'); expect(await rows()).toHaveLength(0);
    const m = (await repo.active())!; expect((await coordinator(await LocalSpool.open(root,limits)).recover(signal())).result?.received).toBe(5);
    expect((await fixture.admin.query('SELECT id FROM olos_export.batches')).rows[0].id).toBe(m.identity.batchId);
  });
  it('duplicatas divergentes entre parcelas fazem rollback integral inclusive checkpoint/staging', async () => {
    const source = rawSource(5,{ duplicate: true }), original = source.read.bind(source);
    source.read = async function* (plan,s) { let n = 0; for await (const e of original(plan,s)) {
      if (e.kind === 'row' && ++n === 4) { const v = [...e.values]; v[0] = Buffer.from([1,0,0,0]); yield { kind: 'row', values: v }; } else yield e;
    } };
    await expect(coordinator().receive(largePlan(),source,signal())).rejects.toMatchObject({ code: 'VALIDATION_FAILED' });
    expect(await rows()).toHaveLength(0); expect(await cp()).toBe('0');
    expect((await fixture.admin.query('SELECT count(*) FROM olos_export.large_batch_keys')).rows[0].count).toBe('0');
  });
  it('corrupção após spool não confirma parcelas já enviadas ao PostgreSQL', async () => {
    vi.spyOn(target,'persist').mockRejectedValueOnce(new PersistenceError('PERSISTENCE_FAILED'));
    await expect(coordinator().receive(largePlan(),rawSource(5),signal())).rejects.toThrow();
    const m = (await repo.active())!, path = join(root,'batches',m.identity.batchId,'records.ready'), data = await readFile(path);
    data[data.indexOf(Buffer.from('"X"'))+1] = 89; await writeFile(path,data); // JSON válido e tamanho igual, hash inválido ao EOF
    await expect(coordinator(await LocalSpool.open(root,limits)).recover(signal())).rejects.toThrow();
    expect(await rows()).toHaveLength(0); expect(await cp()).toBe('0');
  });
  it('lease expirada após restart bloqueia replay sem recibo e abre conciliação existente', async () => {
    vi.spyOn(target,'persist').mockRejectedValueOnce(new PersistenceError('PERSISTENCE_FAILED'));
    await expect(coordinator().receive(largePlan(),rawSource(3),signal())).rejects.toThrow(); await expire();
    await expect(coordinator(await LocalSpool.open(root,limits)).recover(signal())).rejects.toMatchObject({ code: 'LEASE_LOST' });
    expect((await fixture.admin.query('SELECT state FROM olos_export.runs')).rows[0].state).toBe('needs_reconciliation');
    expect((await repo.active())?.sealed).toBe(true); expect(await rows()).toHaveLength(0);
  });
  it('gates reais e preflight de privilégios bloqueiam antes de qualquer leitura', async () => {
    const native: RawSource = { kind: 'mssql', read: vi.fn(async function* () {}) };
    await expect(coordinator().receive(largePlan(),native,signal())).rejects.toMatchObject({ code: 'SOURCE_BLOCKED' }); expect(native.read).not.toHaveBeenCalled();
    await fixture.admin.query('REVOKE INSERT ON olos_export.large_batch_keys FROM olos_export_worker');
    const source = rawSource(1);
    try { await expect(coordinator().receive(largePlan(),source,signal())).rejects.toMatchObject({ code: 'CONTRACT_UNCONFIRMED' }); expect(source.calls).toBe(0); }
    finally { await fixture.admin.query('GRANT INSERT ON olos_export.large_batch_keys TO olos_export_worker'); }
  });
  it('resposta vazia completa ainda possui recibo e não presume paginação', async () => {
    const source = rawSource(0); expect((await coordinator().receive(largePlan(),source,signal())).received).toBe(0);
    expect(await cp()).toBe('1'); expect(source.calls).toBe(1); expect(await rows()).toHaveLength(0);
  });
  it('OUTPUT de erro ou dataset extra não sela resposta nem cria checkpoint de dados', async () => {
    const source = rawSource(1), read = source.read.bind(source);
    source.read = async function* (p,s) { for await (const e of read(p,s)) yield e.kind === 'complete' ? { ...e, errorMessage: 'private output error' } : e; };
    await expect(coordinator().receive(largePlan(),source,signal())).rejects.toMatchObject({ code: 'SOURCE_INTERRUPTED' });
    expect((await repo.active())?.sealed).toBe(false); expect(await cp()).toBe('0');
  });
  it('timeout solicita cancelamento e aguarda drenagem antes de liberar a exclusão local', async () => {
    const cfg = spoolLimits({ ...limits, readTimeoutMs: 20 }), c = new LargeBatchCoordinator(core,repo,target,cfg);
    let aborted = false, finish!: () => void;
    const source: RawSource = { kind: 'synthetic', async *read(_p,s) {
      await new Promise<void>(r => { s.addEventListener('abort',() => { aborted = true; r(); },{ once: true }); });
      await new Promise<void>(r => { finish = r; });
    } };
    const pending = c.receive(largePlan(),source,signal()), assertion = expect(pending).rejects.toThrow();
    for (let i = 0; i < 200 && !finish; i++) await new Promise(r => setTimeout(r,5));
    expect(aborted).toBe(true); expect(finish).toBeTypeOf('function');
    await expect(c.receive(largePlan(),rawSource(1),signal())).rejects.toMatchObject({ code: 'BUSY' });
    finish(); await assertion; expect((await repo.active())?.sealed).toBe(false);
  });
  it('backpressure de disco para o produtor e erro de armazenamento aborta antes do finally do transporte', async () => {
    let release!: () => void, entered = false, abortSeen = false, sourceRows = 0;
    const disk = await LocalSpool.open(root,limits,{ async before(op) { if (entered && op === 'write') {
      await new Promise<void>(r => { release = r; }); throw Object.assign(new Error('private'),{ code: 'ENOSPC' });
    } } });
    const source = rawSource(100), read = source.read.bind(source);
    source.read = async function* (p,s) { entered = true; try { for await (const e of read(p,s)) { if (e.kind === 'row') sourceRows++; yield e; } }
      finally { abortSeen = s.aborted; entered = false; } };
    const c = coordinator(disk), pending = c.receive(largePlan(),source,signal()), assertion = expect(pending).rejects.toMatchObject({ code: 'DISK_FULL' });
    for (let i = 0; i < 200 && !release; i++) await new Promise(r => setTimeout(r,5));
    const stopped = sourceRows; await new Promise(r => setTimeout(r,30)); expect(sourceRows).toBe(stopped); expect(stopped).toBeLessThan(100);
    release(); await assertion; expect(abortSeen).toBe(true); expect(await cp()).toBe('0');
  });
  it('concorrência de leitores e transporte pendente não libera novo consumo', async () => {
    let release!: () => void; const source = rawSource(1), read = source.read.bind(source);
    source.read = async function* (p,s) { await new Promise<void>(r => { release = r; }); yield* read(p,s); };
    const first = coordinator(), pending = first.receive(largePlan(),source,signal());
    for (let i = 0; i < 100 && !release; i++) await new Promise(r => setTimeout(r,5));
    await expect(first.receive(largePlan(),rawSource(1),signal())).rejects.toMatchObject({ code: 'BUSY' });
    await expect(coordinator(await LocalSpool.open(root,limits)).receive(largePlan(),rawSource(1),signal())).rejects.toMatchObject({ code: 'RECOVERY_REQUIRED' });
    release(); await pending;
  });
  it('planeja recuperação com LogReadData sintético e não invoca ReRead/Reset', async () => {
    await expect(coordinator().receive(largePlan(),rawSource(3,{ failAt: 1 }),signal())).rejects.toThrow(); await expire();
    const [caseId] = await core.recoverExpired(), fake = transport();
    const recovery = new OlosReconciliation(new OlosProcedureClient(fake),core,{ logSignature: fixtureSignature('LogReadData'), rereadSignature: fixtureSignature('ReReadData'),
      evidence: 'SYNTHETIC response interpretation', parameters: r => ({ tablename: r.sourceTable, DateExport: r.exportDate }),
      classifyLog: () => 'remote_read_observed', operatorApprovalReference: 'SYNTHETIC plan only' });
    const plan = await coordinator().planRecovery(caseId!,recovery,signal()); expect(plan.request.caseId).toBe(caseId);
    expect(fake.invoke).toHaveBeenCalledTimes(1); expect(fake.invoke.mock.calls[0]?.[0].procedure).toBe('LogReadData');
    expect((await repo.active())?.sealed).toBe(false);
  });
});

it('captura opcional confirma bruto e outbox na mesma transacao e preserva rollback', async () => {
  const config = {
    sourceNamespace: 'olos-fixture',
    destinationId: 'raw.olos_calls.fixture',
    mappingVersion: 'fixture-v1',
  };

  const captured = new PostgresLargeBatchPersistence(pool, limits, 300000, config);
  const first = await coordinator(repo, captured).receive(largePlan(), rawSource(3), signal());

  expect(first.received).toBe(3);
  expect(first.persisted).toBe(3);

  const overview = await pool.query(`
    SELECT
      (SELECT count(*)::int FROM olos_export.projection_batches) AS batches,
      (SELECT count(*)::int FROM olos_export.projection_outbox) AS deliveries,
      (SELECT count(*)::int FROM olos_export.attempts_raw_data) AS raw_rows,
      (SELECT version::text FROM olos_export.checkpoints LIMIT 1) AS checkpoint
  `);

  expect(overview.rows[0]).toMatchObject({
    batches: 1,
    deliveries: 3,
    raw_rows: 3,
    checkpoint: '1',
  });

  const pending = await pool.query(`
    SELECT count(*)::int AS total
    FROM olos_export.projection_outbox
    WHERE state='pending'
  `);
  expect(pending.rows[0].total).toBe(3);
});

it('captura opcional faz rollback do bruto, outbox e checkpoint quando falha', async () => {
  const config = {
    sourceNamespace: 'olos-fixture',
    destinationId: 'raw.olos_calls.fixture',
    mappingVersion: 'fixture-v1',
  };

  const captured = new PostgresLargeBatchPersistence(pool, limits, 300000, config);

  // Simula indisponibilidade da captura retirando apenas a permissão
  // de atualização dos metadados do lote, dentro do banco descartável.
  try {
  await fixture.admin.query(`
    REVOKE UPDATE(snapshot_digest,row_count)
    ON olos_export.projection_batches
    FROM olos_export_worker
  `);

  await expect(
    coordinator(repo, captured).receive(largePlan(), rawSource(3), signal())
  ).rejects.toMatchObject({ code: 'PERSISTENCE_FAILED' });

  const state = await fixture.admin.query(`
    SELECT
      (SELECT count(*)::int FROM olos_export.attempts_raw_data) AS raw_rows,
      (SELECT count(*)::int FROM olos_export.projection_batches) AS batches,
      (SELECT count(*)::int FROM olos_export.projection_outbox) AS deliveries,
      (SELECT version::text FROM olos_export.checkpoints LIMIT 1) AS checkpoint
  `);

  expect(state.rows[0]).toMatchObject({
    raw_rows: 0,
    batches: 0,
    deliveries: 0,
    checkpoint: '0',
  });

  expect((await repo.active())?.sealed).toBe(true);
  } finally {
    await fixture.admin.query(`
      GRANT UPDATE(snapshot_digest,row_count)
      ON olos_export.projection_batches
      TO olos_export_worker
    `);
  }
});

it('captura opcional preserva recibo no replay do lote confirmado', async () => {
  const config = {
    sourceNamespace: 'olos-fixture',
    destinationId: 'raw.olos_calls.fixture',
    mappingVersion: 'fixture-v1',
  };

  const captured = new PostgresLargeBatchPersistence(pool, limits, 300000, config);
  const b = coordinator(repo, captured);

  await b.receive(largePlan(), rawSource(3), signal());

  const original = await repo.active();

  // O lote confirmado não deve permanecer ativo no spool.
  expect(original).toBeNull();

  const before = await pool.query(`
    SELECT
      (SELECT count(*)::int FROM olos_export.projection_batches) AS batches,
      (SELECT count(*)::int FROM olos_export.projection_outbox) AS deliveries,
      (SELECT version::text FROM olos_export.checkpoints LIMIT 1) AS checkpoint
  `);

  expect(before.rows[0]).toMatchObject({
    batches: 1,
    deliveries: 3,
    checkpoint: '1',
  });

  // Reprocessamento do mesmo conteúdo em um novo run.
  const result = await b.receive(largePlan(), rawSource(3), signal());

  expect(result.persisted).toBe(0);
  expect(result.duplicates).toBe(3);

  const after = await pool.query(`
    SELECT
      (SELECT count(*)::int FROM olos_export.projection_batches) AS batches,
      (SELECT count(*)::int FROM olos_export.projection_outbox) AS deliveries,
      (SELECT version::text FROM olos_export.checkpoints LIMIT 1) AS checkpoint
  `);

  // Duas observações independentes geram duas capturas,
  // mas nenhuma chamada bruta duplicada.
  expect(after.rows[0]).toMatchObject({
    batches: 2,
    deliveries: 6,
    checkpoint: '2',
  });
});
