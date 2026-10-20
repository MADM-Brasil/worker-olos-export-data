import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import pg from 'pg';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import { isolatedPostgres } from './support/postgres.js';
import { fixturePlan } from './support/projection-plan.js';
import { PostgresPersistence } from '../src/persistence/store.js';
import type { PersistenceBatch } from '../src/persistence/types.js';
import { ProjectionOutbox } from '../src/projection/outbox.js';
import { ProjectionDispatcher, type DeliveryTarget } from '../src/projection/dispatcher.js';
import { PostgresDeliveryTarget, requestDigest, type TransactionalBusinessWriter } from '../src/projection/postgres-target.js';
import { projectCall } from '../src/projection/transform.js';
import { RawCallsWriter } from '../src/projection/writer.js';

const config = { sourceNamespace: 'synthetic-source',destinationId: 'synthetic-target',mappingVersion: fixturePlan.version };
let source: Awaited<ReturnType<typeof isolatedPostgres>>;
let destination: Awaited<ReturnType<typeof isolatedPostgres>>;
let ingester: pg.Pool, projector: pg.Pool, receiver: pg.Pool;
let persistence: PostgresPersistence, outbox: ProjectionOutbox;
const fixtureWriter: TransactionalBusinessWriter = {
  preflight: row => typeof row.raw_olos_call_id==='string' ? 'allowed' : 'rejected',
  async writeInTransaction(client, request) {
    // Destino somente SINTÉTICO, não uma reprodução do DDL raw real.
    await client.query(`INSERT INTO olos_export.fixture_calls(id,payload,writes) VALUES($1,$2::jsonb,1)
      ON CONFLICT(id) DO UPDATE SET payload=EXCLUDED.payload,writes=olos_export.fixture_calls.writes+1`, [request.row.raw_olos_call_id,JSON.stringify(request.row)]);
  },
};
function target(pool = receiver) { return new PostgresDeliveryTarget(pool,config.destinationId,fixtureWriter); }
function dispatcher(destinationTarget: DeliveryTarget = target(), box = outbox) {
  return new ProjectionDispatcher(box,destinationTarget,message => projectCall(message.payload,{},fixturePlan));
}
function value(id = '9007199254740993', text: string | null = '0001-Call'): PersistenceBatch {
  return { id: randomUUID(),records: [{ FixtureId: id,CallText: text,Clock: '2026-10-15T09:30:00.1234567',Internal: 'independent-text-id' }],expected: { version: '0',cursor: null },nextCursor: 'fixture:1' };
}
async function start(store = persistence) {
  return store.startRun({ table: 'AttemptsRawData',contractVersion: 'fixture-source-v1',readType: 'investigation',exportDate: '2026-10-15',pkRange: null },new AbortController().signal);
}
async function ingest(batch = value(), store = persistence) {
  const run = await start(store);
  await store.observeBatch(run,batch);
  await store.persistBatch(run,batch);
  return { run,batch };
}
async function saved() { return (await source.admin.query('SELECT * FROM olos_export.projection_outbox ORDER BY delivery_id')).rows; }
async function makeAvailable() { await source.admin.query("UPDATE olos_export.projection_outbox SET available_at=clock_timestamp()-interval '1 second'"); }
// Injeta somente perda de ACK do COMMIT; mantém SQL real no PostgreSQL descartável.
function loseCommitAck(pool: pg.Pool, serverCommits: boolean, failureAt = 1): Pick<pg.Pool,'connect'> {
  let commits = 0;
  return { async connect() {
    const client = await pool.connect();
    return { async query(sql: string, values?: unknown[]) {
      if (sql==='COMMIT' && ++commits===failureAt) { if (serverCommits) await client.query(sql); throw new Error('private-driver-message'); }
      return client.query(sql,values);
    },release: (destroy?: boolean) => client.release(destroy) };
  } } as unknown as Pick<pg.Pool,'connect'>;
}

beforeAll(async () => {
  source = await isolatedPostgres();
  try {
    destination = await isolatedPostgres();
    await source.admin.query(await readFile(new URL('../migrations/003_projection_control.sql',import.meta.url),'utf8'));
    await source.admin.query(await readFile(new URL('../permissions/projection.sql',import.meta.url),'utf8'));
    await destination.admin.query(await readFile(new URL('../migrations/004_delivery_receipts.sql',import.meta.url),'utf8'));
    await destination.admin.query(await readFile(new URL('../permissions/receiver.sql',import.meta.url),'utf8'));
    await destination.admin.query('CREATE TABLE olos_export.fixture_calls(id text PRIMARY KEY,payload jsonb NOT NULL,writes integer NOT NULL)');
    await destination.admin.query('GRANT SELECT,INSERT,UPDATE ON olos_export.fixture_calls TO olos_export_receiver');
  } catch (error) { await destination?.stop(); await source.stop(); throw error; }
},180_000);
afterAll(async () => { await destination?.stop(); await source?.stop(); },30_000);
beforeEach(async () => {
  await source.admin.query('TRUNCATE olos_export.source_contracts,olos_export.streams,olos_export.source_locks CASCADE');
  await source.admin.query('ALTER TABLE olos_export.projection_outbox DROP CONSTRAINT IF EXISTS reject_capture');
  await destination.admin.query('TRUNCATE olos_export.delivery_receipts,olos_export.fixture_calls');
  const fields = { FixtureId: { type: 'int64',nullable: false },CallText: { type: 'text',nullable: true },Clock: { type: 'local_timestamp',nullable: true },Internal: { type: 'text',nullable: true } };
  await source.admin.query(`INSERT INTO olos_export.source_contracts(source_table,version,approved,fields,key_fields,layout_evidence,key_evidence)
    VALUES('AttemptsRawData','fixture-source-v1',true,$1::jsonb,ARRAY['FixtureId'],'SYNTHETIC ONLY','SYNTHETIC UNIQUE KEY ONLY')`, [JSON.stringify(fields)]);
  await source.admin.query("INSERT INTO olos_export.source_locks(source_table) VALUES('AttemptsRawData')");
  ingester = source.worker();
  projector = new pg.Pool({ ...source.connection,options: '-c role=olos_export_projector' });
  receiver = new pg.Pool({ ...destination.connection,options: '-c role=olos_export_receiver' });
  persistence = new PostgresPersistence(ingester,{},config);
  outbox = new ProjectionOutbox(projector,config.destinationId,60_000,1);
});
afterEach(async () => { vi.restoreAllMocks(); await Promise.all([ingester?.end(),projector?.end(),receiver?.end()]); });

describe('captura atômica e snapshots da projeção', () => {
  it('commita envelope, snapshot, intenção e checkpoint juntos sem afetar o ledger antigo', async () => {
    const { run,batch } = await ingest();
    const rows = await saved();
    expect(rows).toHaveLength(1); expect(rows[0].state).toBe('pending');
    expect(rows[0].payload.Clock).toBe('2026-10-15T09:30:00.1234567');
    expect(rows[0].payload.Internal).toBe('independent-text-id');
    expect(await persistence.checkpoint(run)).toEqual({ version: '1',cursor: 'fixture:1' });
    expect((await source.admin.query('SELECT version FROM olos_export.schema_versions ORDER BY version')).rows).toEqual([{ version: 1 },{ version: 2 }]);
    await persistence.persistBatch(run,batch);
    expect((await saved()).map(r => r.delivery_id)).toEqual([rows[0].delivery_id]);
  });
  it('preserva snapshots anteriores mesmo que o envelope mais recente seja alterado', async () => {
    const { run } = await ingest(value('1','old'));
    const next: PersistenceBatch = { ...value('1','new'),expected: { version: '1',cursor: 'fixture:1' },nextCursor: 'fixture:2' };
    await persistence.observeBatch(run,next); await persistence.persistBatch(run,next);
    expect((await saved()).map(r => r.payload.CallText).sort()).toEqual(['new','old']);
    expect((await source.admin.query('SELECT payload FROM olos_export.attempts_raw_data')).rows[0].payload.CallText).toBe('new');
  });
  it('falha da outbox reverte também envelope e checkpoint', async () => {
    const run = await start(), batch = value(); await persistence.observeBatch(run,batch);
    await source.admin.query('ALTER TABLE olos_export.projection_outbox ADD CONSTRAINT reject_capture CHECK(false)');
    await expect(persistence.persistBatch(run,batch)).rejects.toMatchObject({ outcome: 'not_committed' });
    expect(await saved()).toEqual([]);
    expect((await source.admin.query('SELECT count(*)::text n FROM olos_export.attempts_raw_data')).rows[0].n).toBe('0');
    expect((await persistence.checkpoint(run)).version).toBe('0');
    await source.admin.query('ALTER TABLE olos_export.projection_outbox DROP CONSTRAINT reject_capture');
    await persistence.persistBatch(run,batch); expect(await saved()).toHaveLength(1);
  });
  it('não reconstrói silenciosamente projeção de lote antigo sem captura', async () => {
    const legacy = new PostgresPersistence(ingester);
    const { run,batch } = await ingest(value(),legacy);
    await expect(persistence.persistBatch(run,batch)).rejects.toMatchObject({ code: 'PROJECTION_CAPTURE_MISSING' });
    expect(await saved()).toEqual([]); expect((await legacy.checkpoint(run)).version).toBe('1');
  });
  it.each([true,false])('COMMIT da ingestão sem ACK preserva atomicidade local (servidor commitou=%s)', async committed => {
    const batch = value(), run = await start(); await persistence.observeBatch(run,batch);
    const unstable = new PostgresPersistence(loseCommitAck(ingester,committed),{},config);
    await expect(unstable.persistBatch(run,batch)).rejects.toMatchObject({ code: 'COMMIT_UNKNOWN' });
    expect(await saved()).toHaveLength(committed ? 1 : 0);
    if (committed) {
      const id = (await saved())[0].delivery_id;
      await persistence.persistBatch(run,batch); expect((await saved())[0].delivery_id).toBe(id);
    } else expect((await source.admin.query('SELECT version FROM olos_export.checkpoints')).rows[0].version).toBe('0');
  });
  it('marker existe para lote vazio e duplicatas do mesmo lote não criam duas entregas', async () => {
    const b = value(); const duplicated = { ...b,records: [b.records[0]!,b.records[0]!] };
    const { run } = await ingest(duplicated); expect(await saved()).toHaveLength(1);
    const empty: PersistenceBatch = { ...value(),records: [],expected: { version: '1',cursor: 'fixture:1' } };
    await persistence.observeBatch(run,empty); await persistence.persistBatch(run,empty);
    expect((await source.admin.query('SELECT row_count FROM olos_export.projection_batches WHERE batch_id=$1',[empty.id])).rows[0].row_count).toBe(0);
  });
});

describe('projetores e receptor idempotente em bancos PostgreSQL distintos', () => {
  it('crash depois da ingestão é recuperado sem nova leitura Olos', async () => {
    const { run } = await ingest(); await persistence.finishRun(run);
    const restarted = new ProjectionOutbox(projector,config.destinationId,60_000,1);
    expect(await dispatcher(target(),restarted).runOnce()).toBe('confirmed');
    const state = (await saved())[0]; expect(state.state).toBe('confirmed');
    expect((await destination.admin.query('SELECT payload,writes FROM olos_export.fixture_calls')).rows[0]).toEqual({ payload: { raw_olos_call_id: '9007199254740993',olos_call_id: '0001-Call' },writes: 1 });
    expect((await source.admin.query('SELECT version FROM olos_export.checkpoints')).rows[0].version).toBe('1');
    expect(await dispatcher().runOnce()).toBe('empty');
  });
  it('destino raw bloqueado mantém entrega pendente sem registrar autorização de envio', async () => {
    await ingest();
    const wrapped = new PostgresDeliveryTarget(receiver,config.destinationId,new RawCallsWriter());
    const connect = vi.spyOn(receiver,'connect');
    expect(await dispatcher(wrapped).runOnce()).toBe('pending');
    expect(connect).not.toHaveBeenCalled();
    expect((await saved())[0]).toMatchObject({ state: 'pending',last_code: 'DESTINATION_BLOCKED',attempt_id: null });
  });
  it('projeção sem identidade comprovada permanece pendente e não chama destino', async () => {
    await ingest(); const t = target(); const send = vi.spyOn(t,'deliver');
    const d = new ProjectionDispatcher(outbox,t,m => projectCall(m.payload,{}, { ...fixturePlan,identity: { ...fixturePlan.identity,approved: false } }));
    expect(await d.runOnce()).toBe('pending'); expect(send).not.toHaveBeenCalled();
    expect((await saved())[0].request_payload).toBeNull();
  });
  it('falha de conexão do destino permite retry com mesma identidade e payload', async () => {
    await ingest(); const id = (await saved())[0].delivery_id;
    const unavailable = vi.spyOn(receiver,'connect').mockRejectedValue(new Error('private-target-address'));
    expect(await dispatcher().runOnce()).toBe('pending');
    expect((await saved())[0].delivery_id).toBe(id);
    unavailable.mockRestore(); await makeAvailable();
    expect(await dispatcher().runOnce()).toBe('confirmed');
    expect((await destination.admin.query('SELECT writes FROM olos_export.fixture_calls')).rows[0].writes).toBe(1);
  });
  it.each([true,false])('COMMIT do destino sem ACK fica incerto até recibo (servidor commitou=%s)', async committed => {
    await ingest();
    const t = new PostgresDeliveryTarget(loseCommitAck(receiver,committed),config.destinationId,fixtureWriter);
    const d = dispatcher(t); expect(await d.runOnce()).toBe('uncertain');
    const id = (await saved())[0].delivery_id;
    expect(await dispatcher().runOnce()).toBe('empty');
    expect(await dispatcher().reconcile(id)).toBe(committed);
    expect((await saved())[0].state).toBe(committed ? 'confirmed' : 'uncertain');
    expect((await destination.admin.query('SELECT count(*)::text n FROM olos_export.fixture_calls')).rows[0].n).toBe(committed ? '1' : '0');
    if (!committed) expect((await saved())[0].last_code).toBe('RECEIPT_ABSENT');
  });
  it('resposta perdida após commit comprovado é conciliada sem reenviar', async () => {
    await ingest(); const t = target(); const send = t.deliver.bind(t);
    vi.spyOn(t,'deliver').mockImplementation(async r => { await send(r); throw new Error('sensitive-post-send-error'); });
    expect(await dispatcher(t).runOnce()).toBe('uncertain');
    const restarted = dispatcher(); expect(await restarted.reconcile((await saved())[0].delivery_id)).toBe(true);
    expect((await destination.admin.query('SELECT writes FROM olos_export.fixture_calls')).rows[0].writes).toBe(1);
  });
  it('dois projetores não enviam a mesma entrega em paralelo', async () => {
    await ingest(); let unblock!: () => void; let entered!: () => void;
    const began = new Promise<void>(resolve => { entered = resolve; });
    const wait = new Promise<void>(resolve => { unblock = resolve; });
    const t = target(); const send = t.deliver.bind(t);
    const spy = vi.spyOn(t,'deliver').mockImplementation(async r => { entered(); await wait; return send(r); });
    const first = dispatcher(t).runOnce();
    await began;
    try { expect(await dispatcher(t).runOnce()).toBe('empty'); expect(spy).toHaveBeenCalledTimes(1); }
    finally { unblock(); }
    expect(await first).toBe('confirmed');
  });
  it('claim expirado pode ser retomado, mas A antigo não autoriza nem libera B', async () => {
    await ingest(); const a = await outbox.claim(randomUUID()); expect(a).not.toBeNull();
    await source.admin.query("UPDATE olos_export.projection_outbox SET lease_until=clock_timestamp()-interval '1 second'");
    const b = await new ProjectionOutbox(projector,config.destinationId).claim(randomUUID()); expect(b).not.toBeNull();
    await expect(outbox.authorize(a!,{ raw_olos_call_id: '1' })).rejects.toMatchObject({ code: 'LEASE_LOST' });
    await expect(outbox.defer(a!)).rejects.toMatchObject({ code: 'LEASE_LOST' });
    expect((await saved())[0].lease_owner).toBe(b!.owner);
  });
  it('crash depois de autorizar não libera reenvio por expiração de lease', async () => {
    await ingest(); const claim = (await outbox.claim(randomUUID()))!;
    await outbox.authorize(claim,{ raw_olos_call_id: '9007199254740993',olos_call_id: '0001-Call' });
    await source.admin.query("UPDATE olos_export.projection_outbox SET lease_until=clock_timestamp()-interval '1 second'");
    expect(await dispatcher().runOnce()).toBe('empty');
    expect(await dispatcher().reconcile(claim.id)).toBe(false);
    expect((await saved())[0].state).toBe('uncertain');
    expect((await destination.admin.query('SELECT count(*)::text n FROM olos_export.fixture_calls')).rows[0].n).toBe('0');
  });
  it('receptor deduplica reprocessamento e concorrência usando recibo transacional', async () => {
    const row = { raw_olos_call_id: '1',olos_call_id: '0001' };
    const request = { deliveryId: randomUUID(),destinationId: config.destinationId,contractVersion: 'raw.olos_calls/v1',mappingVersion: fixturePlan.version,row,
      requestDigest: requestDigest(row,config.destinationId,'raw.olos_calls/v1',fixturePlan.version) };
    expect(await Promise.all([target().deliver(request),target().deliver(request)])).toEqual(['confirmed','confirmed']);
    expect(await target().deliver(request)).toBe('confirmed');
    expect((await destination.admin.query('SELECT writes FROM olos_export.fixture_calls')).rows[0].writes).toBe(1);
    const changed = { ...request,row: { ...row,olos_call_id: 'changed' } };
    await expect(target().deliver({ ...changed,requestDigest: requestDigest(changed.row,config.destinationId,changed.contractVersion,changed.mappingVersion) })).rejects.toMatchObject({ outcome: 'not_committed' });
    expect((await destination.admin.query('SELECT payload FROM olos_export.fixture_calls')).rows[0].payload.olos_call_id).toBe('0001');
  });
  it('rollback do receptor desfaz recibo e escrita quando writer falha', async () => {
    await ingest();
    const failing: TransactionalBusinessWriter = { ...fixtureWriter,async writeInTransaction(c,r) { await fixtureWriter.writeInTransaction(c,r); throw new Error('fail-after-first-write'); } };
    expect(await dispatcher(new PostgresDeliveryTarget(receiver,config.destinationId,failing)).runOnce()).toBe('pending');
    expect((await destination.admin.query('SELECT count(*)::text n FROM olos_export.delivery_receipts')).rows[0].n).toBe('0');
    expect((await destination.admin.query('SELECT count(*)::text n FROM olos_export.fixture_calls')).rows[0].n).toBe('0');
  });
  it('roles não podem alterar snapshot, aprovar contrato ou escrever em colunas não concedidas', async () => {
    await ingest();
    await expect(projector.query("UPDATE olos_export.projection_outbox SET payload='{}'::jsonb")).rejects.toMatchObject({ code: '42501' });
    await expect(ingester.query("UPDATE olos_export.projection_outbox SET state='confirmed'")).rejects.toMatchObject({ code: '42501' });
    await expect(projector.query('UPDATE olos_export.source_contracts SET approved=true')).rejects.toMatchObject({ code: '42501' });
    await expect(receiver.query('DELETE FROM olos_export.delivery_receipts')).rejects.toMatchObject({ code: '42501' });
  });
  it('projeção rejeitada fica terminal sem tocar o destino', async () => {
    await ingest(); const t = target(); const send = vi.spyOn(t,'deliver');
    const invalidPlan = { ...fixturePlan,bindings: [...fixturePlan.bindings,fixturePlan.bindings[0]!] };
    const d = new ProjectionDispatcher(outbox,t,m => projectCall(m.payload,{},invalidPlan));
    expect(await d.runOnce()).toBe('rejected'); expect(send).not.toHaveBeenCalled();
    expect((await saved())[0].state).toBe('rejected'); expect(await d.runOnce()).toBe('empty');
  });
  it('lease expirada não libera transporte remoto ainda pendente', async () => {
    await ingest(); let unblock!: () => void; let entered!: () => void;
    const began = new Promise<void>(r => { entered = r; }); const wait = new Promise<void>(r => { unblock = r; });
    const t = target(), send = t.deliver.bind(t);
    const spy = vi.spyOn(t,'deliver').mockImplementation(async request => { entered(); await wait; return send(request); });
    const first = dispatcher(t).runOnce(); await began;
    try {
      await source.admin.query("UPDATE olos_export.projection_outbox SET lease_until=clock_timestamp()-interval '1 second'");
      expect(await dispatcher(t).runOnce()).toBe('empty'); expect(spy).toHaveBeenCalledTimes(1);
    } finally { unblock(); }
    expect(await first).toBe('uncertain');
    expect(await dispatcher().reconcile((await saved())[0].delivery_id)).toBe(true);
    expect((await destination.admin.query('SELECT writes FROM olos_export.fixture_calls')).rows[0].writes).toBe(1);
  });
  it.each([true,false])('ACK local após entrega perdida é recuperado pelo recibo (commitou=%s)', async committed => {
    await ingest();
    const unstable = new ProjectionOutbox(loseCommitAck(projector,committed,3),config.destinationId,60_000,1);
    expect(await dispatcher(target(),unstable).runOnce()).toBe('uncertain');
    const state = (await saved())[0];
    if (committed) expect(state.state).toBe('confirmed');
    else {
      expect(state.state).toBe('uncertain');
      await source.admin.query("UPDATE olos_export.projection_outbox SET lease_until=clock_timestamp()-interval '1 second'");
      expect(await dispatcher().reconcile(state.delivery_id)).toBe(true);
    }
    expect((await destination.admin.query('SELECT writes FROM olos_export.fixture_calls')).rows[0].writes).toBe(1);
  });
  it.each([true,false])('ACK da autorização perdido não permite enviar ao destino (commitou=%s)', async committed => {
    await ingest(); const t = target(); const send = vi.spyOn(t,'deliver');
    const unstable = new ProjectionOutbox(loseCommitAck(projector,committed,2),config.destinationId,60_000,1);
    expect(await dispatcher(t,unstable).runOnce()).toBe('uncertain'); expect(send).not.toHaveBeenCalled();
    expect((await saved())[0].state).toBe(committed ? 'uncertain' : 'pending');
    expect((await destination.admin.query('SELECT count(*)::text n FROM olos_export.fixture_calls')).rows[0].n).toBe('0');
  });
  it('retry usa snapshot da requisição original, sem remapear referências', async () => {
    await ingest(); const unavailable = vi.spyOn(receiver,'connect').mockRejectedValue(new Error('offline'));
    expect(await dispatcher().runOnce()).toBe('pending'); unavailable.mockRestore(); await makeAvailable();
    const reproject = vi.fn(() => { throw new Error('must not reproject'); });
    expect(await new ProjectionDispatcher(outbox,target(),reproject).runOnce()).toBe('confirmed');
    expect(reproject).not.toHaveBeenCalled();
  });

});
