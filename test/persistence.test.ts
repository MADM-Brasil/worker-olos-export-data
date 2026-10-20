import { randomUUID } from 'node:crypto';
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';
import type pg from 'pg';
import { PostgresPersistence } from '../src/persistence/store.js';
import { type RunHandle, type PersistenceBatch, type FieldDefinition } from '../src/persistence/types.js';
import { isolatedPostgres } from './support/postgres.js';

// Contrato SINTÉTICO: nenhum desses campos comprova o layout da Olos.
const fields: Record<string, FieldDefinition> = {
  FixtureId: { type: 'int64', nullable: false }, FixtureText: { type: 'text', nullable: true },
  FixtureLocal: { type: 'local_timestamp', nullable: true }, FixtureOffset: { type: 'offset_timestamp', nullable: true },
  FixtureAmount: { type: 'decimal', nullable: true },
};
const record = (id = '9007199254740993', text: string | null = null) => ({ FixtureId: id, FixtureText: text,
  FixtureLocal: '2026-10-15T09:30:00.1234567', FixtureOffset: '2026-10-15T09:30:00.1234567-03:00', FixtureAmount: '12345678901234567890.1234000' });
let fixture: Awaited<ReturnType<typeof isolatedPostgres>>;
let pool: pg.Pool;
let store: PostgresPersistence;
const signal = () => new AbortController().signal;
const start = () => store.startRun({ table: 'AttemptsRawData', contractVersion: 'fixture-v1', readType: 'investigation', exportDate: '2026-10-15', pkRange: null }, signal());
const batch = (records: PersistenceBatch['records'] = [record()], expected = { version: '0', cursor: null as string | null }, nextCursor: string | null = 'fixture:1'): PersistenceBatch => ({ id: randomUUID(), records, expected, nextCursor });
async function persist(run: RunHandle, value: PersistenceBatch) { await store.observeBatch(run, value); return store.persistBatch(run, value); }
async function rowCount(table = 'attempts_raw_data') {
  // Identificadores constantes de teste; nenhuma entrada externa.
  const allowed = ['attempts_raw_data','batches','audit_events','sync_errors','recovery_cases'];
  if (!allowed.includes(table)) throw new Error('Invalid test identifier');
  return Number((await fixture.admin.query(`SELECT count(*) AS n FROM olos_export.${table}`)).rows[0].n);
}

beforeAll(async () => { fixture = await isolatedPostgres(); }, 150_000);
afterAll(async () => { await fixture?.stop(); }, 30_000);
beforeEach(async () => {
  // Exclusivamente o banco descartável criado pelo helper, nunca PG_HOST/PGDATABASE da máquina.
  await fixture.admin.query('TRUNCATE olos_export.source_contracts, olos_export.streams, olos_export.source_locks CASCADE');
  await fixture.admin.query('ALTER TABLE olos_export.attempts_raw_data DROP CONSTRAINT IF EXISTS reject_fixture');
  await fixture.admin.query('ALTER TABLE olos_export.checkpoints DROP CONSTRAINT IF EXISTS reject_checkpoint');
  await fixture.admin.query(`INSERT INTO olos_export.source_contracts(source_table,version,approved,fields,key_fields,layout_evidence,key_evidence)
    VALUES ('AttemptsRawData','fixture-v1',true,$1::jsonb,ARRAY['FixtureId'],'SYNTHETIC TEST ONLY','FixtureId unique in synthetic fixture only')`, [JSON.stringify(fields)]);
  await fixture.admin.query(`INSERT INTO olos_export.source_contracts(source_table,version) VALUES
    ('InboundRawData','pending'),('PbxBillingData','pending'),('Users','pending'),('Campaign','pending'),('Disposition','pending')`);
  await fixture.admin.query('INSERT INTO olos_export.source_locks(source_table) SELECT source_table FROM olos_export.source_contracts');
  pool = fixture.worker();
  store = new PostgresPersistence(pool, { chunkSize: 1 });
});
afterEach(async () => { vi.restoreAllMocks(); await pool?.end(); });

describe('persistência transacional em PostgreSQL 16 isolado', () => {
  it('insere lote, checkpoint, contagens e auditoria atomicamente', async () => {
    const run = await start();
    const value = batch();
    expect(await persist(run, value)).toEqual({ received: 1, persisted: 1, duplicates: 0, rejected: 0 });
    expect(await store.checkpoint(run)).toEqual({ version: '1', cursor: 'fixture:1' });
    expect(await rowCount()).toBe(1);
    expect(await rowCount('audit_events')).toBe(1);
    await store.finishRun(run);
    const saved = (await fixture.admin.query('SELECT * FROM olos_export.runs')).rows[0];
    expect(saved.state).toBe('succeeded');
    expect(saved.persisted_count).toBe('1');
    expect((await fixture.admin.query('SELECT last_success_at IS NOT NULL AS ok FROM olos_export.streams')).rows[0].ok).toBe(true);
  });
  it('reprocessa lote confirmado sem writes, nova contagem ou regressão do checkpoint', async () => {
    const run = await start(), first = batch();
    const receipt = await persist(run, first);
    const second = batch([record('2')], { version: '1', cursor: 'fixture:1' }, 'fixture:2');
    await persist(run, second);
    await store.finishRun(run);
    const afterRestart = new PostgresPersistence(pool);
    expect(await afterRestart.persistBatch(run, first)).toEqual(receipt);
    const cp = (await fixture.admin.query('SELECT version,cursor_value FROM olos_export.checkpoints')).rows[0];
    expect(cp).toEqual({ version: '2', cursor_value: 'fixture:2' });
    expect(await rowCount('audit_events')).toBe(2);
  });
  it('deduplica registros iguais dentro do lote e em lotes diferentes', async () => {
    const run = await start();
    expect(await persist(run, batch([record(), record()]))).toEqual({ received: 2, persisted: 1, duplicates: 1, rejected: 0 });
    expect(await persist(run, batch([record()], { version: '1', cursor: 'fixture:1' }, 'fixture:2'))).toEqual({ received: 1, persisted: 0, duplicates: 1, rejected: 0 });
    expect(await rowCount()).toBe(1);
  });
  it('faz UPSERT de payload alterado com chave sintética comprovada e checkpoint válido', async () => {
    const run = await start();
    await persist(run, batch());
    expect((await persist(run, batch([record('9007199254740993','changed')], { version: '1', cursor: 'fixture:1' }))).persisted).toBe(1);
    const payload = (await fixture.admin.query('SELECT payload FROM olos_export.attempts_raw_data')).rows[0].payload;
    expect(payload.FixtureText).toBe('changed');
    expect(await rowCount()).toBe(1);
  });
  it('rejeita duplicatas conflitantes sem escolher arbitrariamente um valor', async () => {
    const run = await start();
    await expect(store.observeBatch(run, batch([record('1','a'),record('1','b')]))).rejects.toMatchObject({ code: 'VALIDATION_FAILED' });
    expect((await fixture.admin.query('SELECT rejected_count FROM olos_export.sync_errors')).rows[0].rejected_count).toBe(2);
    expect(await rowCount()).toBe(0);
    expect((await store.checkpoint(run)).version).toBe('0');
  });
  it('rollback após primeira parcela e reprocessamento do mesmo lote sem perda local', async () => {
    const run = await start(), value = batch([record('1'),record('2','reject')]);
    await store.observeBatch(run, value);
    await fixture.admin.query("ALTER TABLE olos_export.attempts_raw_data ADD CONSTRAINT reject_fixture CHECK (payload->>'FixtureText' IS DISTINCT FROM 'reject')");
    await expect(store.persistBatch(run, value)).rejects.toMatchObject({ code: 'PERSISTENCE_FAILED', outcome: 'not_committed' });
    expect(await rowCount()).toBe(0);
    expect(await rowCount('audit_events')).toBe(0);
    expect((await store.checkpoint(run)).version).toBe('0');
    const failed = (await fixture.admin.query('SELECT state,rejected_count FROM olos_export.batches')).rows[0];
    expect(failed).toEqual({ state: 'failed', rejected_count: 2 });
    expect(await rowCount('sync_errors')).toBe(1);
    await fixture.admin.query('ALTER TABLE olos_export.attempts_raw_data DROP CONSTRAINT reject_fixture');
    expect(await store.persistBatch(run, value)).toEqual({ received: 2, persisted: 2, duplicates: 0, rejected: 0 });
    expect((await fixture.admin.query('SELECT rejected_count FROM olos_export.runs')).rows[0].rejected_count).toBe('0');
  });
  it('preserva BIGINT, DECIMAL, precisão temporal e timezone sem conversão Date/number', async () => {
    const run = await start();
    await persist(run, batch([record('9223372036854775807'),record('-9223372036854775808')]));
    const rows = (await fixture.admin.query('SELECT payload,source_key FROM olos_export.attempts_raw_data ORDER BY source_key')).rows;
    expect(rows.map(r => r.payload.FixtureId).sort()).toEqual(['-9223372036854775808','9223372036854775807'].sort());
    expect(rows[0].payload.FixtureLocal).toBe('2026-10-15T09:30:00.1234567');
    expect(rows[0].payload.FixtureOffset).toBe('2026-10-15T09:30:00.1234567-03:00');
    expect(rows[0].payload.FixtureAmount).toBe('12345678901234567890.1234000');
  });
  it('preserva null e diferencia string vazia', async () => {
    const run = await start();
    await persist(run, batch([record('1',null),record('2','')]));
    const rows = (await fixture.admin.query("SELECT payload->>'FixtureId' AS id,payload FROM olos_export.attempts_raw_data ORDER BY payload->>'FixtureId'")).rows;
    expect(rows[0].payload.FixtureText).toBeNull();
    expect(rows[1].payload.FixtureText).toBe('');
  });
  it.each([9007199254740992, '9223372036854775808', null])('rejeita ID inválido %s, sem arredondamento ou checkpoint', async id => {
    const run = await start();
    await expect(store.observeBatch(run, batch([{ ...record(), FixtureId: id }]))).rejects.toMatchObject({ code: 'VALIDATION_FAILED' });
    expect(await rowCount()).toBe(0);
    expect((await store.checkpoint(run)).version).toBe('0');
    expect((await fixture.admin.query('SELECT received_count,rejected_count FROM olos_export.sync_errors')).rows[0]).toEqual({ received_count: 1, rejected_count: 1 });
  });
  it('não aceita passwords mesmo com contrato aprovado indevidamente', async () => {
    await fixture.admin.query(`UPDATE olos_export.source_contracts SET fields=fields||'{"Password":{"type":"text","nullable":true}}'::jsonb WHERE source_table='AttemptsRawData'`);
    await expect(start()).rejects.toMatchObject({ code: 'VALIDATION_FAILED' });
    // Defesa no banco: o envelope também recusa payload aninhado e variantes de senha.
    expect((await fixture.admin.query(`SELECT olos_export.safe_payload('{"Pass_word":"secret"}'::jsonb) AS ok`)).rows[0].ok).toBe(false);
    expect((await fixture.admin.query(`SELECT olos_export.safe_payload('{"profile":{"Password":"secret"}}'::jsonb) AS ok`)).rows[0].ok).toBe(false);
  });
  it('bloqueia todas as tabelas sem layout/chave aprovados', async () => {
    for (const table of ['Users','Campaign','Disposition','PbxBillingData','InboundRawData'] as const) {
      await expect(store.startRun({ table,contractVersion: 'pending',readType: 'investigation',exportDate: '2026-10-15',pkRange: null }, signal())).rejects.toMatchObject({ code: 'CONTRACT_UNCONFIRMED' });
    }
    expect(await rowCount()).toBe(0);
  });
  it('recusa o mesmo batch ID com conteúdo diferente sem modificar o recibo', async () => {
    const run = await start(), original = batch();
    await persist(run, original);
    await expect(store.persistBatch(run, { ...original, records: [record('2')] })).rejects.toMatchObject({ code: 'BATCH_CONFLICT' });
    expect(await rowCount()).toBe(1);
    expect(await rowCount('audit_events')).toBe(1);
  });
  it('detecta cursor anterior incompatível e versão antiga', async () => {
    const run = await start();
    await persist(run, batch());
    await expect(store.observeBatch(run, batch([record('2')], { version: '1', cursor: 'wrong' }))).rejects.toMatchObject({ code: 'CHECKPOINT_CONFLICT' });
    await expect(store.observeBatch(run, batch([record('2')], { version: '0', cursor: null }))).rejects.toMatchObject({ code: 'CHECKPOINT_CONFLICT' });
    expect(await rowCount()).toBe(1);
    expect((await store.checkpoint(run)).version).toBe('1');
  });
  it('rollback dos registros se checkpoint mudou depois da observação', async () => {
    const run = await start(), value = batch();
    await store.observeBatch(run, value);
    await fixture.admin.query("UPDATE olos_export.checkpoints SET cursor_value='tampered'");
    await expect(store.persistBatch(run, value)).rejects.toMatchObject({ code: 'CHECKPOINT_CONFLICT' });
    expect(await rowCount()).toBe(0);
    expect(await rowCount('audit_events')).toBe(0);
  });
  it('impede duas instâncias de adquirir a mesma tabela, inclusive datas diferentes', async () => {
    const other = new PostgresPersistence(pool);
    const results = await Promise.allSettled([start(), other.startRun({ table: 'AttemptsRawData',contractVersion: 'fixture-v1',readType: 'investigation',exportDate: '2026-10-14',pkRange: null }, signal())]);
    expect(results.filter(r => r.status === 'fulfilled')).toHaveLength(1);
    expect(results.filter(r => r.status === 'rejected')).toHaveLength(1);
  });
  it('duas persistências concorrentes não duplicam dados/auditoria/checkpoint', async () => {
    const run = await start(), value = batch();
    await store.observeBatch(run, value);
    const other = new PostgresPersistence(pool);
    const results = await Promise.allSettled([store.persistBatch(run, value),other.persistBatch(run, value)]);
    expect(results.some(r => r.status === 'fulfilled')).toBe(true);
    for (const result of results) if (result.status === 'rejected') expect(result.reason.code).toBe('BUSY');
    expect(await rowCount()).toBe(1);
    expect(await rowCount('audit_events')).toBe(1);
    expect((await store.checkpoint(run)).version).toBe('1');
  });
  it('fencing e cancelamento impedem writes de execução sem posse', async () => {
    const run = await start(), value = batch();
    await store.observeBatch(run, value);
    await expect(store.persistBatch({ ...run,fencingToken: '999' }, value)).rejects.toMatchObject({ code: 'LEASE_LOST' });
    const controller = new AbortController(); controller.abort();
    await expect(store.persistBatch({ ...run,signal: controller.signal }, value)).rejects.toMatchObject({ code: 'LEASE_LOST' });
    expect(await rowCount()).toBe(0);
  });
  it('renova lease sem alterar geração e não renova lease expirada', async () => {
    const run = await start();
    await store.renew(run);
    expect((await fixture.admin.query('SELECT generation FROM olos_export.source_locks WHERE source_table=$1',['AttemptsRawData'])).rows[0].generation).toBe(run.fencingToken);
    await fixture.admin.query("UPDATE olos_export.source_locks SET lease_until=clock_timestamp()-interval '1 second' WHERE owner_run_id=$1", [run.runId]);
    await expect(store.renew(run)).rejects.toMatchObject({ code: 'LEASE_LOST' });
  });
  it.each([false,true])('reinicialização após leitura sem persistência (lote observado=%s) exige conciliação', async observed => {
    const run = await start(), value = batch();
    if (observed) await store.observeBatch(run, value);
    await fixture.admin.query("UPDATE olos_export.source_locks SET lease_until=clock_timestamp()-interval '1 second' WHERE owner_run_id=$1", [run.runId]);
    const restarted = new PostgresPersistence(pool);
    const cases = await restarted.recoverExpired();
    expect(cases).toHaveLength(1);
    expect(await restarted.recoverExpired()).toHaveLength(0);
    expect((await fixture.admin.query('SELECT state FROM olos_export.runs')).rows[0].state).toBe('needs_reconciliation');
    expect(await rowCount()).toBe(0);
    await expect(start()).rejects.toMatchObject({ code: 'RECOVERY_REQUIRED' });
    await expect(store.observeBatch(run, value)).rejects.toMatchObject({ code: 'LEASE_LOST' });
    await restarted.recordRecoveryAttempt(cases[0]!, 'inspect_log','inconclusive',null);
    expect((await fixture.admin.query('SELECT attempts,state FROM olos_export.recovery_cases')).rows[0]).toEqual({ attempts: 1,state: 'open' });
  });
  it('não recupera execução com lease ativa ou bloqueada por transação em andamento', async () => {
    const run = await start();
    expect(await store.recoverExpired()).toEqual([]);
    await fixture.admin.query("UPDATE olos_export.source_locks SET lease_until=clock_timestamp()-interval '1 second' WHERE owner_run_id=$1", [run.runId]);
    const lock = await fixture.admin.connect();
    try {
      await lock.query('BEGIN');
      await lock.query('SELECT * FROM olos_export.source_locks FOR UPDATE');
      expect(await store.recoverExpired()).toEqual([]);
    } finally { await lock.query('ROLLBACK'); lock.release(); }
    expect(await store.recoverExpired()).toHaveLength(1);
  });
  it('não marca sucesso sem lote ou com lote pendente', async () => {
    const run = await start();
    await expect(store.finishRun(run)).rejects.toMatchObject({ code: 'RECOVERY_REQUIRED' });
    await store.observeBatch(run, batch());
    await expect(store.finishRun(run)).rejects.toMatchObject({ code: 'RECOVERY_REQUIRED' });
  });
  it('respeita limites de registros e bytes antes de gravar payload', async () => {
    const run = await start();
    const limited = new PostgresPersistence(pool, { maxRecords: 1,chunkSize: 1,maxBytes: 1024 });
    await expect(limited.observeBatch(run, batch([record('1'),record('2')]))).rejects.toMatchObject({ code: 'VALIDATION_FAILED' });
    await expect(limited.observeBatch(run, batch([record('1','x'.repeat(2000))]))).rejects.toMatchObject({ code: 'VALIDATION_FAILED' });
    expect(await rowCount()).toBe(0);
  });
  it('usuário do worker não cria/apaga/trunca tabelas nem aprova contratos', async () => {
    const user = (await pool.query('SELECT current_user')).rows[0].current_user;
    expect(user).toBe('olos_export_worker');
    for (const sql of ['CREATE TABLE olos_export.forbidden(id int)', 'DELETE FROM olos_export.attempts_raw_data',
      'TRUNCATE olos_export.attempts_raw_data', 'ALTER TABLE olos_export.attempts_raw_data ADD COLUMN forbidden text',
      'UPDATE olos_export.source_contracts SET approved=true']) {
      await expect(pool.query(sql)).rejects.toMatchObject({ code: '42501' });
    }
  });
  it('falha de conexão preserva intent observado e permite repetir sem duplicar', async () => {
    const run = await start(), value = batch();
    await store.observeBatch(run, value);
    const unavailable = vi.spyOn(pool,'connect').mockRejectedValue(new Error('private-connection-string'));
    await expect(store.persistBatch(run, value)).rejects.toMatchObject({ code: 'PERSISTENCE_FAILED', outcome: 'not_committed' });
    unavailable.mockRestore();
    expect(await rowCount()).toBe(0);
    expect((await fixture.admin.query('SELECT state FROM olos_export.batches')).rows[0].state).toBe('read_observed');
    expect((await store.persistBatch(run, value)).persisted).toBe(1);
  });
  it.each([false,true])('COMMIT sem confirmação fica UNKNOWN (commit chegou ao servidor=%s)', async serverCommitted => {
    const run = await start(), value = batch();
    await store.observeBatch(run, value);
    const client = await pool.connect();
    const proxy = {
      async query(sql: string, values?: unknown[]) {
        if (sql === 'COMMIT') {
          if (serverCommitted) await client.query(sql);
          throw new Error('lost-ack-sensitive-driver-message');
        }
        return client.query(sql, values);
      },
      release: (destroy?: boolean) => client.release(destroy),
    };
    let first = true;
    const broken = { connect: async () => { if (first) { first = false; return proxy; } return pool.connect(); } } as unknown as Pick<pg.Pool,'connect'>;
    const ambiguous = new PostgresPersistence(broken);
    await expect(ambiguous.persistBatch(run, value)).rejects.toMatchObject({ code: 'COMMIT_UNKNOWN', outcome: 'unknown' });
    expect(await rowCount()).toBe(serverCommitted ? 1 : 0);
    const error = (await fixture.admin.query('SELECT code,outcome,rejected_count FROM olos_export.sync_errors')).rows[0];
    expect(error).toEqual({ code: 'COMMIT_UNKNOWN',outcome: 'unknown',rejected_count: 0 });
    if (serverCommitted) {
      expect((await store.persistBatch(run, value)).persisted).toBe(1);
      expect(await rowCount('audit_events')).toBe(1);
    } else {
      expect(await rowCount('recovery_cases')).toBe(1);
      await expect(store.persistBatch(run, value)).rejects.toMatchObject({ code: 'RECOVERY_REQUIRED' });
      expect((await fixture.admin.query('SELECT version FROM olos_export.checkpoints')).rows[0].version).toBe('0');
    }
  });
  it('falha do checkpoint após UPSERT também faz rollback dos dados e auditoria', async () => {
    const run = await start(), value = batch();
    await store.observeBatch(run, value);
    await fixture.admin.query('ALTER TABLE olos_export.checkpoints ADD CONSTRAINT reject_checkpoint CHECK (version=0)');
    await expect(store.persistBatch(run, value)).rejects.toMatchObject({ code: 'PERSISTENCE_FAILED' });
    expect(await rowCount()).toBe(0);
    expect(await rowCount('audit_events')).toBe(0);
    expect((await store.checkpoint(run)).version).toBe('0');
  });
  it('execução A antiga não escreve nem libera posse da execução B', async () => {
    const a = await start(), old = batch();
    await store.observeBatch(a, old);
    await fixture.admin.query("UPDATE olos_export.source_locks SET lease_until=clock_timestamp()-interval '1 second' WHERE owner_run_id=$1", [a.runId]);
    await store.recoverExpired();
    // Somente fixture: simula decisão humana com evidência, não implementada como API no worker.
    await fixture.admin.query("UPDATE olos_export.recovery_cases SET state='resolved',resolved_at=clock_timestamp()");
    const b = await start();
    await persist(b, batch([record('2','current')]));
    await expect(store.persistBatch(a, old)).rejects.toMatchObject({ code: 'LEASE_LOST' });
    await expect(store.finishRun(a)).rejects.toMatchObject({ code: 'LEASE_LOST' });
    const lock = (await fixture.admin.query('SELECT owner_run_id FROM olos_export.source_locks WHERE source_table=$1',['AttemptsRawData'])).rows[0];
    expect(lock.owner_run_id).toBe(b.runId);
    expect((await fixture.admin.query('SELECT payload FROM olos_export.attempts_raw_data')).rows[0].payload.FixtureText).toBe('current');
    expect(await rowCount()).toBe(1);
    expect((await store.checkpoint(b)).version).toBe('1');
  });

  it('preserva metadata de exportação, intervalo de PK e geração acima de MAX_SAFE_INTEGER', async () => {
    await fixture.admin.query("UPDATE olos_export.source_locks SET generation='9007199254740992' WHERE source_table='AttemptsRawData'");
    const run = await store.startRun({ table: 'AttemptsRawData',contractVersion: 'fixture-v1',readType: 'previous_days',
      exportDate: '2026-10-14',pkRange: { lower: '9007199254740993',upper: '9223372036854775807' } }, signal());
    expect(run.fencingToken).toBe('9007199254740993');
    const saved = (await fixture.admin.query(`SELECT r.pk_range,s.read_type,s.export_date::text FROM olos_export.runs r JOIN olos_export.streams s ON s.id=r.stream_id`)).rows[0];
    expect(saved).toEqual({ pk_range: { lower: '9007199254740993',upper: '9223372036854775807' },read_type: 'previous_days',export_date: '2026-10-14' });
  });
  it('migrações não concedem aprovação real nem permissões implícitas para outro schema', async () => {
    const rows = (await fixture.admin.query("SELECT source_table,approved,key_fields FROM olos_export.source_contracts WHERE version='pending' ORDER BY source_table")).rows;
    expect(rows).toHaveLength(5); // AttemptsRawData usa exclusivamente contrato sintético do fixture.
    for (const row of rows) { expect(row.approved).toBe(false); expect(row.key_fields).toEqual([]); }
    expect((await pool.query("SELECT has_schema_privilege(current_user,'olos_export','CREATE') AS allowed")).rows[0].allowed).toBe(false);
  });

  it('não inicia intent se versões revisadas de schema estão incompletas', async () => {
    await fixture.admin.query('DELETE FROM olos_export.schema_versions WHERE version=2');
    try {
      await expect(start()).rejects.toMatchObject({ code: 'SCHEMA_NOT_READY' });
      expect((await fixture.admin.query('SELECT count(*)::text AS n FROM olos_export.runs')).rows[0].n).toBe('0');
    } finally { await fixture.admin.query('INSERT INTO olos_export.schema_versions(version) VALUES (2)'); }
  });

});
