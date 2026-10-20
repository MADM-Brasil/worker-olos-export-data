import { randomUUID } from 'node:crypto';
import type { Pool, PoolClient } from 'pg';
import { PersistenceError, defaultLimits, type BatchCounts, type Checkpoint, type PersistenceBatch, type PersistenceLimits,
  type RunHandle, type SourceContract, type StartRun } from './types.js';
import { assertContract, assertTable, assertUuid, assertVersion, validDate, targets, prepareRecords, batchDigest, deduplicate } from './validation.js';
import { transaction } from './transaction.js';
import { captureBatch, captureConfig, verifyCapture, type CaptureConfig } from '../projection/capture.js';

interface RunRow {
  id: string; stream_id: string; source_table: SourceContract['source_table']; contract_version: string;
  fencing_token: string; state: string; owner_run_id: string | null; generation: string; lease_valid: boolean;
}
interface BatchRow {
  id: string; run_id: string; stream_id: string; source_table: SourceContract['source_table']; digest: string;
  state: string; received_count: number; persisted_count: number; duplicate_count: number; rejected_count: number;
}
const counts = (b: BatchRow): BatchCounts => ({ received: b.received_count, persisted: b.persisted_count, duplicates: b.duplicate_count, rejected: b.rejected_count });
const readTypes = ['current_day','previous_days','reread','reference_snapshot','investigation'];

/** Adaptador explícito e injetável. O boot HTTP não importa ou instancia este módulo. */
export class PostgresPersistence {
  readonly limits: PersistenceLimits;
  private readonly projection: Readonly<CaptureConfig> | undefined;
  constructor(private readonly pool: Pick<Pool, 'connect'>, limits: Partial<PersistenceLimits> = {}, projection?: CaptureConfig) {
    this.projection = projection === undefined ? undefined : captureConfig(projection);
    this.limits = Object.freeze({ ...defaultLimits, ...limits });
    for (const value of Object.values(this.limits)) if (!Number.isSafeInteger(value) || value < 1) throw new PersistenceError('VALIDATION_FAILED');
    if (this.limits.chunkSize > this.limits.maxRecords || this.limits.maxRecords > 100_000 || this.limits.maxBytes > 64 * 1024 * 1024 || this.limits.leaseMs > 300_000) throw new PersistenceError('VALIDATION_FAILED');
  }
  private async contract(client: PoolClient, table: string, version: string): Promise<SourceContract> {
    const result = await client.query<SourceContract>('SELECT * FROM olos_export.source_contracts WHERE source_table=$1 AND version=$2', [table, version]);
    const c = result.rows[0];
    if (!c) throw new PersistenceError('CONTRACT_UNCONFIRMED');
    assertContract(c);
    return c;
  }
  private handle(handle: RunHandle): void {
    assertUuid(handle.runId); assertUuid(handle.streamId); assertVersion(handle.fencingToken);
    if (handle.signal.aborted) throw new PersistenceError('LEASE_LOST');
  }
  private async guard(client: PoolClient, handle: RunHandle): Promise<RunRow> {
    this.handle(handle);
    const result = await client.query<RunRow>(`SELECT r.*, l.owner_run_id, l.generation,
      (l.lease_until > clock_timestamp()) AS lease_valid
      FROM olos_export.runs r JOIN olos_export.source_locks l ON l.source_table=r.source_table
      WHERE r.id=$1 FOR UPDATE OF l NOWAIT`, [handle.runId]);
    const row = result.rows[0];
    if (!row || row.stream_id !== handle.streamId || row.owner_run_id !== handle.runId || row.generation !== handle.fencingToken ||
      row.fencing_token !== handle.fencingToken || !row.lease_valid) throw new PersistenceError('LEASE_LOST');
    if (!['read_intent','persisting'].includes(row.state)) throw new PersistenceError('RECOVERY_REQUIRED');
    return row;
  }
  private snapshot(input: PersistenceBatch): PersistenceBatch {
    try {
      const batch = structuredClone(input);
      assertUuid(batch.id); assertVersion(batch.expected.version);
      if (!Array.isArray(batch.records) || batch.records.length > this.limits.maxRecords) throw new Error();
      for (const cursor of [batch.expected.cursor, batch.nextCursor]) if (cursor !== null && (typeof cursor !== 'string' || cursor.length > 2048)) throw new Error();
      return batch;
    } catch { throw new PersistenceError('VALIDATION_FAILED'); }
  }
  private async checkCheckpoint(client: PoolClient, handle: RunHandle, expected: Checkpoint): Promise<void> {
    const result = await client.query<{ version: string; cursor_value: string | null }>(
      'SELECT version, cursor_value FROM olos_export.checkpoints WHERE stream_id=$1 FOR UPDATE', [handle.streamId]);
    const cp = result.rows[0];
    if (!cp || cp.version !== expected.version || cp.cursor_value !== expected.cursor) throw new PersistenceError('CHECKPOINT_CONFLICT');
  }
  private async aggregate(client: PoolClient, runId: string): Promise<void> {
    await client.query(`UPDATE olos_export.runs SET
      received_count=(SELECT coalesce(sum(received_count),0) FROM olos_export.batches WHERE run_id=$1)
        +(SELECT coalesce(sum(received_count),0) FROM olos_export.sync_errors WHERE run_id=$1 AND batch_id IS NULL),
      persisted_count=(SELECT coalesce(sum(persisted_count),0) FROM olos_export.batches WHERE run_id=$1),
      duplicate_count=(SELECT coalesce(sum(duplicate_count),0) FROM olos_export.batches WHERE run_id=$1),
      rejected_count=(SELECT coalesce(sum(rejected_count),0) FROM olos_export.batches WHERE run_id=$1)
        +(SELECT coalesce(sum(rejected_count),0) FROM olos_export.sync_errors WHERE run_id=$1 AND batch_id IS NULL)
      WHERE id=$1`, [runId]);
  }

  // Gravar ANTES de qualquer futura leitura remota; não há leitura remota neste adaptador.
  async startRun(input: StartRun, signal: AbortSignal): Promise<RunHandle> {
    assertTable(input.table);
    if (!validDate(input.exportDate) || !readTypes.includes(input.readType) || typeof input.contractVersion !== 'string' || !input.contractVersion || input.contractVersion.length > 128 || signal.aborted ||
      (input.pkRange !== null && (typeof input.pkRange !== 'object' || Array.isArray(input.pkRange) ||
        Object.getPrototypeOf(input.pkRange) !== Object.prototype || Object.keys(input.pkRange).length > 32 ||
        Object.entries(input.pkRange).some(([k,v]) => k.length > 128 || /password|passwd|senha/.test(k.toLowerCase().replace(/[^a-z]/g,'')) || typeof v !== 'string' || v.length > 2048)))) throw new PersistenceError('VALIDATION_FAILED');
    const spec = structuredClone(input);
    return transaction(this.pool, async client => {
      const versions = await client.query<{ version: number }>('SELECT version FROM olos_export.schema_versions ORDER BY version');
      if (versions.rows.length !== 2 || versions.rows[0]?.version !== 1 || versions.rows[1]?.version !== 2) throw new PersistenceError('SCHEMA_NOT_READY');
      await this.contract(client, spec.table, spec.contractVersion);
      const lock = (await client.query<{ owner_run_id: string | null; generation: string }>(
        'SELECT owner_run_id, generation FROM olos_export.source_locks WHERE source_table=$1 FOR UPDATE NOWAIT', [spec.table])).rows[0];
      if (!lock) throw new PersistenceError('CONTRACT_UNCONFIRMED');
      if (lock.owner_run_id) throw new PersistenceError('BUSY'); // Expiração exige recoverExpired(), nunca takeover silencioso.
      const unresolved = await client.query(`SELECT 1 FROM olos_export.recovery_cases c
        JOIN olos_export.runs r ON r.id=c.run_id WHERE r.source_table=$1 AND c.state='open' LIMIT 1`, [spec.table]);
      if (unresolved.rowCount) throw new PersistenceError('RECOVERY_REQUIRED');
      const stream = (await client.query<{ id: string }>(`INSERT INTO olos_export.streams(id,source_table,read_type,export_date)
        VALUES ($1,$2,$3,$4) ON CONFLICT (source_table,read_type,export_date)
        DO UPDATE SET id=olos_export.streams.id RETURNING id`, [randomUUID(), spec.table, spec.readType, spec.exportDate])).rows[0]!;
      await client.query('INSERT INTO olos_export.checkpoints(stream_id) VALUES ($1) ON CONFLICT (stream_id) DO NOTHING', [stream.id]);
      const runId = randomUUID(), token = (BigInt(lock.generation) + 1n).toString();
      await client.query(`INSERT INTO olos_export.runs(id,stream_id,source_table,contract_version,fencing_token,state,pk_range)
        VALUES ($1,$2,$3,$4,$5,'read_intent',$6::jsonb)`, [runId,stream.id,spec.table,spec.contractVersion,token,spec.pkRange === null ? null : JSON.stringify(spec.pkRange)]);
      await client.query(`UPDATE olos_export.source_locks SET generation=$2,owner_run_id=$3,
        lease_until=clock_timestamp()+($4::double precision * interval '1 millisecond') WHERE source_table=$1`, [spec.table,token,runId,this.limits.leaseMs]);
      if (signal.aborted) throw new PersistenceError('LEASE_LOST');
      return { runId, streamId: stream.id, fencingToken: token, signal };
    });
  }

  async checkpoint(handle: RunHandle): Promise<Checkpoint> {
    return transaction(this.pool, async client => {
      await this.guard(client, handle);
      const cp = (await client.query<{ version: string; cursor_value: string | null }>('SELECT version,cursor_value FROM olos_export.checkpoints WHERE stream_id=$1', [handle.streamId])).rows[0]!;
      return { version: cp.version, cursor: cp.cursor_value };
    });
  }
  async renew(handle: RunHandle): Promise<void> {
    await transaction(this.pool, async client => {
      const run = await this.guard(client, handle);
      const renewed = await client.query(`UPDATE olos_export.source_locks SET lease_until=clock_timestamp()+($2::double precision * interval '1 millisecond')
        WHERE source_table=$1 AND lease_until>clock_timestamp()`, [run.source_table, this.limits.leaseMs]);
      if (renewed.rowCount !== 1 || handle.signal.aborted) throw new PersistenceError('LEASE_LOST');
    });
  }
  async observeBatch(handle: RunHandle, input: PersistenceBatch): Promise<void> {
    const batch = this.snapshot(input);
    try {
      await transaction(this.pool, async client => {
        const run = await this.guard(client, handle);
        const contract = await this.contract(client, run.source_table, run.contract_version);
        const { rows } = prepareRecords(batch.records, contract, this.limits);
        deduplicate(rows); // Duplicatas conflitantes não podem depender da ordem da entrada.
        const digest = batchDigest(batch, rows, run.contract_version);
        const prior = (await client.query<BatchRow>('SELECT * FROM olos_export.batches WHERE id=$1', [batch.id])).rows[0];
        if (prior) {
          if (prior.run_id !== handle.runId || prior.digest !== digest) throw new PersistenceError('BATCH_CONFLICT');
          return;
        }
        await this.checkCheckpoint(client, handle, batch.expected);
        await client.query(`INSERT INTO olos_export.batches(id,run_id,stream_id,source_table,digest,expected_version,previous_cursor,next_cursor,state,received_count)
          VALUES ($1,$2,$3,$4,$5,$6,$7,$8,'read_observed',$9)`,
          [batch.id,handle.runId,handle.streamId,run.source_table,digest,batch.expected.version,batch.expected.cursor,batch.nextCursor,rows.length]);
        await client.query("UPDATE olos_export.runs SET state='persisting' WHERE id=$1", [handle.runId]);
        await this.aggregate(client, handle.runId);
        await this.guard(client, handle);
      });
    } catch (error) {
      await this.recordFailure(handle, batch, error);
      throw error;
    }
  }

  async persistBatch(handle: RunHandle, input: PersistenceBatch): Promise<BatchCounts> {
    const batch = this.snapshot(input);
    try {
      return await transaction(this.pool, async client => {
        // Reenvio de lote comprovadamente committed retorna recibo sem escrever ou regredir checkpoint.
        const prior = (await client.query<BatchRow>('SELECT * FROM olos_export.batches WHERE id=$1', [batch.id])).rows[0];
        if (!prior || prior.run_id !== handle.runId || prior.stream_id !== handle.streamId) throw new PersistenceError('BATCH_CONFLICT');
        const run = (await client.query<RunRow>('SELECT * FROM olos_export.runs WHERE id=$1', [handle.runId])).rows[0]!;
        if (run.fencing_token !== handle.fencingToken) throw new PersistenceError('LEASE_LOST');
        const contract = await this.contract(client, run.source_table, run.contract_version);
        const { rows } = prepareRecords(batch.records, contract, this.limits);
        if (prior.digest !== batchDigest(batch, rows, run.contract_version)) throw new PersistenceError('BATCH_CONFLICT');
        if (prior.state === 'committed') {
          if (this.projection) await verifyCapture(client,batch.id,this.projection,deduplicate(rows));
          return counts(prior);
        }
        await this.guard(client, handle);
        // Reconsultar após lock: outro processo pode ter acabado de confirmar o mesmo lote.
        const locked = (await client.query<BatchRow>('SELECT * FROM olos_export.batches WHERE id=$1 FOR UPDATE', [batch.id])).rows[0]!;
        if (locked.state === 'committed') {
          if (this.projection) await verifyCapture(client,batch.id,this.projection,deduplicate(rows));
          return counts(locked);
        }
        if (!['read_observed','failed'].includes(locked.state)) throw new PersistenceError('RECOVERY_REQUIRED');
        await this.checkCheckpoint(client, handle, batch.expected);
        const unique = deduplicate(rows);
        let persisted = 0;
        for (let offset = 0; offset < unique.length; offset += this.limits.chunkSize) {
          const chunk = unique.slice(offset, offset + this.limits.chunkSize);
          // Identificador vem exclusivamente do mapa fixo; todos os valores usam parâmetros.
          const table = targets[run.source_table];
          const result = await client.query(`INSERT INTO ${table} AS target (source_key,contract_version,payload,last_batch_id)
            SELECT x.key,$2,x.payload,$3 FROM jsonb_to_recordset($1::jsonb) AS x(key text,payload jsonb)
            ON CONFLICT (source_key) DO UPDATE SET payload=EXCLUDED.payload,contract_version=EXCLUDED.contract_version,
              last_batch_id=EXCLUDED.last_batch_id,updated_at=clock_timestamp()
            WHERE target.payload IS DISTINCT FROM EXCLUDED.payload OR target.contract_version IS DISTINCT FROM EXCLUDED.contract_version
            RETURNING source_key`, [JSON.stringify(chunk), run.contract_version, batch.id]);
          persisted += result.rowCount ?? 0;
        }
        const result: BatchCounts = { received: rows.length, persisted, duplicates: rows.length - persisted, rejected: 0 };
        if (this.projection) await captureBatch(client,batch.id,this.projection,contract,unique);
        const cp = await client.query(`UPDATE olos_export.checkpoints SET version=version+1,cursor_value=$2,last_batch_id=$3,last_success_at=clock_timestamp()
          WHERE stream_id=$1 AND version=$4 AND cursor_value IS NOT DISTINCT FROM $5`, [handle.streamId,batch.nextCursor,batch.id,batch.expected.version,batch.expected.cursor]);
        if (cp.rowCount !== 1) throw new PersistenceError('CHECKPOINT_CONFLICT');
        await client.query(`UPDATE olos_export.batches SET state='committed',persisted_count=$2,duplicate_count=$3,rejected_count=0,committed_at=clock_timestamp() WHERE id=$1`, [batch.id,persisted,result.duplicates]);
        await client.query(`INSERT INTO olos_export.audit_events(id,run_id,batch_id,event,received_count,persisted_count,duplicate_count)
          VALUES ($1,$2,$3,'batch_committed',$4,$5,$6)`, [randomUUID(),handle.runId,batch.id,rows.length,persisted,result.duplicates]);
        await this.aggregate(client, handle.runId);
        await this.guard(client, handle); // Prazo/posse/cancelamento ainda válidos antes do COMMIT.
        return result;
      });
    } catch (error) {
      await this.recordFailure(handle, batch, error);
      throw error;
    }
  }

  private async recordFailure(handle: RunHandle, batch: PersistenceBatch, error: unknown): Promise<void> {
    const failure = error instanceof PersistenceError ? error : new PersistenceError('PERSISTENCE_FAILED');
    if (['BATCH_CONFLICT','BUSY','LEASE_LOST','RECOVERY_REQUIRED','CONTRACT_UNCONFIRMED','PROJECTION_CAPTURE_MISSING'].includes(failure.code)) return;
    // Transação independente, best effort. Sem DB, o intent durável continua pendente para recuperação.
    try {
      await transaction(this.pool, async client => {
        await this.guard(client, handle);
        const stored = (await client.query<BatchRow>('SELECT * FROM olos_export.batches WHERE id=$1', [batch.id])).rows[0];
        // Nunca alterar um lote de outra execução.
        if (stored && stored.run_id !== handle.runId) return;
        const code = failure.code === 'VALIDATION_FAILED' || failure.code === 'CHECKPOINT_CONFLICT' || failure.code === 'COMMIT_UNKNOWN' ? failure.code : 'PERSISTENCE_FAILED';
        const rejected = failure.outcome === 'unknown' ? 0 : batch.records.length;
        await client.query(`INSERT INTO olos_export.sync_errors(id,run_id,batch_id,code,outcome,received_count,rejected_count)
          VALUES ($1,$2,$3,$4,$5,$6,$7)`, [randomUUID(),handle.runId,stored?.id ?? null,code,failure.outcome,batch.records.length,rejected]);
        if (stored && stored.state !== 'committed') {
          await client.query('UPDATE olos_export.batches SET state=$2,rejected_count=$3 WHERE id=$1', [batch.id,failure.outcome === 'unknown' ? 'needs_reconciliation' : 'failed',rejected]);
        }
        if (failure.outcome === 'unknown' && stored?.state !== 'committed') {
          await client.query("UPDATE olos_export.runs SET state='needs_reconciliation' WHERE id=$1", [handle.runId]);
          await client.query(`INSERT INTO olos_export.recovery_cases(id,run_id,reason) VALUES ($1,$2,'persistence_unknown') ON CONFLICT (run_id) DO NOTHING`, [randomUUID(),handle.runId]);
        }
        await this.aggregate(client, handle.runId);
      });
    } catch { /* Falha de diagnóstico não substitui o erro original e não gera retry de escrita. */ }
  }

  async finishRun(handle: RunHandle): Promise<void> {
    await transaction(this.pool, async client => {
      const run = await this.guard(client, handle);
      const status = await client.query<{ total: string; pending: string; rejected: string }>(`SELECT count(*) AS total,count(*) FILTER (WHERE state<>'committed') AS pending,
        (SELECT rejected_count FROM olos_export.runs WHERE id=$1) AS rejected FROM olos_export.batches WHERE run_id=$1`, [handle.runId]);
      if (status.rows[0]!.total === '0' || status.rows[0]!.pending !== '0' || status.rows[0]!.rejected !== '0') throw new PersistenceError('RECOVERY_REQUIRED');
      await client.query("UPDATE olos_export.runs SET state='succeeded',finished_at=clock_timestamp() WHERE id=$1", [handle.runId]);
      await client.query('UPDATE olos_export.streams SET last_success_at=clock_timestamp() WHERE id=$1', [handle.streamId]);
      const released = await client.query(`UPDATE olos_export.source_locks SET owner_run_id=NULL,lease_until=NULL
        WHERE source_table=$1 AND owner_run_id=$2 AND generation=$3 AND lease_until>clock_timestamp()`, [run.source_table,handle.runId,handle.fencingToken]);
      if (released.rowCount !== 1 || handle.signal.aborted) throw new PersistenceError('LEASE_LOST');
    });
  }

  async recoverExpired(): Promise<readonly string[]> {
    return transaction(this.pool, async client => {
      const expired = await client.query<{ source_table: string; owner_run_id: string }>(`SELECT source_table,owner_run_id FROM olos_export.source_locks
        WHERE owner_run_id IS NOT NULL AND lease_until<=clock_timestamp() FOR UPDATE SKIP LOCKED`);
      const cases: string[] = [];
      for (const lock of expired.rows) {
        await client.query("UPDATE olos_export.runs SET state='needs_reconciliation' WHERE id=$1", [lock.owner_run_id]);
        await client.query("UPDATE olos_export.batches SET state='needs_reconciliation' WHERE run_id=$1 AND state<>'committed'", [lock.owner_run_id]);
        const recovery = await client.query<{ id: string }>(`INSERT INTO olos_export.recovery_cases(id,run_id,reason) VALUES ($1,$2,'expired_read_intent')
          ON CONFLICT (run_id) DO UPDATE SET run_id=EXCLUDED.run_id RETURNING id`, [randomUUID(),lock.owner_run_id]);
        cases.push(recovery.rows[0]!.id);
        await client.query(`INSERT INTO olos_export.audit_events(id,run_id,event,received_count,persisted_count,duplicate_count)
          VALUES ($1,$2,'reconciliation_required',0,0,0)`, [randomUUID(),lock.owner_run_id]);
        await client.query('UPDATE olos_export.source_locks SET generation=generation+1,owner_run_id=NULL,lease_until=NULL WHERE source_table=$1', [lock.source_table]);
      }
      return cases;
    });
  }
  async recordRecoveryAttempt(caseId: string, action: 'inspect_log' | 'plan_reread' | 'manual_review',
    result: 'inconclusive' | 'evidence_recorded' | 'failed', evidenceDigest: string | null): Promise<void> {
    assertUuid(caseId);
    if (!['inspect_log','plan_reread','manual_review'].includes(action) || !['inconclusive','evidence_recorded','failed'].includes(result) ||
      (evidenceDigest !== null && !/^[0-9a-f]{64}$/.test(evidenceDigest))) throw new PersistenceError('VALIDATION_FAILED');
    await transaction(this.pool, async client => {
      const open = await client.query("SELECT id FROM olos_export.recovery_cases WHERE id=$1 AND state='open' FOR UPDATE", [caseId]);
      if (open.rowCount !== 1) throw new PersistenceError('RECOVERY_REQUIRED');
      await client.query(`INSERT INTO olos_export.recovery_attempts(id,case_id,action,result,evidence_digest) VALUES ($1,$2,$3,$4,$5)`, [randomUUID(),caseId,action,result,evidenceDigest]);
      await client.query('UPDATE olos_export.recovery_cases SET attempts=attempts+1 WHERE id=$1', [caseId]);
    });
  }
}
