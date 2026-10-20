import { randomUUID } from 'node:crypto';
import type { Pool, PoolClient } from 'pg';
import { createHash } from 'node:crypto';
import { transaction } from './transaction.js';
import { canonical, digest, targets, prepareRecords, deduplicate, assertUuid, assertVersion, assertContract } from './validation.js';
import { PersistenceError, defaultLimits, type BatchCounts, type RunHandle, type SourceContract } from './types.js';
import type { SpoolManifest, SpoolRecord, SpoolLimits, WholeBatchWriter, LargePlan } from '../large-batch/types.js';
import { spoolLimits } from '../large-batch/config.js';
import { captureConfig, type CaptureConfig } from '../projection/capture.js';
import { ChunkedProjectionCapture } from '../projection/chunked-capture.js';

export const manifestDigest = (m: SpoolManifest): string => digest({ protocol: 'olos-spool/v1', identity: m.identity, rows: m.rows, bytes: m.bytes, sha256: m.sha256 });
const preflightFailure=(diagnosticCode:string)=>Object.assign(new PersistenceError('CONTRACT_UNCONFIRMED'),{diagnosticCode});
interface RunRow { source_table: SourceContract['source_table']; contract_version: string; stream_id: string; fencing_token: string; state: string;
  owner_run_id: string | null; generation: string; valid: boolean; }
/** Uma resposta = uma transação, inclusive staging de chaves, envelopes, recibo e checkpoint. */
export class PostgresLargeBatchPersistence implements WholeBatchWriter {
  private readonly projection: Readonly<CaptureConfig> | undefined;
  constructor(private readonly pool: Pick<Pool,'connect'>, private readonly limits: SpoolLimits, private readonly leaseMs = 60_000, projection?: CaptureConfig) {
    this.projection = projection === undefined ? undefined : captureConfig(projection);
    this.limits = spoolLimits(limits);
    if (!Number.isSafeInteger(leaseMs) || leaseMs < 1 || leaseMs > 300_000) throw new PersistenceError('VALIDATION_FAILED');
  }
  async preflight(plan: LargePlan): Promise<void> {
    await transaction(this.pool,async client => {
      const versions = (await client.query<{ version: number }>('SELECT version FROM olos_export.large_batch_schema_versions ORDER BY version')).rows;
      if (canonical(versions) !== canonical([{ version: 1 }])) throw new PersistenceError('SCHEMA_NOT_READY');
      const mode = (await client.query<{ value: string }>("SELECT current_setting('transaction_read_only') AS value")).rows[0];
      if (mode?.value !== 'off') throw preflightFailure('PERSISTENCE_CONNECTION_READ_ONLY');
      const c = (await client.query<SourceContract>('SELECT * FROM olos_export.source_contracts WHERE source_table=$1 AND version=$2',[plan.run.table,plan.contract.version])).rows[0];
      if (!c) throw preflightFailure('SOURCE_CONTRACT_MISSING');
      if (!c.approved) throw preflightFailure('SOURCE_CONTRACT_NOT_APPROVED');
      if (canonical(c) !== canonical(plan.contract)) throw preflightFailure('SOURCE_CONTRACT_MISMATCH');
      try{assertContract(c);}catch{throw preflightFailure('SOURCE_CONTRACT_INVALID');}
      const grants = (await client.query<{ allowed: boolean }>(`SELECT
        has_table_privilege(current_user,'olos_export.large_batch_receipts','SELECT') AND has_table_privilege(current_user,'olos_export.large_batch_receipts','INSERT') AND
        has_table_privilege(current_user,'olos_export.large_batch_keys','SELECT') AND has_table_privilege(current_user,'olos_export.large_batch_keys','INSERT') AND
        has_table_privilege(current_user,$1,'SELECT') AND has_table_privilege(current_user,$1,'INSERT') AND has_table_privilege(current_user,$1,'UPDATE') AS allowed`,[targets[plan.run.table]])).rows[0];
      if (!grants?.allowed) throw preflightFailure('PERSISTENCE_ROLE_PRIVILEGES_MISSING');
    });
  }
  private async guard(client: PoolClient, m: SpoolManifest, handle: RunHandle): Promise<RunRow> {
    if (handle.signal.aborted) throw new PersistenceError('LEASE_LOST');
    const row = (await client.query<RunRow>(`SELECT r.*, l.owner_run_id,l.generation,
      (l.lease_until>clock_timestamp()) AS valid FROM olos_export.runs r
      JOIN olos_export.source_locks l ON l.source_table=r.source_table WHERE r.id=$1 FOR UPDATE OF l NOWAIT`,[handle.runId])).rows[0];
    if (!row || row.stream_id !== handle.streamId || row.fencing_token !== handle.fencingToken || row.generation !== handle.fencingToken ||
      row.owner_run_id !== handle.runId || !row.valid) throw new PersistenceError('LEASE_LOST');
    if (!['read_intent','persisting'].includes(row.state)) throw new PersistenceError('RECOVERY_REQUIRED');
    if (row.source_table !== m.identity.plan.run.table || row.contract_version !== m.identity.plan.contract.version) throw new PersistenceError('BATCH_CONFLICT');
    await client.query(`UPDATE olos_export.source_locks SET lease_until=clock_timestamp()+($2::double precision * interval '1 millisecond')
      WHERE source_table=$1`,[row.source_table,this.leaseMs]);
    return row;
  }
  async persist(m: SpoolManifest, handle: RunHandle, records: () => AsyncIterable<SpoolRecord>): Promise<BatchCounts> {
    assertUuid(m.identity.batchId); assertUuid(handle.runId); assertUuid(handle.streamId); assertVersion(handle.fencingToken);
    assertVersion(m.identity.expected.version);
    if (!m.sealed || !/^[a-f0-9]{64}$/.test(m.sha256 ?? '') || m.rows < 0 || m.rows > this.limits.maxRows ||
      handle.runId !== m.identity.runId || handle.streamId !== m.identity.streamId || handle.fencingToken !== m.identity.fencingToken) throw new PersistenceError('VALIDATION_FAILED');
    const bound = structuredClone(m), checksum = manifestDigest(bound), batchId = bound.identity.batchId;
    return transaction(this.pool,async client => {
      const versions = (await client.query<{ version: number }>('SELECT version FROM olos_export.large_batch_schema_versions ORDER BY version')).rows;
      if (versions.length !== 1 || versions[0]?.version !== 1) throw new PersistenceError('SCHEMA_NOT_READY');
      const coreVersions = (await client.query<{ version: number }>('SELECT version FROM olos_export.schema_versions ORDER BY version')).rows;
      if (canonical(coreVersions) !== canonical([{ version: 1 },{ version: 2 }])) throw new PersistenceError('SCHEMA_NOT_READY');
      const prior = (await client.query<{ manifest_digest: string; run_id: string; stream_id: string; received_count: number; persisted_count: number; duplicate_count: number; rejected_count: number; state: string }>(
        `SELECT x.manifest_digest,b.* FROM olos_export.large_batch_receipts x JOIN olos_export.batches b ON b.id=x.batch_id WHERE batch_id=$1`,[batchId])).rows[0];
      if (prior) {
        if (prior.manifest_digest !== checksum || prior.run_id !== handle.runId || prior.stream_id !== handle.streamId || prior.state !== 'committed') throw new PersistenceError('BATCH_CONFLICT');
        return { received: prior.received_count, persisted: prior.persisted_count, duplicates: prior.duplicate_count, rejected: prior.rejected_count };
      }
      const run = await this.guard(client,bound,handle);
      const contract = (await client.query<SourceContract>('SELECT * FROM olos_export.source_contracts WHERE source_table=$1 AND version=$2',
        [run.source_table,run.contract_version])).rows[0];
      if (!contract) throw new PersistenceError('CONTRACT_UNCONFIRMED'); assertContract(contract);
      if (canonical(contract) !== canonical(bound.identity.plan.contract)) throw new PersistenceError('CONTRACT_UNCONFIRMED');
      const cp = (await client.query<{ version: string; cursor_value: string | null }>('SELECT version,cursor_value FROM olos_export.checkpoints WHERE stream_id=$1 FOR UPDATE',[handle.streamId])).rows[0];
      if (!cp || cp.version !== bound.identity.expected.version || cp.cursor_value !== bound.identity.expected.cursor) throw new PersistenceError('CHECKPOINT_CONFLICT');
      // Uma resposta por run. Não sobrescrever totais de um run que já usou o protocolo anterior.
      const pending = await client.query('SELECT 1 FROM olos_export.batches WHERE run_id=$1',[handle.runId]);
      if (pending.rowCount) throw new PersistenceError('RECOVERY_REQUIRED');
      await client.query(`INSERT INTO olos_export.batches(id,run_id,stream_id,source_table,digest,expected_version,previous_cursor,next_cursor,state,received_count)
        VALUES ($1,$2,$3,$4,$5,$6,$7,$7,'read_observed',$8)`,[batchId,handle.runId,handle.streamId,run.source_table,checksum,cp.version,cp.cursor_value,bound.rows]);
      await client.query(`INSERT INTO olos_export.large_batch_receipts(batch_id,manifest_digest,data_digest,record_count,format_version)
        VALUES ($1,$2,$3,$4,'olos-spool/v1')`,[batchId,checksum,bound.sha256,bound.rows]);
      const capture = this.projection
        ? new ChunkedProjectionCapture(client, batchId, this.projection, contract)
        : undefined;
      if (capture) await capture.start();

      const hash = createHash('sha256');
      let chunk: SpoolRecord[] = [], chunkBytes = 0, received = 0, bytes = 0, persisted = 0;
      const flush = async () => {
        if (!chunk.length) return;
        await this.guard(client,bound,handle);
        const rows = deduplicate(prepareRecords(chunk,contract,{ ...defaultLimits, maxRecords: this.limits.chunkRows, maxBytes: this.limits.chunkBytes * 2 }).rows);
        const keyed = rows.map(r => ({ source_key: r.key, payload_digest: digest(r.payload) }));
        const inserted = await client.query<{ source_key: string }>(`INSERT INTO olos_export.large_batch_keys(batch_id,source_key,payload_digest)
          SELECT $1,x.source_key,x.payload_digest FROM jsonb_to_recordset($2::jsonb) AS x(source_key text,payload_digest text)
          ON CONFLICT (batch_id,source_key) DO NOTHING RETURNING source_key`,[batchId,JSON.stringify(keyed)]);
        const seen = await client.query<{ source_key: string; payload_digest: string }>('SELECT source_key,payload_digest FROM olos_export.large_batch_keys WHERE batch_id=$1 AND source_key=ANY($2::text[])',[batchId,keyed.map(k => k.source_key)]);
        const expected = new Map(keyed.map(k => [k.source_key,k.payload_digest]));
        if (seen.rows.length !== keyed.length || seen.rows.some(k => expected.get(k.source_key) !== k.payload_digest)) throw new PersistenceError('VALIDATION_FAILED');
        const fresh = new Set(inserted.rows.map(r => r.source_key)), payload = rows.filter(r => fresh.has(r.key));
        if (capture && payload.length) await capture.append(payload);
        if (payload.length) {
          const result = await client.query(`INSERT INTO ${targets[run.source_table]} AS target(source_key,contract_version,payload,last_batch_id)
            SELECT x.key,$2,x.payload,$3 FROM jsonb_to_recordset($1::jsonb) AS x(key text,payload jsonb)
            ON CONFLICT(source_key) DO UPDATE SET payload=EXCLUDED.payload,contract_version=EXCLUDED.contract_version,last_batch_id=EXCLUDED.last_batch_id,updated_at=clock_timestamp()
            WHERE target.payload IS DISTINCT FROM EXCLUDED.payload OR target.contract_version IS DISTINCT FROM EXCLUDED.contract_version RETURNING source_key`,[JSON.stringify(payload),run.contract_version,batchId]);
          persisted += result.rowCount ?? 0;
        }
        chunk = []; chunkBytes = 0;
      };
      for await (const record of records()) {
        const line = `${JSON.stringify(record)}\n`, size = Buffer.byteLength(line);
        if (++received > this.limits.maxRows || size > this.limits.maxRecordBytes) throw new PersistenceError('VALIDATION_FAILED');
        hash.update(line); bytes += size;
        if (chunk.length >= this.limits.chunkRows || chunkBytes + size > this.limits.chunkBytes) await flush();
        chunk.push(record); chunkBytes += size;
      }
      await flush();
      if (received !== bound.rows || bytes !== bound.bytes || hash.digest('hex') !== bound.sha256) throw new PersistenceError('VALIDATION_FAILED');
      await this.guard(client,bound,handle);
      if (capture) await capture.finish();
      const changed = await client.query(`UPDATE olos_export.checkpoints SET version=version+1,last_batch_id=$2,last_success_at=clock_timestamp()
        WHERE stream_id=$1 AND version=$3 AND cursor_value IS NOT DISTINCT FROM $4`,[handle.streamId,batchId,cp.version,cp.cursor_value]);
      if (changed.rowCount !== 1) throw new PersistenceError('CHECKPOINT_CONFLICT');
      const duplicates = received - persisted;
      await client.query(`UPDATE olos_export.batches SET state='committed',persisted_count=$2,duplicate_count=$3,committed_at=clock_timestamp() WHERE id=$1`,[batchId,persisted,duplicates]);
      await client.query(`UPDATE olos_export.runs SET state='succeeded',received_count=$2,persisted_count=$3,duplicate_count=$4,finished_at=clock_timestamp() WHERE id=$1`,[handle.runId,received,persisted,duplicates]);
      await client.query('UPDATE olos_export.streams SET last_success_at=clock_timestamp() WHERE id=$1',[handle.streamId]);
      await client.query(`INSERT INTO olos_export.audit_events(id,run_id,batch_id,event,received_count,persisted_count,duplicate_count)
        VALUES ($1,$2,$3,'batch_committed',$4,$5,$6)`,[randomUUID(),handle.runId,batchId,received,persisted,duplicates]);
      if (handle.signal.aborted) throw new PersistenceError('LEASE_LOST');
      await client.query('UPDATE olos_export.source_locks SET owner_run_id=NULL,lease_until=NULL WHERE source_table=$1 AND owner_run_id=$2 AND generation=$3',[run.source_table,handle.runId,handle.fencingToken]);
      return { received, persisted, duplicates, rejected: 0 };
    });
  }
}
