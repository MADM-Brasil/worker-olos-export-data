import type { ExportTable, RunContext } from '../sync/contracts.js';
import type { Config } from '../config/env.js';

export type ReadType = 'current_day' | 'previous_days' | 'reread' | 'reference_snapshot' | 'investigation';
export interface StartRun {
  readonly table: ExportTable;
  readonly contractVersion: string;
  readonly readType: ReadType;
  readonly exportDate: string; // data civil de exportação; não é timestamp da origem
  readonly pkRange: Readonly<Record<string, string>> | null;
}
export interface RunHandle extends RunContext { readonly streamId: string; }
export interface Checkpoint { readonly version: string; readonly cursor: string | null; }
export interface PersistenceBatch {
  readonly id: string;
  readonly records: readonly Readonly<Record<string, unknown>>[];
  readonly expected: Checkpoint;
  readonly nextCursor: string | null;
}
export interface BatchCounts { readonly received: number; readonly persisted: number; readonly duplicates: number; readonly rejected: number; }
export interface PersistenceLimits {
  readonly maxRecords: number;
  readonly maxBytes: number;
  readonly chunkSize: number;
  readonly leaseMs: number;
}
export const defaultLimits: PersistenceLimits = { maxRecords: 1000, maxBytes: 4_194_304, chunkSize: 200, leaseMs: 60_000 };
export function configuredPersistenceLimits(c: Config): PersistenceLimits {
  return { maxRecords: c.PERSISTENCE_MAX_RECORDS, maxBytes: c.PERSISTENCE_MAX_BYTES,
    chunkSize: c.PERSISTENCE_CHUNK_SIZE, leaseMs: c.PERSISTENCE_LEASE_MS };
}
export type FailureCode = 'PERSISTENCE_FAILED' | 'COMMIT_UNKNOWN' | 'VALIDATION_FAILED' | 'CHECKPOINT_CONFLICT' | 'LEASE_LOST';
export class PersistenceError extends Error {
  constructor(readonly code: FailureCode | 'BUSY' | 'CONTRACT_UNCONFIRMED' | 'BATCH_CONFLICT' | 'RECOVERY_REQUIRED' | 'SCHEMA_NOT_READY' | 'PROJECTION_CAPTURE_MISSING',
    readonly outcome: 'not_committed' | 'unknown' = 'not_committed') { super(code); this.name = 'PersistenceError'; }
}
export interface FieldDefinition {
  readonly type: 'text' | 'int64' | 'decimal' | 'local_timestamp' | 'offset_timestamp' | 'sql_datetime_ticks' | 'boolean' | 'bytes';
  readonly nullable: boolean;
}
export interface SourceContract {
  readonly source_table: ExportTable;
  readonly version: string;
  readonly approved: boolean;
  readonly fields: Readonly<Record<string, FieldDefinition>>;
  readonly key_fields: readonly string[];
  readonly layout_evidence: string | null;
  readonly key_evidence: string | null;
}
