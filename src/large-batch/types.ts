import type { Checkpoint, RunHandle, SourceContract, StartRun, BatchCounts } from '../persistence/types.js';
import type { ProcedureSignature } from '../olos/types.js';

export class SpoolError extends Error {
  constructor(readonly code: 'BUSY' | 'STORAGE_FAILED' | 'DISK_FULL' | 'QUOTA_EXCEEDED' | 'CORRUPT' | 'INCOMPLETE' |
    'INVALID_CONFIG' | 'INVALID_VALUE' | 'SOURCE_INTERRUPTED' | 'SOURCE_BLOCKED' | 'RECOVERY_REQUIRED') { super(code); this.name = 'SpoolError'; }
}
export interface SpoolLimits {
  readonly maxRows: number; readonly maxDiskBytes: number; readonly minFreeBytes: number;
  readonly bufferBytes: number; readonly maxRecordBytes: number; readonly syncEveryBytes: number;
  readonly chunkRows: number; readonly chunkBytes: number;
  readonly readTimeoutMs: number;
}
export interface WireField {
  readonly name: string; readonly type: 'int' | 'bigint' | 'datetime' | 'varchar' | 'nvarchar' | 'char' | 'decimal';
  readonly nullable: boolean; readonly maxBytes?: number; readonly precision?: number; readonly scale?: number;
  readonly encoding?: 'ascii' | 'utf-8' | 'windows-1252';
}
export interface LargePlan {
  readonly run: StartRun; readonly contract: SourceContract; readonly fields: readonly WireField[];
  readonly signature: ProcedureSignature; readonly parameters: Readonly<Record<string, string | null | boolean>>;
  readonly acquisitionEvidence: string; readonly responseEvidence: string; readonly partitionEvidence: string;
  readonly successReturnValues: readonly number[];
}
export interface SpoolIdentity {
  readonly format: 'olos-spool/v1'; readonly batchId: string; readonly runId: string; readonly streamId: string;
  readonly fencingToken: string; readonly plan: LargePlan; readonly expected: Checkpoint;
}
export type SpoolState = 'receiving' | 'sealed' | 'persisting' | 'uncertain' | 'recovery_required' | 'committed';
export interface SpoolManifest {
  readonly identity: SpoolIdentity; readonly state: SpoolState; readonly sealed: boolean;
  readonly rows: number; readonly bytes: number; readonly sha256: string | null;
}
export type SpoolRecord = Readonly<Record<string, string | boolean | null>>;
export interface SpoolWriter {
  append(record: SpoolRecord): Promise<void>;
  seal(): Promise<SpoolManifest>;
  interrupt(): Promise<void>;
}
/** Filesystem somente no adaptador opcional; nenhum runner de SQL ou leitura de migrations. */
export interface SpoolRepository {
  create(identity: SpoolIdentity): Promise<SpoolWriter>;
  active(): Promise<SpoolManifest | null>;
  records(manifest: SpoolManifest): AsyncIterable<SpoolRecord>;
  mark(manifest: SpoolManifest, state: 'persisting' | 'uncertain' | 'committed'): Promise<SpoolManifest>;
}
export type RawEvent = { readonly kind: 'row'; readonly values: readonly (Uint8Array | null)[] } |
  { readonly kind: 'complete'; readonly returnValue: number; readonly errorMessage: string | null; readonly datasetCount: number };
export interface RawSource {
  readonly kind: 'synthetic' | 'mssql' | 'isolated-sqlserver' | 'controlled-disposition';
  // Pull/await ou pause/resume real; nunca emitir eventos async sem aguardar o sink. Deve drenar no abort.
  read(plan: LargePlan, signal: AbortSignal): AsyncIterable<RawEvent>;
}
export interface WholeBatchWriter {
  preflight(plan: LargePlan): Promise<void>;
  persist(manifest: SpoolManifest, handle: RunHandle, records: () => AsyncIterable<SpoolRecord>): Promise<BatchCounts>;
}
