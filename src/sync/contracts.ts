// Contratos da fundação. O protocolo durável da Fase 2A vive em persistence/types.ts e store.ts.
// Não há adaptador de extração ou execução de DDL no runtime.
export const exportTables = ['AttemptsRawData', 'InboundRawData', 'PbxBillingData', 'Users', 'Campaign', 'Disposition'] as const;
export type ExportTable = typeof exportTables[number];
// Preservar BIGINT/DECIMAL e timestamps de alta precisão como strings; nunca arredondar para number/Date.
export type ExactValue = string | boolean | null | Uint8Array;
export type ExportRecord = Readonly<Record<string, ExactValue>>;
export interface Cursor { readonly opaque: string; }
export interface Batch {
  readonly table: ExportTable;
  readonly records: readonly ExportRecord[];
  readonly nextCursor: Cursor | null;
  readonly checksum: string;
}
export interface RunContext { readonly runId: string; readonly fencingToken: string; readonly signal: AbortSignal; }
export interface TableExtractor { extract(table: ExportTable, cursor: Cursor | null, context: RunContext): AsyncIterable<Batch>; }
export interface BatchWriter {
  // Futuro adaptador: atomicamente deduplicar, persistir lote e checkpoint com o fencing token válido.
  persistAndCheckpoint(batch: Batch, context: RunContext): Promise<{ inserted: number; duplicates: number }>;
}
export type RunState = 'running' | 'succeeded' | 'failed' | 'needs_reconciliation';
export interface RunStore {
  begin(table: ExportTable, context: RunContext): Promise<Cursor | null>;
  finish(state: Exclude<RunState, 'running'>, context: RunContext): Promise<void>;
  recoverInterrupted(context: RunContext): Promise<readonly string[]>;
}
export interface InstanceLease {
  acquire(signal: AbortSignal): Promise<{ fencingToken: string } | null>;
  renew(fencingToken: string): Promise<boolean>;
  release(fencingToken: string): Promise<void>;
}
export interface AuditEvent {
  readonly runId: string;
  readonly table: ExportTable;
  readonly event: 'batch_committed' | 'run_failed' | 'reconciliation_required';
  readonly rows: number;
  readonly checksum: string | null;
}
export interface AuditStore { append(event: AuditEvent, context: RunContext): Promise<void>; }
export interface Reconciler { reconcile(table: ExportTable, context: RunContext): Promise<{ matches: boolean }>; }
