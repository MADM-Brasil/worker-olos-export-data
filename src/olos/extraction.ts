import { randomUUID } from 'node:crypto';
import type { PostgresPersistence } from '../persistence/store.js';
import type { Checkpoint, PersistenceBatch, RunHandle, StartRun } from '../persistence/types.js';
import { adaptDataset, validateTableContract, type ValidatedTableContract } from './adapter.js';
import type { OlosProcedureClient } from './client.js';
import { OlosError, type PreparedCall, type ProcedureReply, type ProcedureSignature } from './types.js';
import type { ExportRecord, ExportTable } from '../sync/contracts.js';

export type IngestionPort = Pick<PostgresPersistence, 'limits' | 'startRun' | 'checkpoint' | 'renew' | 'observeBatch' |
  'persistBatch' | 'finishRun' | 'recoverExpired' | 'recordRecoveryAttempt'>;
export interface ReadPlan {
  readonly run: StartRun;
  readonly tableContract: ValidatedTableContract;
  readonly signature: ProcedureSignature;
  readonly parameters: (checkpoint: Checkpoint) => PreparedCall['values'];
  readonly responseEvidence: string;
  readonly readTypeEvidence: string; // associação explícita ao rótulo interno da Fase 2A, não semântica inferida
  readonly successfulReturnValues: readonly number[];
  readonly datasetIndex: number;
  readonly timeoutMs: number;
  // null = exatamente uma chamada. Nenhuma inferência de paginação, fim por lote vazio ou PK máxima.
  readonly repetition: null | {
    readonly evidence: string;
    readonly maxCalls: number;
    readonly continuation: (reply: ProcedureReply, records: readonly ExportRecord[], checkpoint: Checkpoint) =>
      { readonly done: boolean; readonly nextCursor: string | null };
  };
}
export interface ExtractionReceipt { readonly runId: string; readonly batches: readonly string[]; readonly received: number; }

/** Coordenador injetável, não instanciado no boot. Lock durável vem de startRun; Set apenas evita sobreposição local. */
export class OlosExtraction {
  private readonly active = new Set<ExportTable>();
  constructor(private readonly client: Pick<OlosProcedureClient, 'call'>, private readonly ingestion: IngestionPort) {}
  async run(input: ReadPlan, signal: AbortSignal): Promise<ExtractionReceipt> {
    const plan: ReadPlan = { ...input, run: structuredClone(input.run), tableContract: structuredClone(input.tableContract),
      signature: structuredClone(input.signature), successfulReturnValues: [...input.successfulReturnValues],
      repetition: input.repetition ? { ...input.repetition } : null };
    validateTableContract(plan.tableContract);
    if (plan.run.table !== plan.tableContract.source.source_table || plan.run.contractVersion !== plan.tableContract.source.version ||
      !plan.responseEvidence || !plan.readTypeEvidence || !Number.isSafeInteger(plan.datasetIndex) || plan.datasetIndex < 0 ||
      plan.successfulReturnValues.length === 0 || plan.successfulReturnValues.some(n => !Number.isSafeInteger(n))) throw new OlosError('CONTRACT_UNCONFIRMED');
    if (!['ReadData','ReadDataCurrentDay','ReadDataPreviousDays','ReReadData'].includes(plan.signature.procedure)) throw new OlosError('CONTRACT_UNCONFIRMED');
    if (plan.repetition && (!plan.repetition.evidence || !Number.isSafeInteger(plan.repetition.maxCalls) ||
      plan.repetition.maxCalls < 1 || plan.repetition.maxCalls > 10_000)) throw new OlosError('REPETITION_UNCONFIRMED');
    const table = plan.run.table;
    if (this.active.has(table)) throw new OlosError('BUSY');
    this.active.add(table);
    try {
      // COMMIT do intent confirmado ANTES de chamar a origem; falha de startRun implica zero leituras.
      const handle = await this.ingestion.startRun(plan.run, signal);
      let checkpoint = await this.ingestion.checkpoint(handle);
      const batches: string[] = [];
      let received = 0;
      const seenCursors = new Set([checkpoint.cursor]);
      for (let page = 0; page < (plan.repetition?.maxCalls ?? 1); page++) {
        if (signal.aborted) throw new OlosError('ABORTED');
        await this.ingestion.renew(handle);
        const reply = await this.client.call(plan.signature, plan.parameters(checkpoint), signal, plan.timeoutMs);
        if (reply.ErrorMessage !== null && reply.ErrorMessage !== '') throw new OlosError('OUTPUT_ERROR', 'unknown');
        if (!plan.successfulReturnValues.includes(reply.returnValue as number)) throw new OlosError('OUTPUT_ERROR', 'unknown');
        // Múltiplos datasets só após associação contratada. Não descartar silenciosamente datasets extras.
        if (reply.datasets.length !== 1 || plan.datasetIndex !== 0) throw new OlosError('INVALID_RESPONSE', 'unknown');
        const records = adaptDataset(reply.datasets[0]!, plan.tableContract, this.ingestion.limits);
        const continuation = plan.repetition?.continuation(reply, records, checkpoint) ?? { done: true, nextCursor: checkpoint.cursor };
        if (typeof continuation.done !== 'boolean' || (continuation.nextCursor !== null &&
          (typeof continuation.nextCursor !== 'string' || continuation.nextCursor.length > 2048))) throw new OlosError('INVALID_RESPONSE', 'unknown');
        if (!continuation.done && (seenCursors.has(continuation.nextCursor) || page + 1 === plan.repetition?.maxCalls)) throw new OlosError('NO_PROGRESS', 'unknown');
        // Um lote remoto cabe inteiro nos limites. Não fracionar e avançar checkpoint parcialmente por suposição.
        const batch: PersistenceBatch = { id: randomUUID(), records, expected: checkpoint, nextCursor: continuation.nextCursor };
        await this.ingestion.renew(handle); // lease expirada durante leitura impede persistência/novo consumo
        await this.ingestion.observeBatch(handle, batch);
        await this.ingestion.persistBatch(handle, batch);
        // Backpressure: próxima chamada somente após commit + releitura consistente do checkpoint.
        checkpoint = await this.ingestion.checkpoint(handle);
        if (checkpoint.version !== (BigInt(batch.expected.version) + 1n).toString() || checkpoint.cursor !== batch.nextCursor) throw new OlosError('INVALID_RESPONSE', 'unknown');
        batches.push(batch.id); received += records.length;
        if (continuation.done) {
          await this.ingestion.finishRun(handle);
          return { runId: handle.runId, batches: Object.freeze(batches), received };
        }
        seenCursors.add(continuation.nextCursor);
      }
      throw new OlosError('NO_PROGRESS', 'unknown');
    } finally { this.active.delete(table); }
    // Em qualquer falha, NÃO finish/release/reler: intent e lease ficam para recoverExpired e conciliação.
  }
  /** Retry local só com o MESMO payload/lote retido. Nunca executa uma procedure para preencher dados perdidos. */
  async retryRetainedBatch(handle: RunHandle, batch: PersistenceBatch): Promise<void> {
    await this.ingestion.persistBatch(handle, batch);
  }
  async recoverInterrupted(): Promise<readonly string[]> { return this.ingestion.recoverExpired(); }
}

