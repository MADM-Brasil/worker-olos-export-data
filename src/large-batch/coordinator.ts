import { isControlledDispositionSource } from '../integration/disposition.js';
import { isIsolatedTdsSource, assertRegisteredTdsExecution } from '../olos/tds/source.js';
import { randomUUID } from 'node:crypto';
import type { PostgresPersistence } from '../persistence/store.js';
import type { OlosRecoveryPort, ValidatedReReadPlan } from '../sync/recovery.js';
import type { BatchCounts } from '../persistence/types.js';
import { PersistenceError } from '../persistence/types.js';
import { validateLargePlan, decodeWireRow } from './wire.js';
import { spoolLimits } from './config.js';
import { SpoolError, type LargePlan, type RawSource, type SpoolRepository, type SpoolWriter, type SpoolLimits, type SpoolManifest, type WholeBatchWriter } from './types.js';

export class LargeBatchCoordinator {
  private busy = false;
  constructor(private readonly control: Pick<PostgresPersistence,'startRun' | 'checkpoint' | 'renew' | 'recoverExpired'>,
    private readonly spool: SpoolRepository, private readonly target: WholeBatchWriter, private readonly limits: SpoolLimits) { this.limits = spoolLimits(limits); }
  private async deliver(m: SpoolManifest, signal: AbortSignal): Promise<BatchCounts> {
    const handle = { runId: m.identity.runId, streamId: m.identity.streamId, fencingToken: m.identity.fencingToken, signal };
    await this.spool.mark(m,'persisting');
    try {
      const result = await this.target.persist(m,handle,() => this.spool.records(m));
      await this.spool.mark(m,'committed');
      return result;
    } catch (e) {
      try { await this.spool.mark(m,'uncertain'); } catch { /* intent/arquivo original permanece; nenhuma leitura liberada */ }
      throw e;
    }
  }
  async receive(input: LargePlan, source: RawSource, signal: AbortSignal): Promise<BatchCounts> {
    if (source.kind !== 'synthetic' && !isIsolatedTdsSource(source) && !isControlledDispositionSource(source)) throw new SpoolError('SOURCE_BLOCKED');
    assertRegisteredTdsExecution(source);
    const plan = structuredClone(input); validateLargePlan(plan);
    if (this.busy) throw new SpoolError('BUSY'); this.busy = true;
    let writer: SpoolWriter | undefined, sealed = false;
    let deadline: ReturnType<typeof setTimeout> | undefined;
    const abort = new AbortController(), cancel = () => abort.abort(); signal.addEventListener('abort',cancel,{ once: true });
    try {
      if (signal.aborted) throw new SpoolError('SOURCE_INTERRUPTED');
      if (await this.spool.active()) throw new SpoolError('RECOVERY_REQUIRED');
      await this.target.preflight(plan);
      const handle = await this.control.startRun(plan.run,signal), expected = await this.control.checkpoint(handle);
      writer = await this.spool.create({ format: 'olos-spool/v1', batchId: randomUUID(), runId: handle.runId, streamId: handle.streamId,
        fencingToken: handle.fencingToken, plan, expected });
      let complete = false, lastRenew = Date.now();
      deadline = setTimeout(() => abort.abort(),this.limits.readTimeoutMs);
      try {
        for await (const event of source.read(plan,abort.signal)) {
          try {
            if (signal.aborted || abort.signal.aborted || complete) throw new SpoolError('SOURCE_INTERRUPTED');
            if (event.kind === 'row') await writer.append(decodeWireRow(plan,event.values,this.limits.maxRecordBytes));
            else {
              if (event.kind !== 'complete' || event.datasetCount !== 1 || event.errorMessage !== null && event.errorMessage !== '' ||
                  !plan.successReturnValues.includes(event.returnValue)) throw new SpoolError('SOURCE_INTERRUPTED');
              complete = true;
            }
            if (Date.now()-lastRenew >= 1000) { await this.control.renew(handle); lastRenew = Date.now(); }
          } catch (e) { abort.abort(); throw e; }
        }
      } catch (e) { abort.abort(); throw e; } // iterator deve drenar antes de retornar; não há Promise.race liberando transporte
      clearTimeout(deadline);
      if (!complete || signal.aborted || abort.signal.aborted) throw new SpoolError('INCOMPLETE');
      const manifest = await writer.seal(); sealed = true;
      return await this.deliver(manifest,signal);
    } catch (e) {
      if (!sealed) { try { await writer?.interrupt(); } catch { /* identidade inicial é a evidência conservadora */ } }
      if (e instanceof PersistenceError || e instanceof SpoolError) throw e;
      if (e !== null && typeof e === 'object' && 'code' in e && ['BUSY','STORAGE_FAILED','DISK_FULL','QUOTA_EXCEEDED','CORRUPT','INCOMPLETE'].includes(String(e.code))) throw new SpoolError(e.code as SpoolError['code']);
      throw new SpoolError('SOURCE_INTERRUPTED');
    } finally { if (deadline) clearTimeout(deadline); signal.removeEventListener('abort',cancel); this.busy = false; }
  }
  /** Não chama a origem. Lote sem selo nunca entra em persistência. Lease expirada exige conciliação existente. */
  async recover(signal: AbortSignal): Promise<{ manifest: SpoolManifest | null; cases: readonly string[]; result: BatchCounts | null }> {
    if (this.busy) throw new SpoolError('BUSY'); this.busy = true;
    try {
      const manifest = await this.spool.active();
      if (!manifest) return { manifest: null, cases: [], result: null };
      if (!manifest.sealed) return { manifest, cases: await this.control.recoverExpired(), result: null };
      try { return { manifest, cases: [], result: await this.deliver(manifest,signal) }; }
      catch (e) { await this.control.recoverExpired(); throw e; }
    } finally { this.busy = false; }
  }
  async planRecovery(caseId: string, recovery: OlosRecoveryPort, signal: AbortSignal): Promise<ValidatedReReadPlan> {
    const m = await this.spool.active(); if (!m) throw new SpoolError('RECOVERY_REQUIRED');
    const r = m.identity.plan.run;
    const request = { caseId, sourceTable: r.table, readType: r.readType, exportDate: r.exportDate, pkRange: r.pkRange };
    const evidence = await recovery.inspectLogReadData(request,signal);
    return recovery.planReReadData(request,evidence); // somente plano, nunca ReRead/Reset executados aqui
  }
}
