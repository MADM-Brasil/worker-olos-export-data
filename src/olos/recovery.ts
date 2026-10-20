import type { OlosRecoveryPort, ReconciliationRequest, LogReadEvidence, ValidatedReReadPlan } from '../sync/recovery.js';
import { assertTable, assertUuid, digest, validDate } from '../persistence/validation.js';
import type { OlosProcedureClient } from './client.js';
import type { IngestionPort } from './extraction.js';
import { OlosError, type PreparedCall, type ProcedureReply, type ProcedureSignature } from './types.js';

export interface RecoveryContract {
  readonly logSignature: ProcedureSignature;
  readonly evidence: string;
  readonly parameters: (request: ReconciliationRequest) => PreparedCall['values'];
  readonly classifyLog: (reply: ProcedureReply, request: ReconciliationRequest) => LogReadEvidence['conclusion'];
  readonly rereadSignature: ProcedureSignature;
  readonly operatorApprovalReference: string;
}
export interface CorrelatedLogEvidence extends LogReadEvidence {
  readonly requestDigest: string;
  readonly logContractVersion: string;
}
/** Planejamento não autoriza consumo. LogReadData somente via transporte sintético nesta fase. */
export class OlosReconciliation implements OlosRecoveryPort {
  constructor(private readonly client: Pick<OlosProcedureClient, 'call'>,
    private readonly audit: Pick<IngestionPort, 'recordRecoveryAttempt'>, private readonly contract: RecoveryContract) {}
  private validate(request: ReconciliationRequest): void {
    assertUuid(request.caseId); assertTable(request.sourceTable);
    if (!validDate(request.exportDate) || !this.contract.evidence || this.contract.logSignature.procedure !== 'LogReadData' ||
      this.contract.rereadSignature.procedure !== 'ReReadData') throw new OlosError('CONTRACT_UNCONFIRMED');
  }
  async inspectLogReadData(input: ReconciliationRequest, signal: AbortSignal): Promise<CorrelatedLogEvidence> {
    const request = structuredClone(input);
    this.validate(request);
    try {
      const reply = await this.client.call(this.contract.logSignature, this.contract.parameters(request), signal);
      if (reply.ErrorMessage !== null && reply.ErrorMessage !== '') throw new OlosError('OUTPUT_ERROR', 'unknown');
      const conclusion = this.contract.classifyLog(reply, request);
      if (!['inconclusive','remote_read_observed'].includes(conclusion)) throw new OlosError('INVALID_RESPONSE', 'unknown');
      // Evidência de leitura remota NÃO prova que foi feita por este worker nem prova persistência.
      const evidence: CorrelatedLogEvidence = { conclusion, requestDigest: digest(request), logContractVersion: this.contract.logSignature.version,
        digest: digest({ request, conclusion,
        contractVersion: this.contract.logSignature.version, datasets: reply.datasets, returnValue: reply.returnValue }) };
      await this.audit.recordRecoveryAttempt(request.caseId, 'inspect_log', conclusion === 'inconclusive' ? 'inconclusive' : 'evidence_recorded', evidence.digest);
      return evidence;
    } catch {
      try { await this.audit.recordRecoveryAttempt(request.caseId, 'inspect_log', 'failed', null); } catch { /* intent continua aberto */ }
      throw new OlosError('EXECUTION_FAILED', 'unknown');
    }
  }
  async planReReadData(input: ReconciliationRequest, evidence: LogReadEvidence): Promise<ValidatedReReadPlan> {
    const request = structuredClone(input);
    this.validate(request);
    if (evidence.conclusion !== 'remote_read_observed' || !/^[0-9a-f]{64}$/.test(evidence.digest) ||
      !('requestDigest' in evidence) || evidence.requestDigest !== digest(request) ||
      !('logContractVersion' in evidence) || evidence.logContractVersion !== this.contract.logSignature.version ||
      !this.contract.operatorApprovalReference || !this.contract.rereadSignature.evidence) throw new OlosError('CONTRACT_UNCONFIRMED');
    await this.audit.recordRecoveryAttempt(request.caseId, 'plan_reread', 'evidence_recorded', evidence.digest);
    // Não invoca ReReadData, não altera checkpoints, não resolve o caso e não usa ResetReadData.
    return { request, procedureContractEvidence: this.contract.rereadSignature.evidence,
      operatorApprovalReference: this.contract.operatorApprovalReference };
  }
}
