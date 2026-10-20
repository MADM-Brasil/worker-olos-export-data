import type { OlosPinApproval } from '../../security/olos-certificate-pin.js';
import { prepareCorporateAttemptsEndpoint, assertCorporateAttemptsExecution, validateCorporateAttemptsPreparation } from '../../olos/tds/attempts-access.js';
import { OlosReconciliation, type RecoveryContract } from '../../olos/recovery.js';
import type { IngestionPort } from '../../olos/extraction.js';
import type { PreparedCall, ProcedureSignature } from '../../olos/types.js';
import { planCorporateAttemptsRecovery } from './recovery.js';
import { prepareCall } from '../../olos/client.js';

/** No network: produces a reviewed, non-executable ReReadData intent. Never infers ownership from LogReadData. */
export function prepareCorporateAttemptsReRead(config: unknown, pin: OlosPinApproval, signature: ProcedureSignature,
  ...args: Parameters<typeof planCorporateAttemptsRecovery>) {
  if (signature.procedure !== 'ReReadData') throw new Error('RECOVERY_CASE_MISMATCH');
  const recovery = planCorporateAttemptsRecovery(...args), i = recovery.interval;
  const values = { TableName: 'AttemptsRawData',DateExport: i.date,PKIDStart: i.start,PKIDEnd: i.end };
  const endpoint = prepareCorporateAttemptsEndpoint(config,pin,signature,values);
  return { recovery,endpoint,call: prepareCall(signature,values,300000),remoteOwnershipProvenByLog: false as const,
    executionAllowed: false as const };
}
/** Plugs into LargeBatchCoordinator.planRecovery via the EXISTING reconciliation/audit port.
 * Log and reread share the same prepared endpoint policy; both remain non-executable.
 * Do not substitute fixture signatures, fabricated replies, or successful return assumptions.
 */
export function prepareCorporateAttemptsRecoveryPort(config: unknown, pin: OlosPinApproval,
  audit: Pick<IngestionPort,'recordRecoveryAttempt'>, contract: RecoveryContract): OlosReconciliation {
  const c: RecoveryContract = { ...contract,
    parameters: request => {
      if (request.sourceTable !== 'AttemptsRawData') throw new Error('RECOVERY_CASE_MISMATCH');
      const values: PreparedCall['values'] = { tablename: 'AttemptsRawData',DateExport: request.exportDate };
      return values;
    } };
  // Validate both official signatures/pin/config at preparation; no socket or SQL request.
  validateCorporateAttemptsPreparation(config,pin,c.logSignature);
  validateCorporateAttemptsPreparation(config,pin,c.rereadSignature);
  if (c.logSignature.procedure !== 'LogReadData' || c.rereadSignature.procedure !== 'ReReadData') throw new Error('RECOVERY_CASE_MISMATCH');
  return new OlosReconciliation({ call: async (signature,values,signal) => {
    if (signal.aborted) throw new Error('ABORTED');
    const endpoint = prepareCorporateAttemptsEndpoint(config,pin,signature,values);
    assertCorporateAttemptsExecution(endpoint);
    throw new Error('REMOTE_RECOVERY_REQUIRES_SEPARATE_REVIEW');
  } },audit,c);
}
