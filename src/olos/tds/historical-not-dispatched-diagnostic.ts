import {z} from 'zod';

// Frozen wire vocabulary from e53ad7b^ (not the evolving producer vocabulary).
// Recognition is not authorization: callers must also prove an already archived
// local closure, its original digest and empty response, with no import spool.
const stages=['AUTHORIZATION_STARTED','PERMIT_ISSUED','CONNECTION_INIT_STARTED','SOCKET_CONNECTED',
  'TLS_HANDSHAKE_STARTED','TLS_VALIDATED','SQL_LOGIN_PREPARATION','SQL_LOGIN_STARTED','SQL_LOGIN_ACCEPTED'] as const;
const errors=[
  'SERVICE_AUTHORIZATION_REJECTED','SERVICE_CONFIGURATION_CHANGED','OBSERVED_CONTRACT_NOT_APPROVED','ACTIVATION_REQUIREMENTS_MISSING','RECOVERY_NOT_ESTABLISHED','INSPECTION_EXTERNAL_RECONCILIATION_REQUIRED','POSTGRES_DESTINATION_CHANGED','RECOVERY_REQUIRED',
  'REAL_EXECUTION_BLOCKED','ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED','PROCEDURE_BLOCKED','CONTRACT_UNCONFIRMED','INVALID_PARAMETER',
  'CONNECTION_FAILED','EXECUTION_FAILED','TIMEOUT','INVALID_RESPONSE','OUTPUT_ERROR','ABORTED','BUSY','LIMIT_EXCEEDED','INCOMPLETE',
  'PIN_MISMATCH','PIN_APPROVAL_REQUIRED','PIN_APPROVAL_INVALID','PIN_APPROVAL_EXPIRED','PIN_CONFIGURATION_INVALID','PIN_TLS_REJECTED',
  'CERT_HAS_EXPIRED','CERT_NOT_YET_VALID','DEPTH_ZERO_SELF_SIGNED_CERT','SELF_SIGNED_CERT_IN_CHAIN','UNABLE_TO_VERIFY_LEAF_SIGNATURE',
  'ERR_TLS_CERT_ALTNAME_INVALID','ERR_SSL_TLSV1_ALERT_PROTOCOL_VERSION','INVALID_TDS',
  'ELOGIN','ESOCKET','ETIMEOUT','ECONNREFUSED','ECONNRESET','EPIPE','ENOTFOUND','EHOSTUNREACH','ENETUNREACH','ETIMEDOUT',
  'STORAGE_FAILED','DISK_FULL','ENOSPC','EDQUOT','EACCES','EPERM','ENOENT','EEXIST','QUOTA_EXCEEDED','CORRUPT',
  'CORPORATE_APPROVAL_EXPIRED','REMOTE_OPERATION_REQUIRES_REVIEW','LOG_INSPECTION_DATE_MISMATCH',
] as const;
const historical=z.object({
  attemptId:z.string().uuid(),version:z.literal('olos-log-inspection-diagnostic/v1'),
  phase:z.enum(['AUTHORIZATION','PERMIT_ISSUED','CONNECTION_INIT','TLS_HANDSHAKE','SQL_LOGIN']),
  lastObservedStage:z.enum(stages),errorCode:z.enum(errors),observedStages:z.array(z.enum(stages)).min(1),
  rpcDispatchState:z.literal('NOT_OBSERVED'),remoteExecutionOutcome:z.literal('NOT_OBSERVED'),retryRemote:z.literal(false)
}).strict();

/** No normalization/defaults: the original JSON remains the digest authority. */
export function isHistoricalNotDispatchedDiagnostic(value:unknown):boolean{
  const parsed=historical.safeParse(value);
  if(!parsed.success)return false;
  const observed=parsed.data.observedStages;
  return observed[0]==='AUTHORIZATION_STARTED' && parsed.data.lastObservedStage===observed.at(-1) &&
    observed.every((stage,index)=>index===0 || stages.indexOf(stage)>stages.indexOf(observed[index-1]!));
}
