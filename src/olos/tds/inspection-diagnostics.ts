/** Fixed vocabulary only. No driver messages, SQL outputs, parameters, stack or cause are serialized. */
const stagePhases = {
  AUTHORIZATION_STARTED:'AUTHORIZATION', PERMIT_ISSUED:'PERMIT_ISSUED',
  CONNECTION_INIT_STARTED:'CONNECTION_INIT', SOCKET_CONNECTED:'CONNECTION_INIT',
  TLS_HANDSHAKE_STARTED:'TLS_HANDSHAKE', TLS_VALIDATED:'TLS_HANDSHAKE',
  SQL_LOGIN_PREPARATION:'SQL_LOGIN', SQL_LOGIN_STARTED:'SQL_LOGIN', SQL_LOGIN_ACCEPTED:'SQL_LOGIN',
  RPC_DISPATCH_STARTED:'RPC_DISPATCH', RPC_HANDOFF_STARTED:'RPC_DISPATCH', RPC_HANDOFF_RETURNED:'RPC_DISPATCH',
  CAPTURE_FINALIZATION_STARTED:'RESPONSE_CAPTURE', RESPONSE_CAPTURE_STARTED:'RESPONSE_CAPTURE', RESPONSE_VALIDATION_STARTED:'RESPONSE_VALIDATION', RESPONSE_VALIDATED:'RESPONSE_VALIDATION',
} as const;
export type InspectionStage = keyof typeof stagePhases;
export type InspectionPhase = typeof stagePhases[InspectionStage];
const errorCodes = [
  'REAL_EXECUTION_BLOCKED','ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED','PROCEDURE_BLOCKED','CONTRACT_UNCONFIRMED','INVALID_PARAMETER',
  'CONNECTION_FAILED','EXECUTION_FAILED','TIMEOUT','INVALID_RESPONSE','OUTPUT_ERROR','ABORTED','BUSY','LIMIT_EXCEEDED','INCOMPLETE',
  'PIN_MISMATCH','PIN_APPROVAL_REQUIRED','PIN_APPROVAL_INVALID','PIN_APPROVAL_EXPIRED','PIN_CONFIGURATION_INVALID','PIN_TLS_REJECTED',
  'CERT_HAS_EXPIRED','CERT_NOT_YET_VALID','DEPTH_ZERO_SELF_SIGNED_CERT','SELF_SIGNED_CERT_IN_CHAIN','UNABLE_TO_VERIFY_LEAF_SIGNATURE',
  'ERR_TLS_CERT_ALTNAME_INVALID','ERR_SSL_TLSV1_ALERT_PROTOCOL_VERSION','INVALID_TDS',
  'ELOGIN','ESOCKET','ETIMEOUT','ECONNREFUSED','ECONNRESET','EPIPE','ENOTFOUND','EHOSTUNREACH','ENETUNREACH','ETIMEDOUT',
  'STORAGE_FAILED','DISK_FULL','ENOSPC','EDQUOT','EACCES','EPERM','ENOENT','EEXIST','QUOTA_EXCEEDED','CORRUPT',
  'CORPORATE_APPROVAL_EXPIRED','REMOTE_OPERATION_REQUIRES_REVIEW','LOG_INSPECTION_DATE_MISMATCH',
] as const;
export type InspectionErrorCode = typeof errorCodes[number];
const allowed = new Set<string>(errorCodes);
export function inspectionErrorCode(error:unknown,fallback:InspectionErrorCode='EXECUTION_FAILED'):InspectionErrorCode {
  // Follow only a bounded cause chain, returning an exact allowlisted code, never its surrounding text.
  let current=error;
  for(let depth=0;depth<3 && current && typeof current==='object';depth++){
    const e=current as {code?:unknown;message?:unknown;cause?:unknown};
    if(typeof e.code==='string' && allowed.has(e.code))return e.code as InspectionErrorCode;
    if(typeof e.message==='string' && allowed.has(e.message))return e.message as InspectionErrorCode;
    current=e.cause;
  }
  return allowed.has(fallback)?fallback:'EXECUTION_FAILED';
}
export type InspectionDiagnosticEvent = {kind:'stage';stage:InspectionStage} | {kind:'failure';errorCode:InspectionErrorCode};
export type InspectionDiagnosticObserver = (event:InspectionDiagnosticEvent)=>void;
export function reportInspectionFailure(observer:InspectionDiagnosticObserver|undefined,error:unknown,fallback:InspectionErrorCode='EXECUTION_FAILED'){
  observer?.({kind:'failure',errorCode:inspectionErrorCode(error,fallback)});
}
export class InspectionDiagnostics {
  private stage:InspectionStage='AUTHORIZATION_STARTED';
  private readonly observed:InspectionStage[]=['AUTHORIZATION_STARTED'];
  private failure:{phase:InspectionPhase;lastObservedStage:InspectionStage;errorCode:InspectionErrorCode}|null=null;
  private dispatch:'NOT_OBSERVED'|'DRIVER_HANDOFF_STARTED'|'DRIVER_HANDOFF_RETURNED'='NOT_OBSERVED';
  private outcome:'NOT_OBSERVED'|'UNKNOWN'|'RESPONSE_VALIDATED'='NOT_OBSERVED';
  readonly observe:InspectionDiagnosticObserver=event=>{
    if(this.failure)return; // Cleanup/wrapping cannot replace the first observed failure.
    if(event.kind==='failure'){
      this.failure={phase:stagePhases[this.stage],lastObservedStage:this.stage,errorCode:inspectionErrorCode({code:event.errorCode})};return;
    }
    if(!Object.hasOwn(stagePhases,event.stage))return;
    this.stage=event.stage;
    if(!this.observed.includes(event.stage))this.observed.push(event.stage);
    if(event.stage==='RPC_HANDOFF_STARTED'){this.dispatch='DRIVER_HANDOFF_STARTED';this.outcome='UNKNOWN';}
    if(event.stage==='RPC_HANDOFF_RETURNED')this.dispatch='DRIVER_HANDOFF_RETURNED';
    if(event.stage==='RESPONSE_CAPTURE_STARTED')this.outcome='UNKNOWN';
    if(event.stage==='RESPONSE_VALIDATED')this.outcome='RESPONSE_VALIDATED';
  };
  snapshot(){return {version:'olos-log-inspection-diagnostic/v1' as const,
    phase:this.failure?.phase??stagePhases[this.stage],lastObservedStage:this.failure?.lastObservedStage??this.stage,
    errorCode:this.failure?.errorCode??null,observedStages:[...this.observed],rpcDispatchState:this.dispatch,
    remoteExecutionOutcome:this.outcome,retryRemote:false as const};}
}
