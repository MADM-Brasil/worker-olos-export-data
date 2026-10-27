/** Fixed vocabulary only. No driver messages, SQL outputs, parameters, stack or cause are serialized. */
const stagePhases = {
  AUTHORIZATION_STARTED:'AUTHORIZATION', PERMIT_ISSUED:'PERMIT_ISSUED',
  CONNECTION_INIT_STARTED:'CONNECTION_INIT', SOCKET_CONNECTED:'CONNECTION_INIT',
  PRELOGIN_SENT:'PRELOGIN', PRELOGIN_RESPONSE_RECEIVED:'PRELOGIN',
  TLS_HANDSHAKE_STARTED:'TLS_HANDSHAKE', TLS_VALIDATED:'TLS_HANDSHAKE',
  SQL_LOGIN_PREPARATION:'SQL_LOGIN', SQL_LOGIN_STARTED:'SQL_LOGIN', SQL_LOGIN_ACCEPTED:'SQL_LOGIN',
  RPC_DISPATCH_STARTED:'RPC_DISPATCH', RPC_HANDOFF_STARTED:'RPC_DISPATCH', RPC_HANDOFF_RETURNED:'RPC_DISPATCH',
  CAPTURE_FINALIZATION_STARTED:'RESPONSE_CAPTURE', RESPONSE_CAPTURE_STARTED:'RESPONSE_CAPTURE', RESPONSE_VALIDATION_STARTED:'RESPONSE_VALIDATION', RESPONSE_VALIDATED:'RESPONSE_VALIDATION',
} as const;
export type InspectionStage = keyof typeof stagePhases;
export type InspectionPhase = typeof stagePhases[InspectionStage];
const errorCodes = [
  'SERVICE_AUTHORIZATION_REJECTED','SERVICE_CONFIGURATION_CHANGED','OBSERVED_CONTRACT_NOT_APPROVED','ACTIVATION_REQUIREMENTS_MISSING','RECOVERY_NOT_ESTABLISHED','INSPECTION_EXTERNAL_RECONCILIATION_REQUIRED','POSTGRES_DESTINATION_CHANGED','RECOVERY_REQUIRED',
  'REAL_EXECUTION_BLOCKED','ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED','PROCEDURE_BLOCKED','CONTRACT_UNCONFIRMED','INVALID_PARAMETER',
  'CONNECTION_FAILED','EXECUTION_FAILED','TIMEOUT','INVALID_RESPONSE','OUTPUT_ERROR','ABORTED','BUSY','LIMIT_EXCEEDED','INCOMPLETE',
  'PIN_MISMATCH','PIN_APPROVAL_REQUIRED','PIN_APPROVAL_INVALID','PIN_APPROVAL_EXPIRED','PIN_CONFIGURATION_INVALID','PIN_TLS_REJECTED',
  'CERT_HAS_EXPIRED','CERT_NOT_YET_VALID','DEPTH_ZERO_SELF_SIGNED_CERT','SELF_SIGNED_CERT_IN_CHAIN','UNABLE_TO_VERIFY_LEAF_SIGNATURE',
  'ERR_TLS_CERT_ALTNAME_INVALID','ERR_SSL_TLSV1_ALERT_PROTOCOL_VERSION','INVALID_TDS',
  'ERR_SSL_WRONG_VERSION_NUMBER','ERR_SSL_SSLV3_ALERT_HANDSHAKE_FAILURE','ERR_SSL_UNEXPECTED_MESSAGE','ERR_SSL_BAD_RECORD_TYPE','ERR_SSL_UNEXPECTED_EOF_WHILE_READING','ERR_SSL_NO_SUITABLE_SIGNATURE_ALGORITHM','ERR_TLS_HANDSHAKE_TIMEOUT','ERR_STREAM_PREMATURE_CLOSE','ERR_OUT_OF_RANGE','ERR_BUFFER_OUT_OF_BOUNDS',
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
/** Prefer the original technical code over transport/operational wrappers, without exposing causes. */
export function inspectionTechnicalErrorCode(error:unknown,fallback:InspectionErrorCode='EXECUTION_FAILED'):InspectionErrorCode{
 let current=error,selected=inspectionErrorCode(error,fallback);
 for(let depth=0;depth<3 && current && typeof current==='object';depth++){
  const e=current as {technicalCode?:unknown;cause?:unknown};
  if(typeof e.technicalCode==='string' && allowed.has(e.technicalCode))return e.technicalCode as InspectionErrorCode;
  const code=inspectionErrorCode(current,fallback);
  if(!['ESOCKET','CONNECTION_FAILED','EXECUTION_FAILED'].includes(code))return code;
  selected=code;current=e.cause;
 }
 return selected;
}
const transportReasons=[
 'PRELOGIN_RESPONSE_RECEIVED','TDS_PACKET_STREAM_ERROR','TDS_MESSAGE_READ_FAILED','TDS_MESSAGE_STREAM_ERROR','PRELOGIN_DECODE_FAILED',
 'TLS_RESPONSE_TYPE_UNEXPECTED','TLS_RESPONSE_RECEIVED','TLS_HANDSHAKE_FAILED','TLS_CONFIGURATION_REJECTED',
 'TLS_STREAM_FAILED','TLS_SOCKET_ERROR','TLS_SOCKET_ENDED','TLS_SOCKET_CLOSED','TOKEN_PARSER_FAILED',
] as const;
export type TransportReason=typeof transportReasons[number];
const nativeErrorTypes=['Error','TypeError','RangeError','ConnectionError','RequestError','AggregateError','UNKNOWN'] as const;
type NativeErrorType=typeof nativeErrorTypes[number];
function nativeErrorType(value:unknown):NativeErrorType{return nativeErrorTypes.includes(value as NativeErrorType)?value as NativeErrorType:'UNKNOWN';}
export type TransportDetail={reason:TransportReason;errorCode?:InspectionErrorCode;packetType?:number;nativeErrorType?:NativeErrorType};
export function reportTransportDiagnostic(observer:InspectionDiagnosticObserver|undefined,reason:TransportReason,error?:unknown,packetType?:number){
 const detail:TransportDetail={reason};
 if(error!==undefined){detail.errorCode=inspectionTechnicalErrorCode(error,'CONNECTION_FAILED');detail.nativeErrorType=nativeErrorType(error && typeof error==='object'?error.constructor?.name:undefined);}
 if(typeof packetType==='number' && Number.isInteger(packetType) && packetType>=0 && packetType<=255)detail.packetType=packetType;
 observer?.({kind:'transport',...detail});
}
export type InspectionDiagnosticEvent = ({kind:'transport'} & TransportDetail) | {kind:'stage';stage:InspectionStage} | {kind:'failure';errorCode:InspectionErrorCode};
export type InspectionDiagnosticObserver = (event:InspectionDiagnosticEvent)=>void;
export function reportInspectionFailure(observer:InspectionDiagnosticObserver|undefined,error:unknown,fallback:InspectionErrorCode='EXECUTION_FAILED'){
  observer?.({kind:'failure',errorCode:inspectionTechnicalErrorCode(error,fallback)});
}
export class InspectionDiagnostics {
  private stage:InspectionStage='AUTHORIZATION_STARTED';
  private readonly observed:InspectionStage[]=['AUTHORIZATION_STARTED'];
  private failure:{phase:InspectionPhase;lastObservedStage:InspectionStage;errorCode:InspectionErrorCode}|null=null;
  private readonly transport:({phase:InspectionPhase;lastObservedStage:InspectionStage} & TransportDetail)[]=[];
  private dispatch:'NOT_OBSERVED'|'DRIVER_HANDOFF_STARTED'|'DRIVER_HANDOFF_RETURNED'='NOT_OBSERVED';
  private outcome:'NOT_OBSERVED'|'UNKNOWN'|'RESPONSE_VALIDATED'='NOT_OBSERVED';
  readonly observe:InspectionDiagnosticObserver=event=>{
    if(event.kind==='transport'){
      if(!transportReasons.includes(event.reason))return;
      const detail:TransportDetail={reason:event.reason};
      if(event.errorCode!==undefined)detail.errorCode=inspectionErrorCode({code:event.errorCode},'CONNECTION_FAILED');
      if(typeof event.packetType==='number' && Number.isInteger(event.packetType) && event.packetType>=0 && event.packetType<=255)detail.packetType=event.packetType;
      if(event.nativeErrorType!==undefined)detail.nativeErrorType=nativeErrorType(event.nativeErrorType);
      const entry={phase:stagePhases[this.stage],lastObservedStage:this.stage,...detail};
      if(this.transport.length<32)this.transport.push(entry);
      else if(detail.errorCode && !this.failure)this.transport[31]=entry;
      if(detail.errorCode && !this.failure)this.failure={phase:stagePhases[this.stage],lastObservedStage:this.stage,errorCode:detail.errorCode};
      return;
    }
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
  // v1 historically had two shapes. v2 always includes transport; never add a
  // required wire field without a new version and an explicit reader policy.
  snapshot(){return {version:'olos-log-inspection-diagnostic/v2' as const,
    phase:this.failure?.phase??stagePhases[this.stage],lastObservedStage:this.failure?.lastObservedStage??this.stage,
    errorCode:this.failure?.errorCode??null,observedStages:[...this.observed],rpcDispatchState:this.dispatch,
    remoteExecutionOutcome:this.outcome,retryRemote:false as const,transport:this.transport.map(detail=>({...detail}))};}
}
