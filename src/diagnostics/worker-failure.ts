import {inspectionErrorCode} from '../olos/tds/inspection-diagnostics.js';

const messages:Record<string,string>={
 EXECUTION_FAILED:'Failure details omitted',
 '55P03':'PostgreSQL lock unavailable', '57014':'PostgreSQL query cancelled',
 '40P01':'PostgreSQL deadlock detected', '40001':'PostgreSQL serialization failure',
 '23505':'PostgreSQL unique constraint violation', '23503':'PostgreSQL foreign key violation',
 '23514':'PostgreSQL check constraint violation', '22001':'PostgreSQL value too long',
 '23502':'PostgreSQL null constraint violation', '22003':'PostgreSQL numeric value out of range',
 'P0001':'PostgreSQL raised exception', '08006':'PostgreSQL connection failure',
 '22P02':'PostgreSQL invalid representation', '42501':'PostgreSQL permission denied',
 '42P01':'PostgreSQL relation missing', '25P03':'PostgreSQL idle transaction timeout',
 '53300':'PostgreSQL connection limit', '57P01':'PostgreSQL shutdown',
 PG_QUERY_TIMEOUT:'PostgreSQL client query timeout',
};
const codes=new Set(['PERSISTENCE_FAILED','COMMIT_UNKNOWN','VALIDATION_FAILED','CHECKPOINT_CONFLICT','LEASE_LOST','BATCH_CONFLICT','SCHEMA_NOT_READY',
 'PROJECTION_CAPTURE_MISSING','DELIVERY_REQUIRES_RECONCILIATION','TARGET_UNKNOWN','TARGET_NOT_COMMITTED','RECEIPT_CONFLICT','RECEIPT_ABSENT',
 'PROJECTION_REJECTED','DESTINATION_BLOCKED','DESTINATION_VERSION_MISMATCH','CAPTURE_QUEUE_REQUIRES_REVIEW','CAPTURE_QUEUE_RECOVERY_REQUIRED',
 'LOCAL_COMMIT_REQUIRES_REVIEW','BOOTSTRAP_PERSISTENCE_NOT_CONFIRMED','SOURCE_INTERRUPTED','SOURCE_BLOCKED','INVALID_CONFIG','INVALID_VALUE',
 'INSTANCE_BUSY','INSTANCE_LOST','ROLE_REJECTED','PRIVILEGE_REJECTED','POSTGRES_RUNTIME_NOT_READY','STOPPED',...Object.keys(messages)]);
const stages=['START','INITIALIZE','PROBE','INSTANCE_LOCK','ACQUIRE','AUTHORIZATION','ADMISSION','PREFLIGHT','CAPTURE','HANDOFF',
 'DELIVER','MATERIALIZE','VALIDATE_BATCH','RECONCILE','CLAIM','PREPARE','AUTHORIZE','DESTINATION','SETTLE','DELIVERY_STATUS','CHECKPOINT','ACK'] as const;
const flows=['acquisition','delivery','initialization','probe','instance'] as const;
export type FailureContext={flow?:typeof flows[number]|undefined;stage?:typeof stages[number]|undefined;captureId?:string|undefined;batchId?:string|undefined;streamId?:string|undefined};
type Metadata=FailureContext & {technicalCode?:string;technicalMessage?:string;causeCodes?:readonly string[]};
const metadata=new WeakMap<object,Metadata>();
function cleanContext(context:FailureContext):FailureContext{
 const result:FailureContext={};
 if(flows.includes(context.flow!))result.flow=context.flow;
 if(stages.includes(context.stage!))result.stage=context.stage;
 for(const key of ['captureId','batchId','streamId'] as const)if(typeof context[key]==='string' && /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i.test(context[key]!))result[key]=context[key];
 return result;
}
function codeOf(error:unknown){
 const e=error as {code?:unknown;message?:unknown}|null;
 for(const value of [e?.code,e?.message])if(typeof value==='string' && codes.has(value))return value;
 // Exact known pg client text only; never copy interpolated SQL, values or driver detail.
 if(e?.message==='Query read timeout')return 'PG_QUERY_TIMEOUT';
 return inspectionErrorCode(error);
}
export function failureDetails(error:unknown,context:FailureContext={}){
 const saved=error && typeof error==='object'?metadata.get(error):undefined,code=codeOf(error);
 const outcome=(error as {outcome?:unknown}|null)?.outcome;
 return {code,message:messages[code]??code,...(outcome==='unknown'||outcome==='not_committed'?{outcome}:{}),
  ...cleanContext(context),...saved};
}
/** Preserve the deepest stage and attach only internal UUIDs; keep instanceof/error codes intact. */
export function annotateFailure(error:unknown,context:FailureContext):unknown{
 if(error && typeof error==='object')metadata.set(error,{...cleanContext(context),...metadata.get(error)});
 return error;
}
/** Wrappers retain sanitized cause information, never the raw driver error or its stack. */
export function preserveFailure<T extends Error>(wrapper:T,cause:unknown):T{
 const detail=failureDetails(cause);
 const technicalCode=detail.technicalCode && detail.technicalCode!=='EXECUTION_FAILED'?detail.technicalCode:detail.code;
 metadata.set(wrapper,{...cleanContext(detail),technicalCode,technicalMessage:messages[technicalCode]??technicalCode,
  causeCodes:[detail.code,...detail.causeCodes??[]].slice(0,4)});
 return wrapper;
}
export function logWorkerFailure(event:'worker_needs_reconciliation'|'worker_start_failed'|'worker_flow_failed'|'worker_delivery_deferred',error:unknown,context:FailureContext={},retryAt?:string){
 const entry={level:event==='worker_delivery_deferred'?40:50,event,at:new Date().toISOString(),...failureDetails(error,context),...(retryAt?{retryAt}:{}),retryRemote:false};
 process.stderr.write(`${JSON.stringify(entry)}\n`);
 return entry;
}
