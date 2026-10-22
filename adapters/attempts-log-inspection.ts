import {mkdir,open,lstat} from 'node:fs/promises';
import {constants,existsSync,openSync,readFileSync,fstatSync,closeSync} from 'node:fs';
import {join} from 'node:path';
import {z} from 'zod';
import {durableExclusive,privateDirectory,privateJson} from './disposition-integration.js';
import {LocalSpool} from './local-spool.js';
import {digest,validDate} from '../src/persistence/validation.js';
import {loadOlosPinApproval,assertOlosPinApproval,type OlosPinApproval} from '../src/security/olos-certificate-pin.js';
import {prepareCorporateAttemptsEndpoint,authorizeCorporateLogInspection} from '../src/olos/tds/attempts-access.js';
import {reportedSignature} from '../src/olos/procedure-signatures.js';
import {IsolatedTdsSource} from '../src/olos/tds/source.js';
import {parserLimits} from '../src/olos/tds/raw-parser.js';
import {spoolLimits} from '../src/large-batch/config.js';

const authorizationSchema=z.object({scope:z.literal('OLOS_LOGREADDATA_INSPECTION_ONLY'),operationId:z.string().uuid(),
  operator:z.string().trim().min(1).max(100),reference:z.string().trim().min(1).max(200),
  procedure:z.literal('LogReadData'),tableName:z.literal('AttemptsRawData'),dateExport:z.string().refine(validDate),
  issuedAt:z.string().datetime(),expiresAt:z.string().datetime(),
  pinApprovalId:z.string().min(1),fingerprintSha256:z.string().min(1)}).strict();
export const logInspectionConfigSchema=z.object({version:z.literal('olos-log-inspection-config/v1'),endpoint:z.unknown(),
  pinApprovalFile:z.string().startsWith('/'),authorization:authorizationSchema}).strict();
export type LogInspectionConfig=z.infer<typeof logInspectionConfigSchema>;
async function syncDirectory(path:string){const f=await open(path,constants.O_RDONLY|constants.O_DIRECTORY|constants.O_NOFOLLOW);try{await f.sync();}finally{await f.close();}}
async function absent(path:string){try{await lstat(path);}catch(e){if((e as NodeJS.ErrnoException).code==='ENOENT')return;throw e;}throw Error('REMOTE_OPERATION_REQUIRES_REVIEW');}
function validate(config:LogInspectionConfig,pin:OlosPinApproval,date:string){
  assertOlosPinApproval(pin);const a=config.authorization,now=Date.now();
  if(!validDate(date) || date!==a.dateExport)throw Error('LOG_INSPECTION_DATE_MISMATCH');
  if(Date.parse(a.issuedAt)>now || Date.parse(a.expiresAt)<=now || Date.parse(a.expiresAt)<=Date.parse(a.issuedAt) || Date.parse(a.expiresAt)-Date.parse(a.issuedAt)>12*3600000)throw Error('CORPORATE_APPROVAL_EXPIRED');
  if(a.pinApprovalId!==pin.approvalId || a.fingerprintSha256!==pin.fingerprintSha256)throw Error('PIN_MISMATCH');
}
/** Exclusive operation ID comes from the private authorization; a second invocation cannot create another case. */
export async function prepareLogInspection(base:string,config:LogInspectionConfig,pin:OlosPinApproval,dateExport:string){
  validate(config,pin,dateExport);
  await mkdir(base,{recursive:true,mode:0o700});await privateDirectory(base);
  const root=join(base,'log-inspections');await mkdir(root,{recursive:true,mode:0o700});await privateDirectory(root);await syncDirectory(base);
  const attemptId=config.authorization.operationId,workDir=join(root,attemptId);
  await mkdir(workDir,{mode:0o700});await privateDirectory(workDir);await syncDirectory(root);
  const configurationDigest=digest(config),a=config.authorization;
  const intent={kind:'olos-log-inspection/v1',scope:a.scope,attemptId,procedure:a.procedure,tableName:a.tableName,dateExport,
    configurationDigest,operator:a.operator,approvalReference:a.reference,pinApprovalId:pin.approvalId,
    fingerprintSha256:pin.fingerprintSha256,issuedAt:a.issuedAt,expiresAt:a.expiresAt,createdAt:new Date().toISOString(),state:'PREPARED'};
  const intentPath=join(workDir,'intent.json'),pendingPath=join(workDir,'pending.json');
  await durableExclusive(intentPath,intent);
  await durableExclusive(pendingPath,{purpose:'log-inspection',attemptId,configurationDigest,workDir,intentDigest:digest(intent)});
  // Shared exclusion only; investigation bytes never enter the acquisition spool. Keep claim after all outcomes for review.
  await durableExclusive(join(base,'log-inspection-active.json'),{attemptId,workDir,configurationDigest});
  await absent(join(base,'remote-pending.json'));
  const spool=await LocalSpool.open(join(workDir,'spool'),spoolLimits());
  const capture=await spool.createResponseCapture(attemptId);
  return {attemptId,workDir,intent,intentPath,pendingPath,capture};
}

/** No PostgreSQL import, pool, mapping, scheduler or acquisition authorization is used by this operation. */
export async function executeLogInspection(configInput:unknown,pin:OlosPinApproval,base:string,dateExport:string,
  makeSource=(...args:ConstructorParameters<typeof IsolatedTdsSource>)=>new IsolatedTdsSource(...args)){
  const config=logInspectionConfigSchema.parse(configInput);validate(config,pin,dateExport);
  const signature=reportedSignature('LogReadData',{evidence:'server-reported LogReadData signature; manual divergence retained; result layout unapproved',
    nullable:{tablename:false,DateExport:false,Message:true,ErrorMessage:true}});
  const values={tablename:'AttemptsRawData',DateExport:dateExport};
  const endpoint=prepareCorporateAttemptsEndpoint(config.endpoint,pin,signature,values);
  const prepared=await prepareLogInspection(base,config,pin,dateExport);
  const {attemptId,workDir,capture}=prepared,abort=new AbortController(),cancel=()=>abort.abort();
  let rawComplete=false,source:IsolatedTdsSource|undefined,received=0,completion:Record<string,unknown>|undefined;
  let status='REMOTE_OUTCOME_UNKNOWN',failure=false;
  process.once('SIGINT',cancel);process.once('SIGTERM',cancel);
  try{
    await authorizeCorporateLogInspection(endpoint,()=>{
      validate(config,pin,dateExport);
      if(existsSync(join(base,'remote-pending.json')))throw Error('REMOTE_OPERATION_REQUIRES_REVIEW');
      const fd=openSync(join(base,'log-inspection-active.json'),constants.O_RDONLY|constants.O_NOFOLLOW);
      try{const st=fstatSync(fd);if(!st.isFile() || st.uid!==process.getuid?.() || (st.mode&0o777)!==0o600 || st.size>32768 ||
        digest(JSON.parse(readFileSync(fd,'utf8')))!==digest({attemptId,workDir,configurationDigest:prepared.intent.configurationDigest}))throw Error('REMOTE_OPERATION_REQUIRES_REVIEW');}finally{closeSync(fd);}
      if(abort.signal.aborted)throw Error('ABORTED');},
      async()=>({intentPath:prepared.intentPath,pendingPath:prepared.pendingPath}),prepared.intent);
    // Durable conservative marker BEFORE any socket. Survives SIGKILL and ambiguous dispatch failures.
    await durableExclusive(join(workDir,'remote-outcome-unknown.json'),{attemptId,state:'REMOTE_OUTCOME_UNKNOWN',retryRemote:false});
    async function* replay(){
      // Called only after the complete TDS response message has reached the capture sink.
      rawComplete=true;
      const f=await open(join(workDir,'spool','batches',attemptId,'response.partial'),constants.O_RDONLY|constants.O_NOFOLLOW);
      try{for(;;){const b=Buffer.alloc(65536),r=await f.read(b,0,b.length,null);if(!r.bytesRead)break;yield b.subarray(0,r.bytesRead);}}finally{await f.close();}
    }
    source=makeSource(endpoint,[],parserLimits,chunk=>capture.append(chunk),replay);
    for await(const event of source.call(signature,values,abort.signal)){
      if(event.kind==='row')received++;
      else completion={returnValue:event.returnValue,datasetCount:event.datasetCount,errorMessagePresent:Boolean(event.errorMessage),messagePresent:Boolean(event.message)};
    }
    if(!completion || !rawComplete)throw Error('INCOMPLETE');
    status=completion.returnValue===0 && !completion.errorMessagePresent?'RESPONSE_SEALED':'REVIEW_REQUIRED';
  }catch{failure=true;}
  finally{process.removeListener('SIGINT',cancel);process.removeListener('SIGTERM',cancel);abort.abort();}
  // Even unsupported metadata/SQL errors retain the entire received message for private human review.
  const detail={received:completion?received:null,parsedRows:received,fields:source?.observedFields??[],metadata:source?.observedMetadata??[],
    datasets:source?.observedDatasets??[],transportSecurity:source?.transportSecurity??[],completion:completion??null,observedOutcome:source?.responseObservation??null,state:status,
    rawMessageComplete:rawComplete,responseValidated:!failure,reviewRequired:true,rangeAttribution:'REMOTE_RANGE_ATTRIBUTION_NOT_ESTABLISHED'};
  let sealed:{sha256:string;bytes:number}|undefined;
  if(rawComplete)sealed=await capture.seal(detail);
  else await capture.interrupt(detail);
  const result={event:'corporate_log_inspection_requires_review',attemptId,dateExport,state:failure?(rawComplete?'REVIEW_REQUIRED':'REMOTE_OUTCOME_UNKNOWN'):status,
    responseComplete:!failure && Boolean(completion),rawMessageComplete:rawComplete,responseValidated:!failure,received:completion?received:null,datasetCount:source?.responseObservation.datasetCount??null,
    returnValue:source?.responseObservation.returnValue??null,...sealed,persisted:0,checkpointAdvanced:false,retryRemote:false,reviewRequired:true,
    rangeAttribution:'REMOTE_RANGE_ATTRIBUTION_NOT_ESTABLISHED'};
  await durableExclusive(join(workDir,'result.json'),result);
  return result;
}
export async function runLogInspection(configFile:string,base:string,dateExport:string){
  let input:unknown;
  try{input=await privateJson(configFile);}catch{throw Error('LOG_INSPECTION_PRIVATE_AUTHORIZATION_REQUIRED');}
  const parsed=logInspectionConfigSchema.safeParse(input);
  if(!parsed.success)throw Error('LOG_INSPECTION_PRIVATE_AUTHORIZATION_REQUIRED');
  const pin=await loadOlosPinApproval(parsed.data.pinApprovalFile);
  return executeLogInspection(parsed.data,pin,base,dateExport);
}
