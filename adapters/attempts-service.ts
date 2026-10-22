import {mkdir,readdir,lstat,open,rename} from 'node:fs/promises';
import {constants,openSync,readFileSync,fstatSync,closeSync,existsSync,realpathSync} from 'node:fs';
import {join,dirname} from 'node:path';
import {randomUUID} from 'node:crypto';
import {z} from 'zod';
import type {PoolClient} from 'pg';
import {privateDirectory,privateJson,durableExclusive} from './disposition-integration.js';
import {LocalSpool} from './local-spool.js';
import {localSettings} from './local-worker.js';
import {loadOlosPinApproval} from '../src/security/olos-certificate-pin.js';
import {serviceConfigSchema,validateServiceApproval,contractReviewSchema,validateReviewedContract,validateActivation,planForCapture,acquisitionProcedures,type ServiceConfig,type ContractReview,type AcquisitionProcedure,type ServiceApproval} from '../src/integration/attempts/activation.js';
import {digest,canonical} from '../src/persistence/validation.js';
import {catalog,recordsFromJson,parseLossless,type Catalogs} from '../src/offline/model.js';
import {spoolLimits} from '../src/large-batch/config.js';
import {reportedSignature} from '../src/olos/procedure-signatures.js';
import {blockingCorporateInspection,prepareCorporateAttemptsEndpoint,authorizeCorporateAttemptsOnce} from '../src/olos/tds/attempts-access.js';
import {IsolatedTdsSource,replayTdsResponse} from '../src/olos/tds/source.js';
import {parserLimits} from '../src/olos/tds/raw-parser.js';
import {InspectionDiagnostics,inspectionErrorCode,reportInspectionFailure} from '../src/olos/tds/inspection-diagnostics.js';
import {createApprovedCaptureReplayBinding} from '../src/integration/attempts/corporate.js';
import {decodeWireRow} from '../src/large-batch/wire.js';
import {projectTdsAttempts} from '../src/offline/tds-projection.js';
import {IsolatedTdsCallsWriter} from '../src/offline/tds-writer.js';
import {createAttemptsPipeline} from '../src/integration/attempts/pipeline.js';
import {PostgresLargeBatchPersistence} from '../src/persistence/large-batch.js';
import type {SpoolManifest} from '../src/large-batch/types.js';
import {AttemptsService,type RuntimePools} from '../src/integration/attempts/service.js';
import {createLocalRuntimePools} from '../src/integration/attempts/pools.js';
import {serveWorker} from '../src/main.js';

const text=z.string().trim().min(1).max(1000),sha=z.string().regex(/^[a-f0-9]{64}$/);
export async function serviceExists(path:string){try{await lstat(path);return true;}catch(e){if((e as NodeJS.ErrnoException).code==='ENOENT')return false;throw e;}}
async function syncDir(path:string){const f=await open(path,constants.O_RDONLY|constants.O_DIRECTORY|constants.O_NOFOLLOW);try{await f.sync();}finally{await f.close();}}
export function privateServiceJsonSync(path:string):unknown{
 if(realpathSync(path)!==path)throw Error('PRIVATE_CONFIGURATION_REQUIRED');
 const fd=openSync(path,constants.O_RDONLY|constants.O_NOFOLLOW);
 try{const s=fstatSync(fd);if(!s.isFile() || s.uid!==process.getuid?.() || s.mode&0o077 || s.size>131072)throw Error('PRIVATE_CONFIGURATION_REQUIRED');try{return JSON.parse(readFileSync(fd,'utf8'));}catch{throw Error('PRIVATE_CONFIGURATION_REQUIRED');}}finally{closeSync(fd);}
}
export async function loadServiceConfiguration(path:string){
 const config=serviceConfigSchema.parse(await privateJson(path));
 const campaigns=catalog(recordsFromJson(parseLossless(JSON.stringify(await privateJson(config.campaignFile))),true),'CampaignId','Description');
 const dispositions=catalog(recordsFromJson(parseLossless(JSON.stringify(await privateJson(config.dispositionFile))),true),'DispositionId','Description');
 const catalogs:Catalogs={campaigns,dispositions,agents:new Map()};
 return {config,catalogs,configurationDigest:digest({config,campaigns:[...campaigns],dispositions:[...dispositions]})};
}
type Loaded=Awaited<ReturnType<typeof loadServiceConfiguration>>;
export function serviceGuard(configFile:string,loaded:Loaded,approval:ServiceApproval){
 const {config,configurationDigest}=loaded;
 return ()=>{
  try{
  if(digest(privateServiceJsonSync(configFile))!==digest(config))throw Error('SERVICE_CONFIGURATION_CHANGED');
  const campaigns=catalog(recordsFromJson(parseLossless(JSON.stringify(privateServiceJsonSync(config.campaignFile))),true),'CampaignId','Description');
  const dispositions=catalog(recordsFromJson(parseLossless(JSON.stringify(privateServiceJsonSync(config.dispositionFile))),true),'DispositionId','Description');
  if(digest({config,campaigns:[...campaigns],dispositions:[...dispositions]})!==configurationDigest)throw Error('SERVICE_CONFIGURATION_CHANGED');
  const current=validateServiceApproval(privateServiceJsonSync(config.approvalFile),config,configurationDigest,existsSync(approval.revocationFile));
  if(digest(current)!==digest(approval))throw Error('SERVICE_AUTHORIZATION_REJECTED');
  }catch(e){if(e instanceof Error && ['SERVICE_CONFIGURATION_CHANGED','SERVICE_AUTHORIZATION_REJECTED','DESTINATION_MAPPING_NOT_APPROVED'].includes(e.message))throw e;throw Error('SERVICE_AUTHORIZATION_REJECTED');}
 };
}
async function directories(base:string,root:string){await mkdir(base,{recursive:true,mode:0o700});await privateDirectory(base);await mkdir(root,{recursive:true,mode:0o700});await privateDirectory(root);await syncDir(base);}
export const captureIntentSchema=z.object({attemptId:z.string().uuid(),procedure:z.enum(acquisitionProcedures),configurationDigest:sha,
 workDir:z.string(),createdAt:z.string().datetime(),controlDate:z.string(),operator:text,approvalReference:text,pinApprovalId:text,
 contractReviewDigest:sha.nullable(),plan:z.unknown(),state:z.literal('PREPARED')}).strict();
type Intent=z.infer<typeof captureIntentSchema>;
export async function pendingServiceCase(base:string,root:string):Promise<Intent|null>{
 const pendingPath=join(base,'remote-pending.json');if(!await serviceExists(pendingPath))return null;
 const pending=await privateJson(pendingPath) as {attemptId:string;workDir:string;configurationDigest:string};
 if(pending.workDir!==root)throw Error('ANOTHER_ACQUISITION_REQUIRES_REVIEW');
 const intent=captureIntentSchema.parse(await privateJson(join(root,`attempt-${pending.attemptId}.json`)));
 if(intent.workDir!==root || intent.configurationDigest!==pending.configurationDigest)throw Error('RECOVERY_REQUIRED');return intent;
}
export async function observeServiceCapture(base:string,root:string,loaded:Loaded,approval:ServiceApproval,guard:()=>void,
 procedure:AcquisitionProcedure,signal:AbortSignal,review?:ContractReview,
 makeSource=(...args:ConstructorParameters<typeof IsolatedTdsSource>)=>new IsolatedTdsSource(...args)){
 guard();if(await serviceExists(join(base,'remote-pending.json')))throw Error('RECOVERY_REQUIRED');
 const pin=await loadOlosPinApproval(loaded.config.pinApprovalFile);
 const signature=reportedSignature(procedure,{evidence:'server reported acquisition signature; observed response remains quarantined',nullable:{TableName:false,Message:true,ErrorMessage:true}});
 const values={TableName:'AttemptsRawData'},endpoint=prepareCorporateAttemptsEndpoint(loaded.config.endpoint,pin,signature,values);
 const attemptId=randomUUID(),caseDir=join(root,'cases',attemptId);await mkdir(join(root,'cases'),{recursive:true,mode:0o700});await privateDirectory(join(root,'cases'));
 await mkdir(caseDir,{mode:0o700});await syncDir(join(root,'cases'));
 const createdAt=new Date().toISOString(),plan=review?planForCapture(review,new Date(createdAt)):null;
 const intent:Intent={attemptId,procedure,configurationDigest:loaded.configurationDigest,workDir:root,createdAt,
  controlDate:plan?.run.exportDate??new Date().toISOString().slice(0,10),operator:approval.operator,approvalReference:approval.reference,
  pinApprovalId:pin.approvalId,contractReviewDigest:review?digest(review):null,plan,state:'PREPARED'};
 const intentPath=join(root,`attempt-${attemptId}.json`),pendingPath=join(base,'remote-pending.json');
 const spool=await LocalSpool.open(join(caseDir,'capture'),spoolLimits()),capture=await spool.createResponseCapture(attemptId),diagnostics=new InspectionDiagnostics();
 let received=0,complete=false,rawComplete=false,storageError:unknown,source:IsolatedTdsSource|undefined;
 async function* capturedResponse(){rawComplete=true;const f=await open(join(caseDir,'capture','batches',attemptId,'response.partial'),constants.O_RDONLY|constants.O_NOFOLLOW);
  try{for(;;){const b=Buffer.alloc(65536),r=await f.read(b,0,b.length,null);if(!r.bytesRead)break;yield b.subarray(0,r.bytesRead);}}finally{await f.close();}}
 try{
  await authorizeCorporateAttemptsOnce(endpoint,()=>{guard();if(digest(privateServiceJsonSync(loaded.config.pinApprovalFile))!==digest(pin))throw Error('PIN_APPROVAL_INVALID');if(signal.aborted)throw Error('ABORTED');},async()=>{
   await durableExclusive(intentPath,intent);
   await durableExclusive(pendingPath,{attemptId,workDir:root,configurationDigest:loaded.configurationDigest});
   return {intentPath,pendingPath};
  });
  diagnostics.observe({kind:'stage',stage:'PERMIT_ISSUED'});
  await durableExclusive(join(caseDir,'remote-state.json'),{state:'REMOTE_OUTCOME_UNKNOWN',attemptId,retryRemote:false});
  source=makeSource(endpoint,[],parserLimits,async chunk=>{diagnostics.observe({kind:'stage',stage:'RESPONSE_CAPTURE_STARTED'});try{await capture.append(chunk);}catch(e){storageError=e;throw e;}},undefined,diagnostics.observe,capturedResponse);
  for await(const event of source.call(signature,values,signal)){
   if(event.kind==='row')received++;
   else{if(event.returnValue!==0 || event.errorMessage || event.message)throw Error('OUTPUT_ERROR');complete=true;}
  }
  if(!complete)throw Error('INCOMPLETE');
  await capture.seal({received,fields:source.observedFields,metadata:source.observedMetadata,transportSecurity:source.transportSecurity,procedure,
   configurationDigest:loaded.configurationDigest,metadataState:'observed-unapproved',responseValidated:true,rawMessageComplete:rawComplete});
  await durableExclusive(join(caseDir,'diagnostic.json'),{attemptId,...diagnostics.snapshot()});
  return intent;
 }catch(error){reportInspectionFailure(diagnostics.observe,storageError??error);
  if(rawComplete && !storageError)await capture.seal({received,fields:source?.observedFields??[],metadata:source?.observedMetadata??[],transportSecurity:source?.transportSecurity??[],procedure,responseValidated:false,rawMessageComplete:true}).catch(()=>{});
  await capture.interrupt().catch(()=>{});
  const diagnostic={attemptId,...diagnostics.snapshot()};
  await durableExclusive(join(caseDir,'diagnostic.json'),diagnostic).catch(()=>{});
  if(diagnostic.rpcDispatchState==='NOT_OBSERVED' && !diagnostic.observedStages.includes('RESPONSE_CAPTURE_STARTED')){
   await durableExclusive(join(caseDir,'not-dispatched.json'),{kind:'olos-local-not-dispatched/v1',attemptId,diagnosticDigest:digest(diagnostic),evidence:'CONTROLLED_RPC_HANDOFF_NOT_REACHED_AFTER_TRANSPORT_CLOSED'}).catch(()=>{});
  }
  throw error;}
}
async function captureInfo(root:string,id:string){
 const intent=captureIntentSchema.parse(await privateJson(join(root,`attempt-${id}.json`)));
 const spool=await LocalSpool.open(join(root,'cases',id,'capture'),spoolLimits());const response=await spool.responseCapture(id);
 const metadata=await privateJson(join(root,'cases',id,'capture','batches',id,'response.json')) as {responseValidated?:boolean;transportSecurity?:{encrypted:boolean;identityValidated:boolean;protocol:string;tlsMode:string}[]};
 if(metadata.responseValidated!==true || metadata.transportSecurity?.length!==1 || !metadata.transportSecurity.every(t=>t.encrypted && t.identityValidated && t.protocol==='TLSv1.2' && t.tlsMode==='certificate-pinned'))throw Error('OBSERVED_CONTRACT_NOT_APPROVED');
 // Verify bytes even when only reviewing/activating; a JSON descriptor alone is not evidence.
 for await(const _ of spool.responseChunks(id)){}
 return {intent,spool,response};
}
export async function prepareServiceContract(root:string,loaded:Loaded,id:string){
 z.string().uuid().parse(id);const {intent,response}=await captureInfo(root,id);
 if(intent.configurationDigest!==loaded.configurationDigest)throw Error('SERVICE_CONFIGURATION_CHANGED');
 const fields=response.fields as ContractReview['fields'];
 const proposal={version:'olos-contract-review/v1',procedure:intent.procedure,captureId:id,captureSha256:response.sha256,
  configurationDigest:loaded.configurationDigest,operator:'',reference:'',reviewedAt:'',sourceTimeZone:'',controlDayOffset:null,partitionSemanticsEvidence:'',fields,
  plan:{run:{table:'AttemptsRawData',contractVersion:'',readType:intent.procedure==='ReadDataCurrentDay'?'current_day':'previous_days',exportDate:intent.controlDate,pkRange:null},
   contract:{source_table:'AttemptsRawData',version:'',approved:false,fields:Object.fromEntries(fields.map(f=>[f.name,{type:f.type==='datetime'?'sql_datetime_ticks':f.type==='int' || f.type==='bigint'?'int64':f.type==='decimal'?'decimal':'text',nullable:f.nullable}])),key_fields:[],layout_evidence:null,key_evidence:null},
   fields:fields.map(({sqlLength:_s,collationHex:_c,unicodeMetadataPolicy:_u,...f})=>f),
   signature:reportedSignature(intent.procedure,{evidence:'server-reported RPC signature; result contract NOT approved',nullable:{TableName:false,Message:true,ErrorMessage:true}}),
   parameters:{TableName:'AttemptsRawData'},acquisitionEvidence:'',responseEvidence:'',partitionEvidence:'',successReturnValues:[0]}};
 const path=join(root,'cases',id,'contract-candidate.json');await durableExclusive(path,proposal);
 return {captureId:id,proposalFile:path,contractApproved:false,imported:0};
}
export async function reviewServiceContract(root:string,loaded:Loaded,reviewInput:unknown){
 const review=contractReviewSchema.parse(reviewInput),{intent,response,spool}=await captureInfo(root,review.captureId);
 if(intent.procedure!==review.procedure || intent.configurationDigest!==loaded.configurationDigest || review.configurationDigest!==loaded.configurationDigest || response.sha256!==review.captureSha256)throw Error('OBSERVED_CONTRACT_NOT_APPROVED');
 validateReviewedContract(review,response.fields,review.procedure);
 const replay=replayTdsResponse(review.fields,()=>spool.responseChunks(review.captureId),response.received),target=new IsolatedTdsCallsWriter(loaded.config.profile,true);
 for await(const event of replay.read(review.plan,new AbortController().signal))if(event.kind==='row'){
  const row=projectTdsAttempts(decodeWireRow(review.plan,event.values),loaded.catalogs,loaded.config.profile,new Date().toISOString().slice(0,-1)).row;
  if(target.preflight(row)!=='allowed')throw Error('DESTINATION_MAPPING_NOT_APPROVED');
 }
 await mkdir(join(root,'reviews'),{recursive:true,mode:0o700});await privateDirectory(join(root,'reviews'));
 await durableExclusive(join(root,'reviews',`${review.procedure}.json`),review);return review;
}
async function reviewsFor(root:string,loaded:Loaded){
 const reviews:ContractReview[]=[];
 for(const procedure of acquisitionProcedures){
  const review=contractReviewSchema.parse(await privateJson(join(root,'reviews',`${procedure}.json`))),c=await captureInfo(root,review.captureId);
  if(c.intent.configurationDigest!==loaded.configurationDigest || c.intent.procedure!==procedure || review.configurationDigest!==loaded.configurationDigest || review.captureSha256!==c.response.sha256)throw Error('OBSERVED_CONTRACT_NOT_APPROVED');
  validateReviewedContract(review,c.response.fields,procedure);reviews.push(review);
 }return reviews;
}
export const recoveryApprovalSchema=z.object({scope:z.literal('OLOS_CONSUMPTIVE_RECOVERY'),configurationDigest:sha,
 captureIds:z.tuple([z.string().uuid(),z.string().uuid()]),operator:text,reference:text,approvedAt:z.string().datetime(),
 ownershipEvidence:text,retentionCompletenessEvidence:text,recoveryExerciseEvidence:text,externalRunbookReference:text,
 failurePolicy:z.literal('STOP_PRESERVE_EXTERNAL_RECONCILIATION_NO_REMOTE_RETRY')}).strict();
export async function promoteService(root:string,loaded:Loaded,approval:ServiceApproval,input:unknown){
 if(blockingCorporateInspection(dirname(root)))throw Error('INSPECTION_EXTERNAL_RECONCILIATION_REQUIRED');
 const reviews=await reviewsFor(root,loaded),activation=validateActivation(input,loaded.config,loaded.configurationDigest,approval,reviews);
 const recovery=recoveryApprovalSchema.parse(await privateJson(activation.recoveryEvidence));
 if(recovery.configurationDigest!==loaded.configurationDigest || recovery.captureIds.some((id,i)=>id!==reviews[i]!.captureId) || Date.parse(recovery.approvedAt)>Date.now())throw Error('RECOVERY_NOT_ESTABLISHED');
 for(const review of reviews){const ack=await privateJson(join(root,'cases',review.captureId,'delivered.json')) as {captureSha256:string;reviewDigest:string};
  if(ack.captureSha256!==review.captureSha256 || ack.reviewDigest!==digest(review))throw Error('BOOTSTRAP_DELIVERY_NOT_CONFIRMED');}
 await durableExclusive(join(root,'activation.json'),{...activation,recoveryDigest:digest(recovery)});
}

/** No source calls: the only source registered here is replayTdsResponse, sealed and digest checked. */
export async function deliverServiceCapture(root:string,loaded:Loaded,intent:Intent,review:ContractReview,pools:RuntimePools,signal:AbortSignal){
 const {spool:capture,response}=await captureInfo(root,intent.attemptId);
 if(intent.configurationDigest!==loaded.configurationDigest)throw Error('SERVICE_CONFIGURATION_CHANGED');
 validateReviewedContract(review,response.fields,intent.procedure);
 if(intent.contractReviewDigest && intent.contractReviewDigest!==digest(review))throw Error('OBSERVED_CONTRACT_NOT_APPROVED');
 const plan=intent.plan?structuredClone(intent.plan) as ContractReview['plan']:planForCapture(review,new Date(intent.createdAt));
 if(canonical(plan)!==canonical(planForCapture(review,new Date(intent.createdAt))))throw Error('OBSERVED_CONTRACT_NOT_APPROVED');
 const replay=replayTdsResponse(review.fields,()=>capture.responseChunks(intent.attemptId),response.received);
 const binding=createApprovedCaptureReplayBinding(plan,review.fields,loaded.config.profile,loaded.catalogs,replay);
 const dir=join(root,'cases',intent.attemptId),spool=await LocalSpool.open(join(dir,'import'),spoolLimits());
 const pipeline=createAttemptsPipeline(pools.ingest,pools.project,pools.receiver,spool,spoolLimits(),binding);
 const started=join(dir,'import-started.json'),done=join(dir,'delivered.json');
 if(await serviceExists(done)){
  const saved=await privateJson(done) as {received:number;persisted:number;duplicates:number;rejected:number;confirmedAt:string;captureSha256:string;reviewDigest:string;batchId:string};
  if(saved.captureSha256!==response.sha256 || saved.reviewDigest!==digest(review) || !saved.batchId)throw Error('LOCAL_COMMIT_REQUIRES_REVIEW');
  return saved;
 }
 let counts;
 if(await serviceExists(started)){
  // Reconcile the existing batch/receipt/outbox; never silently create a second ingestion run.
  const existing=await readdir(join(dir,'import','batches'));
  counts=existing.length || await spool.active()?await pipeline.recoverLocal(signal):await pipeline.cycle(signal);
 }else{
  await durableExclusive(started,{attemptId:intent.attemptId,reviewDigest:digest(review),captureSha256:response.sha256});
  counts=await pipeline.cycle(signal);
 }
 const batches=await readdir(join(dir,'import','batches'));
 if(batches.length!==1)throw Error('LOCAL_COMMIT_REQUIRES_REVIEW');
 const manifest=await privateJson(join(dir,'import','batches',batches[0]!,'manifest.json')) as SpoolManifest;
 if(!manifest.sealed || manifest.state!=='committed' || canonical(manifest.identity.plan)!==canonical(plan))throw Error('LOCAL_COMMIT_REQUIRES_REVIEW');
 // Coordinator's idempotent receipt path handles COMMIT_UNKNOWN. Checkpoint waits for delivery ACKs.
 const writer=new PostgresLargeBatchPersistence(pools.ingest,spoolLimits(),300000,{sourceNamespace:binding.sourceNamespace,destinationId:binding.destinationId,mappingVersion:binding.mappingVersion},true);
 await writer.confirmDeliveryCheckpoint(manifest);
 const receipt=(await pools.ingest.query('SELECT received_count,persisted_count,duplicate_count,rejected_count FROM olos_export.batches WHERE id=$1 AND state=$2',[manifest.identity.batchId,'committed'])).rows[0];
 if(!receipt)throw Error('LOCAL_COMMIT_REQUIRES_REVIEW');
 counts={received:receipt.received_count as number,persisted:receipt.persisted_count as number,duplicates:receipt.duplicate_count as number,rejected:receipt.rejected_count as number};
 const result={...counts,confirmedAt:new Date().toISOString(),captureSha256:response.sha256,reviewDigest:digest(review),batchId:manifest.identity.batchId};
 await durableExclusive(done,result);return result;
}
export async function finishServiceCase(base:string,root:string,intent:Intent){
 const pending=await pendingServiceCase(base,root);if(!pending || pending.attemptId!==intent.attemptId)throw Error('RECOVERY_REQUIRED');
 await privateJson(join(root,'cases',intent.attemptId,'delivered.json'));
 // Archive the original pending exclusively; never erase acquisition evidence.
 const to=join(root,'cases',intent.attemptId,'remote-pending.completed.json');if(await serviceExists(to))throw Error('RECOVERY_REQUIRED');
 await rename(join(base,'remote-pending.json'),to);await syncDir(base);await syncDir(dirname(to));
}

export const inspectionDecisionSchema=z.object({version:z.literal('olos-inspection-reconciliation/v1'),recordId:z.string().uuid(),
 caseId:z.string().uuid(),operator:text,reference:text,issuedAt:z.string().datetime(),
 decision:z.enum(['PRESERVE_PENDING_EXTERNAL_RECONCILIATION','ADMINISTRATIVE_CLOSURE','TECHNICAL_CLEARANCE']),
 remoteOutcome:z.enum(['INDETERMINATE','REMOTELY_ESTABLISHED']),externalAuditFile:z.string().startsWith('/').optional()}).strict();
export async function reconcileServiceInspection(base:string,input:unknown){
 const decision=inspectionDecisionSchema.parse(input);
 if(Date.parse(decision.issuedAt)>Date.now())throw Error('RECONCILIATION_REJECTED');
 const marker=await privateJson(join(base,'log-inspection-active.json')) as {attemptId:string;workDir:string};
 if(marker.attemptId!==decision.caseId || marker.workDir!==join(base,'log-inspections',decision.caseId))throw Error('RECONCILIATION_REJECTED');
 const source={intent:await privateJson(join(marker.workDir,'intent.json')),pending:await privateJson(join(marker.workDir,'pending.json')),result:await privateJson(join(marker.workDir,'result.json'))};
 let externalEvidence:unknown=null;
 if(decision.externalAuditFile)externalEvidence=await privateJson(decision.externalAuditFile);
 if(decision.decision==='TECHNICAL_CLEARANCE'){
  const audit=z.object({kind:z.literal('olos-remote-audit/v1'),caseId:z.string().uuid(),markerDigest:sha,operator:text,reference:text,
   conclusion:z.literal('NO_UNRESOLVED_INTERFERENCE'),rpcAuditEvidence:text,consumptionImpactEvidence:text,issuedAt:z.string().datetime()}).strict().parse(externalEvidence);
  if(decision.remoteOutcome!=='REMOTELY_ESTABLISHED' || audit.caseId!==decision.caseId || audit.markerDigest!==digest(marker) || Date.parse(audit.issuedAt)>Date.now())throw Error('RECONCILIATION_REJECTED');
 }else if(decision.remoteOutcome!=='INDETERMINATE')throw Error('RECONCILIATION_REJECTED');
 const records=join(base,'inspection-reconciliations');await mkdir(records,{recursive:true,mode:0o700});await privateDirectory(records);
 const record={...decision,markerDigest:digest(marker),sourceCaseDigest:digest(source),externalEvidence,externalEvidenceDigest:externalEvidence?digest(externalEvidence):null,
  consumptiveRelease:decision.decision==='TECHNICAL_CLEARANCE'};
 await durableExclusive(join(records,`${decision.recordId}.json`),record);
 if(record.consumptiveRelease)await durableExclusive(join(base,'log-inspection-clearance.json'),{kind:'olos-inspection-clearance/v1',
  caseId:decision.caseId,markerDigest:digest(marker),recordPath:join(records,`${decision.recordId}.json`),recordDigest:digest(record)});
 return {recordId:decision.recordId,decision:decision.decision,remoteOutcome:decision.remoteOutcome,consumptiveRelease:record.consumptiveRelease};
}

export interface ServiceOptions{base:string;root:string;configFile:string;intervalMs:number;}
async function serviceAuthorization(options:ServiceOptions){
 const loaded=await loadServiceConfiguration(options.configFile);
 const approval=validateServiceApproval(await privateJson(loaded.config.approvalFile),loaded.config,loaded.configurationDigest,
  await serviceExists((await privateJson(loaded.config.approvalFile) as ServiceApproval).revocationFile));
 return {loaded,approval,guard:serviceGuard(options.configFile,loaded,approval)};
}
export async function operationalServiceStatus(options:ServiceOptions){
 const {base,root}=options,loaded=await loadServiceConfiguration(options.configFile);
 const result:{state:'BOOTSTRAP'|'ACTIVE'|'RECOVERY_REQUIRED';configurationDigest:string;[key:string]:unknown}={state:'BOOTSTRAP',configurationDigest:loaded.configurationDigest,
  workerId:loaded.config.workerId,networkAccessed:false,postgresState:'NOT_PROBED',tls:null,lastProcedure:null,lastCycleStartedAt:null,lastCycleCompletedAt:null,
  lastImportConfirmedAt:null,imported:null,duplicates:null,pendingBatch:null,lastError:null,nextExecutionAt:null,schedulerActive:false};
 try{const pin=await loadOlosPinApproval(loaded.config.pinApprovalFile);result.tls={valid:true,expiresAt:pin.expiresAt};}catch(e){result.tls={valid:false,code:inspectionErrorCode(e)};}
 const activationPath=join(root,'activation.json');if(await serviceExists(activationPath)){
  try{const {approval}=await serviceAuthorization(options);const reviews=await reviewsFor(root,loaded);const raw=await privateJson(activationPath) as Record<string,unknown>;const {recoveryDigest,...activation}=raw;
   const approved=validateActivation(activation,loaded.config,loaded.configurationDigest,approval,reviews);
   if(digest(recoveryApprovalSchema.parse(await privateJson(approved.recoveryEvidence)))!==recoveryDigest)throw Error('RECOVERY_NOT_ESTABLISHED');
   result.state='ACTIVE';
  }catch{result.state='RECOVERY_REQUIRED';result.lastError='ACTIVATION_REVALIDATION_FAILED';}
 }
 if(await serviceExists(join(base,'remote-pending.json'))){result.state='RECOVERY_REQUIRED';const p=await privateJson(join(base,'remote-pending.json')) as {attemptId?:string};result.pendingBatch=p.attemptId??null;}
 if(blockingCorporateInspection(base)){result.state='RECOVERY_REQUIRED';result.lastError='INSPECTION_EXTERNAL_RECONCILIATION_REQUIRED';}
 if(await serviceExists(join(root,'cycles'))){const names=(await readdir(join(root,'cycles'))).filter(n=>/^\d+-[a-f0-9-]+\.json$/.test(n)).sort();
  if(names.length){const last=await privateJson(join(root,'cycles',names.at(-1)!)) as Record<string,unknown>;
   for(const field of ['lastProcedure','lastCycleStartedAt','lastCycleCompletedAt','lastImportConfirmedAt','imported','duplicates','lastError'])result[field]=last[field]??null;}}
 if(result.state==='ACTIVE' && (result.tls as {valid:boolean}|null)?.valid===false)result.state='RECOVERY_REQUIRED';
 return result;
}
async function assertActive(options:ServiceOptions,loaded:Loaded,approval:ServiceApproval){
 const raw=await privateJson(join(options.root,'activation.json')) as Record<string,unknown>,{recoveryDigest,...activation}=raw;
 const reviews=await reviewsFor(options.root,loaded),approved=validateActivation(activation,loaded.config,loaded.configurationDigest,approval,reviews);
 if(digest(recoveryApprovalSchema.parse(await privateJson(approved.recoveryEvidence)))!==recoveryDigest)throw Error('RECOVERY_NOT_ESTABLISHED');
 return reviews;
}
async function withLocalLock<T>(options:ServiceOptions,operation:(pools:RuntimePools,signal:AbortSignal)=>Promise<T>,allowLocalRecovery=false){
 const settings=await localSettings(),loaded=await loadServiceConfiguration(options.configFile);
 if(canonical(settings.postgres)!==canonical(loaded.config.postgres))throw Error('POSTGRES_DESTINATION_CHANGED');
 const abort=new AbortController(),cancel=()=>abort.abort(),pools=createLocalRuntimePools(settings.passwords,cancel,settings.postgres),probe=new AttemptsService(pools);
 let lock:PoolClient|undefined;process.once('SIGINT',cancel);process.once('SIGTERM',cancel);
 try{lock=await pools.ingest.connect();lock.on('error',cancel);if(!(await lock.query('SELECT pg_try_advisory_lock(740219,1) AS acquired')).rows[0].acquired)throw Error('INSTANCE_BUSY');
  await probe.probe();if(!allowLocalRecovery && probe.snapshot().recoveryPending)throw Error('RECOVERY_REQUIRED');return await operation(pools,abort.signal);
 }finally{process.removeListener('SIGINT',cancel);process.removeListener('SIGTERM',cancel);if(lock){await lock.query('SELECT pg_advisory_unlock(740219,1)').catch(()=>{});lock.release(true);}await probe.stop();}
}
export async function serviceCycle(options:ServiceOptions,pools:RuntimePools,signal:AbortSignal){
 const {loaded,approval,guard}=await serviceAuthorization(options);guard();if(blockingCorporateInspection(options.base))throw Error('INSPECTION_EXTERNAL_RECONCILIATION_REQUIRED');const reviews=await assertActive(options,loaded,approval);
 const activation=await privateJson(join(options.root,'activation.json')) as {recoveryEvidence:string;recoveryDigest:string};
 const activeGuard=()=>{guard();if(digest(privateServiceJsonSync(join(options.root,'activation.json')))!==digest(activation) || digest(privateServiceJsonSync(activation.recoveryEvidence))!==activation.recoveryDigest ||
  reviews.some(r=>digest(privateServiceJsonSync(join(options.root,'reviews',`${r.procedure}.json`)))!==digest(r)))throw Error('ACTIVATION_REQUIREMENTS_MISSING');};
 activeGuard();
 let acquired=false;
 let pending=await pendingServiceCase(options.base,options.root);
 if(pending && await serviceExists(join(options.root,'cases',pending.attemptId,'not-dispatched.json'))){
  const dir=join(options.root,'cases',pending.attemptId),proof=await privateJson(join(dir,'not-dispatched.json')) as {attemptId:string;diagnosticDigest:string;kind:string};
  const diagnostic=await privateJson(join(dir,'diagnostic.json')) as {rpcDispatchState:string;observedStages:string[]};
  if(proof.kind!=='olos-local-not-dispatched/v1' || proof.attemptId!==pending.attemptId || proof.diagnosticDigest!==digest(diagnostic) || diagnostic.rpcDispatchState!=='NOT_OBSERVED' || diagnostic.observedStages.includes('RESPONSE_CAPTURE_STARTED'))throw Error('RECOVERY_REQUIRED');
  await durableExclusive(join(dir,'not-dispatched-closed.json'),proof);
  await rename(join(options.base,'remote-pending.json'),join(dir,'remote-pending.not-dispatched.json'));await syncDir(options.base);await syncDir(dir);
  return {received:0,persisted:0,duplicates:0,rejected:0,confirmedAt:null,acquisitionPerformed:false};
 }
 if(!pending){
  if((await pools.ingest.query("SELECT 1 FROM olos_export.runs WHERE source_table='AttemptsRawData' AND state IN ('read_intent','persisting','needs_reconciliation') LIMIT 1")).rowCount ||
   (await pools.project.query("SELECT 1 FROM olos_export.projection_outbox WHERE state<>'confirmed' LIMIT 1")).rowCount)throw Error('RECOVERY_REQUIRED');
  const files=(await readdir(options.root)).filter(f=>/^attempt-[a-f0-9-]+\.json$/.test(f));let last:Intent|undefined;
  for(const file of files){const i=captureIntentSchema.parse(await privateJson(join(options.root,file)));
   if(await serviceExists(join(options.root,'cases',i.attemptId,'not-dispatched-closed.json')))continue;
   if(!await serviceExists(join(options.root,'cases',i.attemptId,'delivered.json')))throw Error('RECOVERY_REQUIRED');
   if(!last || i.createdAt>last.createdAt)last=i;}
  const procedure=last?.procedure==='ReadDataCurrentDay'?'ReadDataPreviousDays':'ReadDataCurrentDay';
  const selected=reviews.find(r=>r.procedure===procedure)!;
  await new PostgresLargeBatchPersistence(pools.ingest,spoolLimits(),300000).preflight(planForCapture(selected));
  pending=await observeServiceCapture(options.base,options.root,loaded,approval,activeGuard,procedure,signal,selected);acquired=true;
 }
 // A complete pending response is always replayed locally, before any new RPC. Partial stays blocked.
 const review=reviews.find(r=>r.procedure===pending!.procedure)!;
 const result=await deliverServiceCapture(options.root,loaded,pending,review,pools,signal);
 await finishServiceCase(options.base,options.root,pending);
 return {...result,acquisitionPerformed:acquired};
}
export async function serviceCommand(action:string,options:ServiceOptions,flags:Record<string,string>){
 if(action==='service-status')return operationalServiceStatus(options);
 await directories(options.base,options.root);
 if(action==='reconcile-inspection')return reconcileServiceInspection(options.base,await privateJson(flags['--decision-file']!));
 const {loaded,approval,guard}=await serviceAuthorization(options);guard();
 if(action==='bootstrap'){
  const procedure=z.enum(acquisitionProcedures).parse(flags['--procedure']);
  if(await serviceExists(join(options.root,'activation.json')))throw Error('ALREADY_ACTIVE');
  return withLocalLock(options,async(_pools,signal)=>{const i=await observeServiceCapture(options.base,options.root,loaded,approval,guard,procedure,signal);
   return {state:'BOOTSTRAP',attemptId:i.attemptId,procedure,quarantined:true,imported:0};});
 }
 if(action==='prepare-contract')return prepareServiceContract(options.root,loaded,flags['--capture-id']!);
 if(action==='approve-contract'){
  const review=await reviewServiceContract(options.root,loaded,await privateJson(flags['--review-file']!));
  return {state:'BOOTSTRAP',procedure:review.procedure,contractDigest:digest(review),imported:0};
 }
 if(action==='import-capture')return withLocalLock(options,async(pools,signal)=>{
  const pending=await pendingServiceCase(options.base,options.root);if(!pending)throw Error('NO_PENDING_CAPTURE');
  const review=contractReviewSchema.parse(await privateJson(join(options.root,'reviews',`${pending.procedure}.json`)));
  const result=await deliverServiceCapture(options.root,loaded,pending,review,pools,signal);await finishServiceCase(options.base,options.root,pending);return {...result,state:'BOOTSTRAP'};
 },true);
 if(action==='activate'){
  if(await serviceExists(join(options.base,'remote-pending.json')))throw Error('RECOVERY_REQUIRED');
  if(blockingCorporateInspection(options.base))throw Error('INSPECTION_EXTERNAL_RECONCILIATION_REQUIRED');
  await promoteService(options.root,loaded,approval,await privateJson(flags['--activation-file']!));return {state:'ACTIVE',schedulerStarted:false};
 }
 if(action==='start'){
  if(blockingCorporateInspection(options.base))throw Error('INSPECTION_EXTERNAL_RECONCILIATION_REQUIRED');
  await assertActive(options,loaded,approval);
  const settings=await localSettings();if(canonical(settings.postgres)!==canonical(loaded.config.postgres))throw Error('POSTGRES_DESTINATION_CHANGED');
  const pools=createLocalRuntimePools(settings.passwords,()=>{},settings.postgres);
  const previousStatus=await operationalServiceStatus(options);
  const status:Record<string,unknown>={...previousStatus,nextExecutionAt:null,schedulerActive:false};
  await mkdir(join(options.root,'cycles'),{recursive:true,mode:0o700});await privateDirectory(join(options.root,'cycles'));
  let runtime:AttemptsService;
  runtime=new AttemptsService(pools,120000,async signal=>{
   status.lastCycleStartedAt=new Date().toISOString();status.nextExecutionAt=null;
   try{
    try{const pin=await loadOlosPinApproval(loaded.config.pinApprovalFile);status.tls={valid:true,expiresAt:pin.expiresAt};}catch(error){status.tls={valid:false,code:inspectionErrorCode(error)};} // Local replay remains possible without a live source/pin.
    const counts=await serviceCycle(options,pools,signal);
    const completed=new Date().toISOString();Object.assign(status,{state:'ACTIVE',lastCycleCompletedAt:completed,lastImportConfirmedAt:counts.confirmedAt??status.lastImportConfirmedAt,imported:counts.received,duplicates:counts.duplicates,pendingBatch:null,lastError:null,nextExecutionAt:new Date(Date.now()+120000).toISOString()});
    const intents=(await readdir(options.root)).filter(f=>/^attempt-.*\.json$/.test(f));let latest:Intent|undefined;
    for(const name of intents){const i=captureIntentSchema.parse(await privateJson(join(options.root,name)));if(!await serviceExists(join(options.root,'cases',i.attemptId,'delivered.json')))continue;if(!latest || i.createdAt>latest.createdAt)latest=i;}
    status.lastProcedure=latest?.procedure??null;
    await durableExclusive(join(options.root,'cycles',`${Date.now()}-${randomUUID()}.json`),status);return counts;
   }catch(error){
    try{const pending=await pendingServiceCase(options.base,options.root);status.pendingBatch=pending?.attemptId??null;status.lastAttemptedProcedure=pending?.procedure??null;}catch{/* keep unavailable status null */}
    Object.assign(status,{state:'RECOVERY_REQUIRED',lastError:inspectionErrorCode(error),nextExecutionAt:null,schedulerActive:false});
    await durableExclusive(join(options.root,'cycles',`${Date.now()}-${randomUUID()}.json`),status).catch(()=>{});throw error;}
  },'corporate-service',()=>({...status,state:status.state==='RECOVERY_REQUIRED' || (status.tls as {valid:boolean}|null)?.valid===false || !runtime?.snapshotBaseArmed()?'RECOVERY_REQUIRED':'ACTIVE',postgresState:runtime?.snapshotDatabaseState()??'unknown',countsScope:'confirmed_capture_records_and_ingestion_duplicates',businessRowsInserted:null,nextExecutionAt:runtime?.snapshotNextExecutionAt()??null,schedulerActive:runtime?.snapshotBaseArmed()??false}));
  status.nextExecutionAt=new Date(Date.now()+120000).toISOString();
  await serveWorker({...settings.config,SYNC_INTERVAL_MS:120000},[{name:'postgres',configured:true,probe:()=>runtime.probe(),close:async()=>{}}],runtime);
  return;
 }
 throw Error('SERVICE_COMMAND_REJECTED');
}
