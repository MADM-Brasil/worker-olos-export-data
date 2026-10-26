import {localReconciliationApprovalSchema,reconcileLocalIngestion} from '../src/persistence/local-ingestion-reconciliation.js';
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
import {agentMap,parseAgentCatalog} from '../src/offline/agents.js';
import {catalog,recordsFromJson,parseLossless,type Catalogs} from '../src/offline/model.js';
import {spoolLimits} from '../src/large-batch/config.js';
import {reportedSignature} from '../src/olos/procedure-signatures.js';
import {hasUnresolvedCorporateInspection,prepareCorporateAttemptsEndpoint,authorizeCorporateAttemptsOnce} from '../src/olos/tds/attempts-access.js';
import {IsolatedTdsSource,replayTdsResponse} from '../src/olos/tds/source.js';
import {parserLimits} from '../src/olos/tds/raw-parser.js';
import {InspectionDiagnostics,inspectionErrorCode,reportInspectionFailure} from '../src/olos/tds/inspection-diagnostics.js';
import {createApprovedCaptureReplayBinding} from '../src/integration/attempts/corporate.js';
import {decodeWireRow} from '../src/large-batch/wire.js';
import {projectTdsAttempts} from '../src/offline/tds-projection.js';
import {IsolatedTdsCallsWriter} from '../src/offline/tds-writer.js';
import {createAttemptsPipeline} from '../src/integration/attempts/pipeline.js';
import {PostgresLargeBatchPersistence,manifestDigest} from '../src/persistence/large-batch.js';
import type {SpoolManifest} from '../src/large-batch/types.js';
import {AttemptsService,type RuntimePools} from '../src/integration/attempts/service.js';
import {createLocalRuntimePools} from '../src/integration/attempts/pools.js';
import {serveWorker} from '../src/main.js';
import {queuedCaptureSchema,handoffCapture,captureAdmission,queueLimitsSchema,FairCaptureDelivery,type QueuedCapture} from './service-capture-queue.js';
import {annotateFailure,failureDetails,logWorkerFailure,type FailureContext} from '../src/diagnostics/worker-failure.js';
import {isRecoverableClaimFailure} from '../src/projection/dispatcher.js';

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
 const agents=config.agentFile?agentMap(parseAgentCatalog(JSON.stringify(await privateJson(config.agentFile)))):new Map<string,string>();
 const catalogs:Catalogs={campaigns,dispositions,agents};
 return {config,catalogs,configurationDigest:digest({config,campaigns:[...campaigns],dispositions:[...dispositions],...(config.agentFile?{agents:[...agents]}:{})})};
}
type Loaded=Awaited<ReturnType<typeof loadServiceConfiguration>>;
export function serviceGuard(configFile:string,loaded:Loaded,approval:ServiceApproval){
 const {config,configurationDigest}=loaded;
 return ()=>{
  try{
  if(digest(privateServiceJsonSync(configFile))!==digest(config))throw Error('SERVICE_CONFIGURATION_CHANGED');
  const campaigns=catalog(recordsFromJson(parseLossless(JSON.stringify(privateServiceJsonSync(config.campaignFile))),true),'CampaignId','Description');
  const dispositions=catalog(recordsFromJson(parseLossless(JSON.stringify(privateServiceJsonSync(config.dispositionFile))),true),'DispositionId','Description');
  const agents=config.agentFile?agentMap(parseAgentCatalog(JSON.stringify(privateServiceJsonSync(config.agentFile)))):new Map<string,string>();
  if(digest({config,campaigns:[...campaigns],dispositions:[...dispositions],...(config.agentFile?{agents:[...agents]}:{})})!==configurationDigest)throw Error('SERVICE_CONFIGURATION_CHANGED');
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
/** Migration, when supplied, runs under the caller's instance lock and never calls Olos. */
export async function promoteService(root:string,loaded:Loaded,approval:ServiceApproval,input:unknown,
 migration?:{options:ServiceOptions;pools:RuntimePools;signal:AbortSignal;guard:()=>void}){
 const reviews=await reviewsFor(root,loaded),activation=validateActivation(input,loaded.config,loaded.configurationDigest,approval,reviews);
 const recovery=recoveryApprovalSchema.parse(await privateJson(activation.recoveryEvidence));
 if(recovery.configurationDigest!==loaded.configurationDigest || recovery.captureIds.some((id,i)=>id!==reviews[i]!.captureId) || Date.parse(recovery.approvedAt)>Date.now())throw Error('RECOVERY_NOT_ESTABLISHED');
 if(migration){
  if(migration.options.root!==root)throw Error('CAPTURE_QUEUE_REQUIRES_REVIEW');
  const queue=createServiceCaptureQueue(migration.options,migration.pools,loaded,migration.guard);
  await queue.initialize(migration.signal,reviews);
  if(digest(await reviewsFor(root,loaded))!==digest(reviews) || digest(await privateJson(activation.recoveryEvidence))!==digest(recovery))throw Error('ACTIVATION_REQUIREMENTS_MISSING');
  migration.guard();if(migration.signal.aborted)throw Error('STOPPED');
 }else{
  // Retain the serial API's stricter requirement; only validated V2 migration accepts backlog.
  for(const review of reviews){const ack=await privateJson(join(root,'cases',review.captureId,'delivered.json')) as {captureSha256:string;reviewDigest:string};
   if(ack.captureSha256!==review.captureSha256 || ack.reviewDigest!==digest(review))throw Error('BOOTSTRAP_DELIVERY_NOT_CONFIRMED');}
 }
 await durableExclusive(join(root,'activation.json'),{...activation,recoveryDigest:digest(recovery)});
}

/** No source calls: the only source registered here is replayTdsResponse, sealed and digest checked. */
async function captureDelivery(root:string,loaded:Loaded,intent:Intent,review:ContractReview,pools:RuntimePools,independent=false){
 const {spool:capture,response}=await captureInfo(root,intent.attemptId);
 if(intent.configurationDigest!==loaded.configurationDigest)throw Error('SERVICE_CONFIGURATION_CHANGED');
 validateReviewedContract(review,response.fields,intent.procedure);
 if(intent.contractReviewDigest && intent.contractReviewDigest!==digest(review))throw Error('OBSERVED_CONTRACT_NOT_APPROVED');
 const plan=intent.plan?structuredClone(intent.plan) as ContractReview['plan']:planForCapture(review,new Date(intent.createdAt));
 if(canonical(plan)!==canonical(planForCapture(review,new Date(intent.createdAt))))throw Error('OBSERVED_CONTRACT_NOT_APPROVED');
 const replay=replayTdsResponse(review.fields,()=>capture.responseChunks(intent.attemptId),response.received);
 const binding=createApprovedCaptureReplayBinding(plan,review.fields,loaded.config.profile,loaded.catalogs,replay);
 const dir=join(root,'cases',intent.attemptId),spool=await LocalSpool.open(join(dir,'import'),spoolLimits());
 const pipeline=createAttemptsPipeline(pools.ingest,pools.project,pools.receiver,spool,spoolLimits(),binding,independent);
 const started=join(dir,'import-started.json'),done=join(dir,'delivered.json');
 return {response,plan,binding,dir,spool,pipeline,started,done};
}
export async function deliverServiceCapture(root:string,loaded:Loaded,intent:Intent,review:ContractReview,pools:RuntimePools,signal:AbortSignal){
 if(await serviceExists(join(root,'queue-mode.json')))throw Error('CAPTURE_QUEUE_RECOVERY_REQUIRED');
 const {response,plan,binding,dir,spool,pipeline,started,done}=await captureDelivery(root,loaded,intent,review,pools);
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

/** Historical evidence remains private and immutable; diagnostic failures cannot gate acquisition. */
async function historicalInspectionStatus(base:string){
 const path=join(base,'log-inspection-active.json');
 try{
  if(!await serviceExists(path))return null;
  const marker=await privateJson(path) as {attemptId?:unknown};
  const caseId=typeof marker?.attemptId==='string' && z.string().uuid().safeParse(marker.attemptId).success?marker.attemptId:null;
  const reviewRequired=hasUnresolvedCorporateInspection(base);
  return {caseId,procedure:'LogReadData',state:reviewRequired?'REMOTE_OUTCOME_UNKNOWN':'REMOTELY_ESTABLISHED',reviewRequired,blocksAcquisition:false};
 }catch{
  return {caseId:null,procedure:'LogReadData',state:'REMOTE_OUTCOME_UNKNOWN',reviewRequired:true,blocksAcquisition:false,diagnosticError:'HISTORICAL_INSPECTION_UNREADABLE'};
 }
}

export interface ServiceOptions{base:string;root:string;configFile:string;intervalMs:number;queueMaxCaptures?:number|undefined;queueMaxBytes?:number|undefined;}
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
 if(await serviceExists(join(root,'queue-mode.json'))){
  let queued=0,delivered=0;
  for(const id of await readdir(join(root,'cases'))){if(!z.string().uuid().safeParse(id).success)continue;
   if(await serviceExists(join(root,'cases',id,'queued.json'))){queuedCaptureSchema.parse(await privateJson(join(root,'cases',id,'queued.json')));queued++;
    if(await serviceExists(join(root,'cases',id,'delivered.json')))delivered++;}}
  result.captureQueue={queued,delivered,pending:queued-delivered};
 }
 result.historicalInspection=await historicalInspectionStatus(base);
 if(await serviceExists(join(root,'cycles'))){const names=(await readdir(join(root,'cycles'))).filter(n=>/^\d+-[a-f0-9-]+\.json$/.test(n)).sort();
  if(names.length){const last=await privateJson(join(root,'cycles',names.at(-1)!)) as Record<string,unknown>;
   for(const field of ['lastProcedure','lastCycleStartedAt','lastCycleCompletedAt','lastImportConfirmedAt','imported','duplicates','lastError'])result[field]=last[field]??null;
   // Old cycle files are immutable; the retired inspection error is no longer an operational failure.
   if(result.lastError==='INSPECTION_EXTERNAL_RECONCILIATION_REQUIRED')result.lastError=null;}}
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
/** Caller holds the existing instance lock. Closure never authorizes or performs acquisition. */
async function closeNotDispatchedServiceCase(options:ServiceOptions,attemptId:string,configurationDigest:string,guard:()=>void,signal:AbortSignal){
 const assertCurrent=()=>{guard();if(signal.aborted)throw Error('ABORTED');};
 assertCurrent();if(!z.string().uuid().safeParse(attemptId).success)throw Error('RECOVERY_REQUIRED');
 const {base,root}=options,dir=join(root,'cases',attemptId),pendingPath=join(base,'remote-pending.json'),archive=join(dir,'remote-pending.not-dispatched.json'),closed=join(dir,'not-dispatched-closed.json');
 const pendingExists=await serviceExists(pendingPath);
 if(!pendingExists && !await serviceExists(archive))throw Error('NO_PENDING_CAPTURE');
 const markerPath=pendingExists?pendingPath:archive;
 const marker=await privateJson(markerPath) as {attemptId?:string;workDir?:string;configurationDigest?:string};
 if(marker.attemptId!==attemptId || marker.workDir!==root || marker.configurationDigest!==configurationDigest)throw Error('RECOVERY_REQUIRED');
 const intentPath=join(root,`attempt-${attemptId}.json`),intent=captureIntentSchema.parse(await privateJson(intentPath));
 if(intent.attemptId!==attemptId || intent.workDir!==root || intent.configurationDigest!==configurationDigest)throw Error('RECOVERY_REQUIRED');
 await privateDirectory(dir);
 const proofPath=join(dir,'not-dispatched.json'),diagnosticPath=join(dir,'diagnostic.json');
 const proof=await privateJson(proofPath) as {kind?:string;attemptId?:string;diagnosticDigest?:string;evidence?:string};
 const diagnostic=await privateJson(diagnosticPath) as {attemptId?:string;rpcDispatchState?:string;remoteExecutionOutcome?:string;observedStages?:unknown};
 if(proof.kind!=='olos-local-not-dispatched/v1' || proof.attemptId!==attemptId || proof.evidence!=='CONTROLLED_RPC_HANDOFF_NOT_REACHED_AFTER_TRANSPORT_CLOSED' ||
  proof.diagnosticDigest!==digest(diagnostic) || diagnostic.attemptId!==attemptId || diagnostic.rpcDispatchState!=='NOT_OBSERVED' || diagnostic.remoteExecutionOutcome!=='NOT_OBSERVED' ||
  !Array.isArray(diagnostic.observedStages) || diagnostic.observedStages.some(stage=>typeof stage!=='string' || ['RPC_HANDOFF_STARTED','RPC_HANDOFF_RETURNED','RESPONSE_CAPTURE_STARTED','RESPONSE_VALIDATION_STARTED','RESPONSE_VALIDATED'].includes(stage)))throw Error('RECOVERY_REQUIRED');
 const captureDir=join(dir,'capture','batches',attemptId);await privateDirectory(captureDir);
 if(await serviceExists(join(captureDir,'response.ready')))throw Error('RECOVERY_REQUIRED');
 if(pendingExists && await serviceExists(archive))throw Error('RECOVERY_REQUIRED');
 const assertEvidence=()=>{
  assertCurrent();
  for(const [path,value] of [[markerPath,marker],[intentPath,intent],[proofPath,proof],[diagnosticPath,diagnostic]] as const)
   if(digest(privateServiceJsonSync(path))!==digest(value))throw Error('RECOVERY_REQUIRED');
  if(pendingExists && existsSync(archive))throw Error('RECOVERY_REQUIRED'); // Never overwrite archived evidence.
  if(existsSync(closed) && digest(privateServiceJsonSync(closed))!==digest(proof))throw Error('RECOVERY_REQUIRED');
  if(existsSync(join(captureDir,'response.ready')))throw Error('RECOVERY_REQUIRED');
  const fd=openSync(join(captureDir,'response.partial'),constants.O_RDONLY|constants.O_NOFOLLOW);
  try{const st=fstatSync(fd);if(!st.isFile() || st.uid!==process.getuid?.() || st.mode&0o077 || st.size!==0)throw Error('RECOVERY_REQUIRED');}finally{closeSync(fd);}
  const descriptor=join(captureDir,'response.json');if(existsSync(descriptor)){
   const response=privateServiceJsonSync(descriptor) as {attemptId?:string;state?:string;bytes?:number};
   if(response.attemptId!==attemptId || response.state!=='incomplete' || response.bytes!==0)throw Error('RECOVERY_REQUIRED');
  }
 };
 assertEvidence();
 if(await serviceExists(closed)){
  if(digest(await privateJson(closed))!==digest(proof))throw Error('RECOVERY_REQUIRED');
 }else{
  if(!pendingExists)throw Error('RECOVERY_REQUIRED');
  assertEvidence();await durableExclusive(closed,proof);
 }
 assertEvidence();
 if(pendingExists)await rename(pendingPath,archive);
 await syncDir(base);await syncDir(dir);
 return {attemptId,alreadyClosed:!pendingExists};
}

export async function serviceCycle(options:ServiceOptions,pools:RuntimePools,signal:AbortSignal){
 if(await serviceExists(join(options.root,'queue-mode.json')))throw Error('CAPTURE_QUEUE_RECOVERY_REQUIRED');
 const {loaded,approval,guard}=await serviceAuthorization(options);guard();const reviews=await assertActive(options,loaded,approval);
 const activation=await privateJson(join(options.root,'activation.json')) as {recoveryEvidence:string;recoveryDigest:string};
 const activeGuard=()=>{guard();if(digest(privateServiceJsonSync(join(options.root,'activation.json')))!==digest(activation) || digest(privateServiceJsonSync(activation.recoveryEvidence))!==activation.recoveryDigest ||
  reviews.some(r=>digest(privateServiceJsonSync(join(options.root,'reviews',`${r.procedure}.json`)))!==digest(r)))throw Error('ACTIVATION_REQUIREMENTS_MISSING');};
 activeGuard();
 let acquired=false;
 let pending=await pendingServiceCase(options.base,options.root);
 if(pending && await serviceExists(join(options.root,'cases',pending.attemptId,'not-dispatched.json'))){
  await closeNotDispatchedServiceCase(options,pending.attemptId,loaded.configurationDigest,activeGuard,signal);
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

/** One instance-lock owner, one serial acquisition scheduler and one local consumer. */
export function createServiceCaptureQueue(options:ServiceOptions,pools:RuntimePools,loaded:Loaded,guard:()=>void,capture=observeServiceCapture){
 const {root,base}=options,limits=queueLimitsSchema.parse({maxCaptures:options.queueMaxCaptures,maxBytes:options.queueMaxBytes});
 type Item={entry:QueuedCapture;intent:Intent;review:ContractReview;context?:Awaited<ReturnType<typeof captureDelivery>>;manifest?:SpoolManifest;done:boolean;retryAt?:number;deferrals?:number};
 const items:Item[]=[],fair=new FairCaptureDelivery();
 let initialized=false,acquiring=false,delivering=false,failed=false,last:Intent|undefined,ordinal=0;
 let lastImportConfirmedAt:string|null=null,lastError:string|null=null;
 let queueFailure:ReturnType<typeof logWorkerFailure>|null=null,lastDeferred:ReturnType<typeof logWorkerFailure>|null=null;
 let admission:Awaited<ReturnType<typeof captureAdmission>>|null=null;
 const check=(signal:AbortSignal)=>{if(failed)throw Error('RECOVERY_REQUIRED');guard();if(signal.aborted)throw Error('STOPPED');};
 function fail(error:unknown,context:FailureContext,signal:AbortSignal){
  annotateFailure(error,context);
  if(!failed){lastError=queueError(error);if(!signal.aborted)queueFailure=logWorkerFailure('worker_flow_failed',error,context);}
  failed=true;return error;
 }
 async function reviewed(intent:Intent){
  const review=contractReviewSchema.parse(await privateJson(join(root,'reviews',`${intent.procedure}.json`)));
  if(review.configurationDigest!==loaded.configurationDigest)throw Error('OBSERVED_CONTRACT_NOT_APPROVED');
  return review;
 }
 async function enqueue(intent:Intent,review:ContractReview){
  const {response}=await captureInfo(root,intent.attemptId),dir=join(root,'cases',intent.attemptId);
  const metadata=await privateJson(join(dir,'capture','batches',intent.attemptId,'response.json')) as {rawMessageComplete?:boolean};
  if(metadata.rawMessageComplete!==true || intent.workDir!==root || intent.configurationDigest!==loaded.configurationDigest)throw Error('CAPTURE_QUEUE_REQUIRES_REVIEW');
  validateReviewedContract(review,response.fields,intent.procedure);
  if(intent.contractReviewDigest && intent.contractReviewDigest!==digest(review))throw Error('OBSERVED_CONTRACT_NOT_APPROVED');
  if(intent.plan && canonical(intent.plan)!==canonical(planForCapture(review,new Date(intent.createdAt))))throw Error('OBSERVED_CONTRACT_NOT_APPROVED');
  const path=join(dir,'queued.json');
  const pendingPath=await serviceExists(path) && !await serviceExists(join(base,'remote-pending.json'))?join(dir,'remote-pending.captured.json'):join(base,'remote-pending.json');
  const existing=await serviceExists(path)?queuedCaptureSchema.parse(await privateJson(path)):null;
  const marker=await privateJson(pendingPath);
  const entry:QueuedCapture={kind:'olos-capture-queue/v1',captureId:intent.attemptId,ordinal:existing?.ordinal??ordinal+1,
   captureSha256:response.sha256,intentDigest:digest(intent),reviewDigest:digest(review),configurationDigest:loaded.configurationDigest,pendingDigest:digest(marker)};
  await handoffCapture(base,root,entry);ordinal=Math.max(ordinal,entry.ordinal);
  items.push({entry,intent,review,done:false});last=intent;return response.received;
 }
 async function context(item:Item){return item.context??=await captureDelivery(root,loaded,item.intent,item.review,pools,true);}
 async function materialize(item:Item,signal:AbortSignal){
  let diagnostic:FailureContext={stage:'MATERIALIZE',captureId:item.intent.attemptId};try{
  const ctx=await context(item),{spool,pipeline,started,dir,plan}=ctx;
  const proof={attemptId:item.intent.attemptId,reviewDigest:item.entry.reviewDigest,captureSha256:item.entry.captureSha256};
  if(await serviceExists(started)){if(digest(await privateJson(started))!==digest(proof))throw Error('LOCAL_COMMIT_REQUIRES_REVIEW');}
  else await durableExclusive(started,proof);
  let batches=await readdir(join(dir,'import','batches'));
  if(await spool.active() || !batches.length)await pipeline.materializeLocal(signal);
  batches=await readdir(join(dir,'import','batches'));
  if(batches.length!==1)throw Error('LOCAL_COMMIT_REQUIRES_REVIEW');
  const m=await privateJson(join(dir,'import','batches',batches[0]!,'manifest.json')) as SpoolManifest;
  diagnostic={...diagnostic,stage:'VALIDATE_BATCH',batchId:m.identity.batchId,streamId:m.identity.streamId};
  if(!m.sealed || m.state!=='committed' || m.rows!==ctx.response.received || m.identity.batchId!==batches[0] || canonical(m.identity.plan)!==canonical(plan))throw Error('LOCAL_COMMIT_REQUIRES_REVIEW');
  if((await pools.ingest.query('SELECT manifest_digest FROM olos_export.large_batch_receipts WHERE batch_id=$1',[m.identity.batchId])).rows[0]?.manifest_digest!==manifestDigest(m))throw Error('LOCAL_COMMIT_REQUIRES_REVIEW');
  // The existing receipt path checks the exact manifest digest; never starts a second ingestion.
  await pipeline.writer.persist(m,{runId:m.identity.runId,streamId:m.identity.streamId,fencingToken:m.identity.fencingToken,signal},()=>spool.records(m));
  await pipeline.writer.validateDeliveryPosition(m);
  item.manifest=m;
  }catch(error){throw annotateFailure(error,diagnostic);}
 }
 return {
  snapshot:()=>({captureQueue:{pending:items.filter(i=>!i.done).length,materialized:items.filter(i=>!i.done && i.manifest).length,limits,admission},deliveryInFlight:delivering,
   lastDurableCaptureId:last?.attemptId??null,lastProcedure:last?.procedure??null,lastImportConfirmedAt,lastError,queueFailure,lastDeferred,
   deferredDeliveries:items.filter(i=>!i.done && (i.retryAt??0)>Date.now()).length}),
  async initialize(signal:AbortSignal,bootstrapReviews:readonly ContractReview[]=[]){
   let diagnostic:FailureContext={flow:'initialization',stage:'INITIALIZE'};
   try{if(initialized)throw Error('CAPTURE_QUEUE_REQUIRES_REVIEW');check(signal);
   const knownBatches:string[]=[],knownRuns:string[]=[],confirmedCaptures=new Set<string>();
   const intents=await Promise.all((await readdir(root)).filter(n=>/^attempt-[a-f0-9-]+\.json$/.test(n)).map(async n=>{
    const intent=captureIntentSchema.parse(await privateJson(join(root,n)));
    if(n!==`attempt-${intent.attemptId}.json` || intent.workDir!==root || intent.configurationDigest!==loaded.configurationDigest)throw Error('CAPTURE_QUEUE_REQUIRES_REVIEW');return intent;
   }));
   const pending=await pendingServiceCase(base,root);
   const positions=new Map<string,number>(),seen=new Set<number>();
   for(const intent of intents){const file=join(root,'cases',intent.attemptId,'queued.json');if(await serviceExists(file)){
    const entry=queuedCaptureSchema.parse(await privateJson(file));
    if(entry.captureId!==intent.attemptId || seen.has(entry.ordinal))throw Error('CAPTURE_QUEUE_REQUIRES_REVIEW');
    positions.set(intent.attemptId,entry.ordinal);seen.add(entry.ordinal);
   }}
   const rank=(intent:Intent)=>positions.get(intent.attemptId)??(intent.attemptId===pending?.attemptId?Number.MAX_SAFE_INTEGER:0);
   intents.sort((a,b)=>rank(a)-rank(b)||a.createdAt.localeCompare(b.createdAt)||a.attemptId.localeCompare(b.attemptId));
   for(const intent of intents){
    diagnostic={flow:'initialization',stage:'VALIDATE_BATCH',captureId:intent.attemptId};
    check(signal);const dir=join(root,'cases',intent.attemptId);
    if(await serviceExists(join(dir,'not-dispatched-closed.json')))continue;
    const review=await reviewed(intent);
    const caseManifests:SpoolManifest[]=[];
    if(await serviceExists(join(dir,'import','batches')))for(const id of await readdir(join(dir,'import','batches'))){
     const m=await privateJson(join(dir,'import','batches',id,'manifest.json')) as SpoolManifest;
     caseManifests.push(m);
     knownBatches.push(m.identity.batchId);knownRuns.push(m.identity.runId);
    }
    if(await serviceExists(join(dir,'delivered.json'))){
     const {response}=await captureInfo(root,intent.attemptId),ack=await privateJson(join(dir,'delivered.json')) as {captureSha256:string;reviewDigest:string;batchId:string;confirmedAt:string};
     if(ack.captureSha256!==response.sha256 || ack.reviewDigest!==digest(review))throw Error('LOCAL_COMMIT_REQUIRES_REVIEW');
     const manifest=caseManifests[0];
     if(caseManifests.length!==1 || !manifest?.sealed || manifest.state!=='committed' || manifest.rows!==response.received || manifest.identity.batchId!==ack.batchId ||
      canonical(manifest.identity.plan)!==canonical(planForCapture(review,new Date(intent.createdAt))))throw Error('LOCAL_COMMIT_REQUIRES_REVIEW');
     if((await pools.ingest.query('SELECT manifest_digest FROM olos_export.large_batch_receipts WHERE batch_id=$1',[ack.batchId])).rows[0]?.manifest_digest!==manifestDigest(manifest) ||
      (await pools.project.query("SELECT 1 FROM olos_export.projection_outbox WHERE batch_id=$1 AND state<>'confirmed' LIMIT 1",[ack.batchId])).rowCount)throw Error('LOCAL_COMMIT_REQUIRES_REVIEW');
     if(!lastImportConfirmedAt || ack.confirmedAt>lastImportConfirmedAt)lastImportConfirmedAt=ack.confirmedAt;
     confirmedCaptures.add(intent.attemptId);
     if(pending?.attemptId===intent.attemptId){await enqueue(intent,review);items.at(-1)!.done=true;}
     if(await serviceExists(join(dir,'queued.json')))ordinal=Math.max(ordinal,queuedCaptureSchema.parse(await privateJson(join(dir,'queued.json'))).ordinal);
     last=intent;continue;
    }
    if(await serviceExists(join(dir,'queued.json'))){
     const entry=queuedCaptureSchema.parse(await privateJson(join(dir,'queued.json')));
     if(entry.ordinal<=ordinal || entry.captureId!==intent.attemptId || entry.intentDigest!==digest(intent) || entry.reviewDigest!==digest(review) || entry.configurationDigest!==loaded.configurationDigest)throw Error('CAPTURE_QUEUE_REQUIRES_REVIEW');
     const {response}=await captureInfo(root,intent.attemptId);
     const meta=await privateJson(join(dir,'capture','batches',intent.attemptId,'response.json')) as {rawMessageComplete?:boolean};
     if(response.sha256!==entry.captureSha256 || meta.rawMessageComplete!==true)throw Error('CAPTURE_QUEUE_REQUIRES_REVIEW');
     if(pending?.attemptId===intent.attemptId)await handoffCapture(base,root,entry);
     else if(digest(await privateJson(join(dir,'remote-pending.captured.json')))!==entry.pendingDigest)throw Error('CAPTURE_QUEUE_REQUIRES_REVIEW');
     ordinal=entry.ordinal;items.push({entry,intent,review,done:false});last=intent;
    }else{
     if(pending?.attemptId!==intent.attemptId)throw Error('CAPTURE_QUEUE_REQUIRES_REVIEW');
     await enqueue(intent,review);
    }
   }
   diagnostic={flow:'initialization',stage:'INITIALIZE'};
   if(await pendingServiceCase(base,root))throw Error('RECOVERY_REQUIRED');
   if((await pools.ingest.query("SELECT 1 FROM olos_export.runs WHERE source_table='AttemptsRawData' AND state IN ('read_intent','persisting','needs_reconciliation') AND NOT(id=ANY($1::uuid[])) LIMIT 1",[knownRuns])).rowCount ||
    (await pools.project.query("SELECT 1 FROM olos_export.projection_outbox WHERE state<>'confirmed' AND NOT(batch_id=ANY($1::uuid[])) LIMIT 1",[knownBatches])).rowCount)throw Error('CAPTURE_QUEUE_REQUIRES_REVIEW');
   const mode={kind:'olos-independent-delivery/v1',configurationDigest:loaded.configurationDigest};
   if(await serviceExists(join(root,'queue-mode.json'))){if(digest(await privateJson(join(root,'queue-mode.json')))!==digest(mode))throw Error('CAPTURE_QUEUE_REQUIRES_REVIEW');}
   else await durableExclusive(join(root,'queue-mode.json'),mode);
   // Recover already-started work before arming any new acquisition. Expired/uncertain local runs still fail closed.
   let unstarted=false;
   for(const item of items){if(item.done)continue;if(await serviceExists(join(root,'cases',item.intent.attemptId,'import-started.json'))){
    if(unstarted)throw Error('CAPTURE_QUEUE_REQUIRES_REVIEW');
    await materialize(item,signal);diagnostic={flow:'initialization',stage:'RECONCILE',captureId:item.intent.attemptId,batchId:item.manifest!.identity.batchId,streamId:item.manifest!.identity.streamId};
    await item.context!.pipeline.reconcileLocal(item.manifest!.identity.batchId,signal);
   }else unstarted=true;}
   for(const review of bootstrapReviews){
    diagnostic={flow:'initialization',stage:'VALIDATE_BATCH',captureId:review.captureId};
    if(!confirmedCaptures.has(review.captureId) && !items.some(i=>i.entry.captureId===review.captureId && i.entry.reviewDigest===digest(review) && i.manifest))throw Error('BOOTSTRAP_PERSISTENCE_NOT_CONFIRMED');
   }
   diagnostic={flow:'initialization',stage:'INITIALIZE'};check(signal);initialized=true;
   }catch(error){throw fail(error,diagnostic,signal);}
  },
  async acquire(signal:AbortSignal){
   if(!initialized || acquiring)throw Error('CAPTURE_QUEUE_REQUIRES_REVIEW');acquiring=true;
   let stage:FailureContext['stage']='AUTHORIZATION',captureId:string|undefined;
   try{check(signal);
    const authorized=await serviceAuthorization(options),reviews=await assertActive(options,authorized.loaded,authorized.approval);
    if(authorized.loaded.configurationDigest!==loaded.configurationDigest)throw Error('SERVICE_CONFIGURATION_CHANGED');
    const activation=await privateJson(join(root,'activation.json')) as {recoveryEvidence:string;recoveryDigest:string};
    const activeGuard=()=>{check(signal);authorized.guard();if(digest(privateServiceJsonSync(join(root,'activation.json')))!==digest(activation) || digest(privateServiceJsonSync(activation.recoveryEvidence))!==activation.recoveryDigest || reviews.some(r=>digest(privateServiceJsonSync(join(root,'reviews',`${r.procedure}.json`)))!==digest(r)))throw Error('ACTIVATION_REQUIREMENTS_MISSING');};
    if(await pendingServiceCase(base,root))throw Error('RECOVERY_REQUIRED');
    stage='ADMISSION';admission=await captureAdmission(root,items.filter(i=>!i.done).length,items.filter(i=>!i.done && !i.manifest).length,limits,spoolLimits().maxDiskBytes,spoolLimits().minFreeBytes);
    if(!admission.allowed)return {received:0,persisted:0,duplicates:0,rejected:0,acquisitionPerformed:false};
    const procedure=last?.procedure==='ReadDataCurrentDay'?'ReadDataPreviousDays':'ReadDataCurrentDay',review=reviews.find(r=>r.procedure===procedure)!;
    stage='PREFLIGHT';await new PostgresLargeBatchPersistence(pools.ingest,spoolLimits(),300000).preflight(planForCapture(review));activeGuard();stage='CAPTURE';
    const intent=await capture(base,root,loaded,authorized.approval,activeGuard,procedure,signal,review);
    captureId=intent.attemptId;stage='HANDOFF';const received=await enqueue(intent,review);
    return {received,persisted:0,duplicates:0,rejected:0,acquisitionPerformed:true};
   }catch(error){
    if(stage==='CAPTURE')try{captureId=(await pendingServiceCase(base,root))?.attemptId;}catch{/* diagnostic only */}
    throw fail(error,{flow:'acquisition',stage,captureId},signal);
   }finally{acquiring=false;}
  },
  async step(signal:AbortSignal){
   if(!initialized || delivering)throw Error('CAPTURE_QUEUE_REQUIRES_REVIEW');delivering=true;
   let stage:FailureContext['stage']='MATERIALIZE',current:Item|undefined;
   try{check(signal);
    const next=items.find(i=>!i.done && !i.manifest);if(next){current=next;await materialize(next,signal);}
    const selected=fair.select(items.filter(i=>!i.done && i.manifest && (i.retryAt??0)<=Date.now()).map(item=>({ordinal:item.entry.ordinal,item})))?.item;
    if(!selected)return false;
    current=selected;stage='DELIVER';
    const ctx=await context(selected);check(signal);
    if(digest(privateServiceJsonSync(join(root,'reviews',`${selected.intent.procedure}.json`)))!==selected.entry.reviewDigest)throw Error('OBSERVED_CONTRACT_NOT_APPROVED');
    const result=await ctx.pipeline.deliverStep(selected.manifest!.identity.batchId,signal);
    if(result.count>0 || result.complete){selected.deferrals=0;delete selected.retryAt;}
    if(result.complete){
     stage='CHECKPOINT';
     await ctx.pipeline.writer.confirmDeliveryCheckpoint(selected.manifest!);
     const receipt=(await pools.ingest.query('SELECT received_count,persisted_count,duplicate_count,rejected_count FROM olos_export.batches WHERE id=$1 AND state=$2',[selected.manifest!.identity.batchId,'committed'])).rows[0];
     if(!receipt)throw Error('LOCAL_COMMIT_REQUIRES_REVIEW');
     const confirmedAt=new Date().toISOString();
     stage='ACK';
     await durableExclusive(ctx.done,{received:receipt.received_count,persisted:receipt.persisted_count,duplicates:receipt.duplicate_count,rejected:receipt.rejected_count,
      confirmedAt,captureSha256:selected.entry.captureSha256,reviewDigest:selected.entry.reviewDigest,batchId:selected.manifest!.identity.batchId});
     lastImportConfirmedAt=confirmedAt;
     selected.done=true;delete selected.context;
     items.splice(items.indexOf(selected),1); // Retain evidence on disk, not completed replay contexts in memory.
    }
    return result.complete || result.count>0;
   }catch(error){
    const context:FailureContext={flow:'delivery',stage,captureId:current?.intent.attemptId,batchId:current?.manifest?.identity.batchId,streamId:current?.manifest?.identity.streamId};
    annotateFailure(error,context);
    if(stage==='DELIVER' && current && !signal.aborted && !failed && isRecoverableClaimFailure(error) && (current.deferrals??0)<3){
     check(signal);current.deferrals=(current.deferrals??0)+1;current.retryAt=Date.now()+1000*2**(current.deferrals-1);
     lastDeferred=logWorkerFailure('worker_delivery_deferred',error,context,new Date(current.retryAt).toISOString());return false;
    }
    throw fail(error,context,signal);
   }finally{delivering=false;}
  }
 };
}
function queueError(error:unknown){
 return failureDetails(error).code;
}
/** Explicit operator-authorized, local-only ingestion reconciliation. No source factory is invoked. */
export async function reconcileServiceIngestion(options:ServiceOptions,loaded:Loaded,pools:RuntimePools,input:unknown,signal:AbortSignal,guard:()=>void){
 const a=localReconciliationApprovalSchema.parse(input);
 let stage:FailureContext['stage']='VALIDATE_BATCH';try{
 const {intent}=await captureInfo(options.root,a.captureId);
 const review=contractReviewSchema.parse(await privateJson(join(options.root,'reviews',`${intent.procedure}.json`)));
 const ctx=await captureDelivery(options.root,loaded,intent,review,pools,true);
 const {spool,dir,plan,response,pipeline}=ctx;
 if(digest(await privateJson(ctx.started))!==digest({attemptId:a.captureId,reviewDigest:digest(review),captureSha256:response.sha256}))throw Error('LOCAL_COMMIT_REQUIRES_REVIEW');
 const batches=await readdir(join(dir,'import','batches'));
 if(batches.length!==1 || batches[0]!==a.batchId)throw Error('LOCAL_COMMIT_REQUIRES_REVIEW');
 const m=await privateJson(join(dir,'import','batches',a.batchId,'manifest.json')) as SpoolManifest;
 if(m.rows!==response.received || canonical(m.identity.plan)!==canonical(plan))throw Error('LOCAL_COMMIT_REQUIRES_REVIEW');
 guard();await pipeline.writer.preflight(plan);
 stage='RECONCILE';const handle=await reconcileLocalIngestion(pools.ingest,spool,m,a,signal);
 guard();
 stage='MATERIALIZE';const counts=await pipeline.writer.persistReconciled(m,handle,()=>spool.records(m));
 // Only a confirmed DB receipt permits the normal durable spool transition; no marker forgery.
 if(await spool.active())await spool.mark(m,'committed');
 await pipeline.writer.validateDeliveryPosition(m);
 return {event:'local_ingestion_reconciled',captureId:a.captureId,batchId:a.batchId,originalRunId:a.runId,recoveryRunId:handle.runId,
  recoveryCaseId:a.recoveryCaseId,fencingToken:handle.fencingToken,...counts,acquisitionPerformed:false,schedulerStarted:false,retryRemote:false};
 }catch(error){const context:FailureContext={flow:'delivery',stage,captureId:a.captureId,batchId:a.batchId};logWorkerFailure('worker_flow_failed',error,context);throw annotateFailure(error,context);}
}
export async function serviceCommand(action:string,options:ServiceOptions,flags:Record<string,string>){
 if(action==='service-status')return operationalServiceStatus(options);
 await directories(options.base,options.root);
 if(action==='reconcile-inspection')return reconcileServiceInspection(options.base,await privateJson(flags['--decision-file']!));
 const {loaded,approval,guard}=await serviceAuthorization(options);guard();
 if(action==='close-not-dispatched')return withLocalLock(options,async(_pools,signal)=>({
  event:'service_not_dispatched_closed',...await closeNotDispatchedServiceCase(options,flags['--attempt-id']!,loaded.configurationDigest,guard,signal),
  acquisitionPerformed:false,schedulerStarted:false,retryRemote:false
 }),true);
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
 if(action==='reconcile-ingestion')return withLocalLock(options,async(pools,signal)=>reconcileServiceIngestion(options,loaded,pools,await privateJson(flags['--reconciliation-file']!),signal,guard),true);
 if(action==='recover-queue')return withLocalLock(options,async(pools,signal)=>{
  const queue=createServiceCaptureQueue(options,pools,loaded,guard);await queue.initialize(signal);
  while((queue.snapshot().captureQueue.pending)>0){if(!await queue.step(signal))throw Error('DELIVERY_REQUIRES_RECONCILIATION');}
  return {event:'service_queue_recovered',...queue.snapshot(),acquisitionPerformed:false,schedulerStarted:false};
 },true);
 if(action==='activate')return withLocalLock(options,async(pools,signal)=>{
  await promoteService(options.root,loaded,approval,await privateJson(flags['--activation-file']!),{options,pools,signal,guard});
  return {state:'ACTIVE',schedulerStarted:false,acquisitionPerformed:false};
 },true);
 if(action==='start'){
  await assertActive(options,loaded,approval);
  const settings=await localSettings();if(canonical(settings.postgres)!==canonical(loaded.config.postgres))throw Error('POSTGRES_DESTINATION_CHANGED');
  const pools=createLocalRuntimePools(settings.passwords,()=>{},settings.postgres);
  const queue=createServiceCaptureQueue(options,pools,loaded,guard);
  const previousStatus=await operationalServiceStatus(options);
  const status:Record<string,unknown>={...previousStatus,nextExecutionAt:null,schedulerActive:false};
  await mkdir(join(options.root,'cycles'),{recursive:true,mode:0o700});await privateDirectory(join(options.root,'cycles'));
  let runtime:AttemptsService;
  runtime=new AttemptsService(pools,120000,async signal=>{
   status.lastCycleStartedAt=new Date().toISOString();status.nextExecutionAt=null;
   try{
    try{const pin=await loadOlosPinApproval(loaded.config.pinApprovalFile);status.tls={valid:true,expiresAt:pin.expiresAt};}catch(error){status.tls={valid:false,code:inspectionErrorCode(error)};} // Local replay remains possible without a live source/pin.
    const counts=await queue.acquire(signal);
    const completed=new Date().toISOString();Object.assign(status,{state:'ACTIVE',lastCycleCompletedAt:completed,pendingBatch:null,lastError:null,nextExecutionAt:new Date(Date.now()+120000).toISOString()});
    const intents=(await readdir(options.root)).filter(f=>/^attempt-.*\.json$/.test(f));let latest:Intent|undefined;
    for(const name of intents){const i=captureIntentSchema.parse(await privateJson(join(options.root,name)));if(!await serviceExists(join(options.root,'cases',i.attemptId,'queued.json')))continue;if(!latest || i.createdAt>latest.createdAt)latest=i;}
    status.lastProcedure=latest?.procedure??null;
    await durableExclusive(join(options.root,'cycles',`${Date.now()}-${randomUUID()}.json`),status);return counts;
   }catch(error){
    try{const pending=await pendingServiceCase(options.base,options.root);status.pendingBatch=pending?.attemptId??null;status.lastAttemptedProcedure=pending?.procedure??null;}catch{/* keep unavailable status null */}
    Object.assign(status,{state:'RECOVERY_REQUIRED',lastError:inspectionErrorCode(error),nextExecutionAt:null,schedulerActive:false});
    await durableExclusive(join(options.root,'cycles',`${Date.now()}-${randomUUID()}.json`),status).catch(()=>{});throw error;}
  },'corporate-service',()=>({...status,...queue.snapshot(),state:(status.tls as {valid:boolean}|null)?.valid===false || !runtime?.snapshotBaseArmed()?'RECOVERY_REQUIRED':'ACTIVE',postgresState:runtime?.snapshotDatabaseState()??'unknown',countsScope:'postgres_aggregate_delivery_counts',businessRowsInserted:null,nextExecutionAt:runtime?.snapshotNextExecutionAt()??null,schedulerActive:runtime?.snapshotBaseArmed()??false}),queue);
  status.nextExecutionAt=new Date(Date.now()+120000).toISOString();
  await serveWorker({...settings.config,SYNC_INTERVAL_MS:120000},[{name:'postgres',configured:true,probe:()=>runtime.probe(),close:async()=>{}}],runtime);
  return;
 }
 throw Error('SERVICE_COMMAND_REJECTED');
}
