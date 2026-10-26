import {beforeAll,afterAll,beforeEach,afterEach,it,expect,vi} from 'vitest';
import {mkdtemp,mkdir,readFile,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {existsSync} from 'node:fs';
import {join} from 'node:path';
import {randomUUID} from 'node:crypto';
import pg from 'pg';
import {isolatedPostgres} from './support/postgres.js';
import {fields,responseForIds,collation} from './support/activation-capture.js';
import {LocalSpool} from '../adapters/local-spool.js';
import {spoolLimits} from '../src/large-batch/config.js';
import {attemptsFixturePlan,signature} from '../src/integration/attempts/contracts.js';
import {deliverServiceCapture,captureIntentSchema,loadServiceConfiguration,reviewServiceContract,promoteService,serviceCycle,createServiceCaptureQueue,observeServiceCapture,reconcileServiceIngestion} from '../adapters/attempts-service.js';
import {serviceConfigSchema,planForCapture,type ContractReview} from '../src/integration/attempts/activation.js';
import {exampleProfile,columnTypes} from '../src/offline/model.js';
import {APPROVED_COLUMNS,IsolatedTdsCallsWriter} from '../src/offline/tds-writer.js';
import {digest} from '../src/persistence/validation.js';
import {IsolatedTdsSource} from '../src/olos/tds/source.js';
import {PostgresDeliveryTarget} from '../src/projection/postgres-target.js';
import {ProjectionDispatcher} from '../src/projection/dispatcher.js';
import {ProjectionOutbox} from '../src/projection/outbox.js';
import {PersistenceError} from '../src/persistence/types.js';
import {preserveFailure} from '../src/diagnostics/worker-failure.js';
import {PostgresPersistence} from '../src/persistence/store.js';
import {PostgresLargeBatchPersistence,manifestDigest} from '../src/persistence/large-batch.js';
import {InspectionDiagnostics} from '../src/olos/tds/inspection-diagnostics.js';
let fixture:Awaited<ReturnType<typeof isolatedPostgres>>,root:string,captureDate:string,sequence=0;
let pools:{ingest:pg.Pool;project:pg.Pool;receiver:pg.Pool};
beforeAll(async()=>{
 fixture=await isolatedPostgres();
 for(const f of ['migrations/003_projection_control.sql','permissions/projection.sql','migrations/005_large_batch_control.sql','permissions/large-batch.sql','migrations/004_delivery_receipts.sql','permissions/receiver.sql','migrations/007_local_ingestion_reconciliation.sql','permissions/local-ingestion-reconciliation.sql'])await fixture.admin.query(await readFile(new URL('../'+f,import.meta.url),'utf8'));
 await fixture.admin.query('CREATE SCHEMA raw');
 await fixture.admin.query(`CREATE TABLE raw.olos_calls(${APPROVED_COLUMNS.map(c=>`"${c}" ${columnTypes[c]} ${c==='internal_id_olos'?'PRIMARY KEY':''}`).join(',')})`);
 await fixture.admin.query('GRANT USAGE ON SCHEMA raw TO olos_export_receiver');await fixture.admin.query('GRANT SELECT,INSERT ON raw.olos_calls TO olos_export_receiver');
},120000);
afterAll(async()=>{await fixture?.stop();});
beforeEach(async()=>{
 const date=new Date('2026-10-22T00:00:00Z');date.setUTCDate(date.getUTCDate()+sequence++);captureDate=date.toISOString().slice(0,10);
 root=await mkdtemp(join(tmpdir(),'olos-service-delivery-'));
 pools={ingest:fixture.worker(),project:new pg.Pool({...fixture.connection,options:'-c role=olos_export_projector'}),receiver:new pg.Pool({...fixture.connection,options:'-c role=olos_export_receiver'})};
 vi.spyOn(IsolatedTdsSource.prototype,'call').mockImplementation(()=>{throw Error('REMOTE_RPC_FORBIDDEN_DURING_LOCAL_REPLAY');});
});
afterEach(async()=>{expect(IsolatedTdsSource.prototype.call).not.toHaveBeenCalled();vi.restoreAllMocks();await Promise.all(Object.values(pools).map(p=>p.end()));await rm(root,{recursive:true,force:true});});
async function prepared(version:string,procedure:'ReadDataCurrentDay'|'ReadDataPreviousDays'='ReadDataCurrentDay',rows=1){
 const config=serviceConfigSchema.parse({version:'olos-service/v2',workerId:'isolated-test',endpoint:{server:'10.196.234.11',port:1433,database:'ExportDataV3',user:'OEC',password:'SYNTHETIC'},postgres:{host:'127.0.0.1',port:5434,database:'olos_worker'},pinApprovalFile:'/unused',approvalFile:join(root,'approval.json'),campaignFile:join(root,'campaign.json'),dispositionFile:join(root,'disposition.json'),profile:{...exampleProfile,phoneMatchKey:'normalized',columns:[...APPROVED_COLUMNS]}});
 await writeFile(join(root,'config.json'),JSON.stringify(config),{mode:0o600});
 await writeFile(config.campaignFile,'[]',{mode:0o600});await writeFile(config.dispositionFile,'[]',{mode:0o600});
 const loaded=await loadServiceConfiguration(join(root,'config.json'));
 const template=attemptsFixturePlan(captureDate,collation.toBuffer().toString('hex'));
 const day=new Date(`${captureDate}T00:00:00Z`);if(procedure==='ReadDataPreviousDays')day.setUTCDate(day.getUTCDate()-1);const controlDate=day.toISOString().slice(0,10);
 const plan={...template,signature:signature(procedure),fields:fields.map(({sqlLength:_s,collationHex:_c,unicodeMetadataPolicy:_u,...f})=>f) as typeof template.fields,contract:{...template.contract,version},run:{...template.run,contractVersion:version,readType:procedure==='ReadDataCurrentDay'?'current_day' as const:'previous_days' as const,exportDate:controlDate}};
 await fixture.admin.query('INSERT INTO olos_export.source_contracts(source_table,version,approved,fields,key_fields,layout_evidence,key_evidence) VALUES($1,$2,true,$3::jsonb,$4,$5,$6) ON CONFLICT DO NOTHING',[plan.contract.source_table,version,JSON.stringify(plan.contract.fields),plan.contract.key_fields,plan.contract.layout_evidence,plan.contract.key_evidence]);
 const attemptId=randomUUID(),dir=join(root,'cases',attemptId);await mkdir(dir,{recursive:true,mode:0o700});
 const spool=await LocalSpool.open(join(dir,'capture'),spoolLimits()),capture=await spool.createResponseCapture(attemptId);await capture.append(responseForIds(Array.from({length:rows},(_,i)=>i+1)));const seal=await capture.seal({received:rows,fields,responseValidated:true,rawMessageComplete:true,transportSecurity:[{encrypted:true,identityValidated:true,protocol:'TLSv1.2',tlsMode:'certificate-pinned'}]});
 const review:ContractReview={version:'olos-contract-review/v1',procedure,captureId:attemptId,captureSha256:seal.sha256,configurationDigest:loaded.configurationDigest,operator:'test-reviewer',reference:'isolated-source-observation',reviewedAt:new Date().toISOString(),sourceTimeZone:'UTC',controlDayOffset:procedure==='ReadDataCurrentDay'?0:-1,partitionSemanticsEvidence:'isolated control bucket',plan,fields};
 const intent=captureIntentSchema.parse({attemptId,procedure,configurationDigest:loaded.configurationDigest,workDir:root,createdAt:`${captureDate}T10:00:00.000Z`,controlDate,operator:'test',approvalReference:'isolated',pinApprovalId:'unused-local',contractReviewDigest:digest(review),plan,state:'PREPARED'});
 await writeFile(join(root,`attempt-${attemptId}.json`),JSON.stringify(intent),{mode:0o600});return {intent,review,loaded};
}
async function activationRequest(current:Awaited<ReturnType<typeof prepared>>,previous:Awaited<ReturnType<typeof prepared>>){
 const grant={scope:'OLOS_ATTEMPTS_SERVICE',approvalId:'test-approval',workerId:current.loaded.config.workerId,configurationDigest:current.loaded.configurationDigest,
  procedures:['ReadDataCurrentDay','ReadDataPreviousDays'],table:'AttemptsRawData',destination:'olos_worker.raw.olos_calls',operator:'fixture-authority',reference:'isolated approval only',
  issuedAt:'2020-01-01T00:00:00.000Z',reviewPolicy:'REVOKE_BY_PRIVATE_FILE',revocationFile:join(root,'revoked.json'),bootstrapRecoveryEvidence:'isolated approved first-read exercise'} as const;
 await writeFile(current.loaded.config.approvalFile,JSON.stringify(grant),{mode:0o600});
 const recoveryFile=join(root,'recovery.json');await writeFile(recoveryFile,JSON.stringify({scope:'OLOS_CONSUMPTIVE_RECOVERY',configurationDigest:current.loaded.configurationDigest,
  captureIds:[current.intent.attemptId,previous.intent.attemptId],operator:'test',reference:'fixture-only',approvedAt:new Date().toISOString(),ownershipEvidence:'isolated ownership exercise',retentionCompletenessEvidence:'isolated fixture completeness',recoveryExerciseEvidence:'real local receipt exercise above; NOT Olos proof',externalRunbookReference:'synthetic fixture reference',failurePolicy:'STOP_PRESERVE_EXTERNAL_RECONCILIATION_NO_REMOTE_RETRY'}),{mode:0o600});
 const request={version:'olos-activation/v1',workerId:current.loaded.config.workerId,configurationDigest:current.loaded.configurationDigest,approvalDigest:digest(grant),
  contractDigests:{ReadDataCurrentDay:digest(current.review),ReadDataPreviousDays:digest(previous.review)},captureIds:{ReadDataCurrentDay:current.intent.attemptId,ReadDataPreviousDays:previous.intent.attemptId},recoveryEvidence:recoveryFile,operator:'fixture-authority',reference:'fixture-only',issuedAt:new Date().toISOString()};
 return {grant,request};
}
it('explicit ingestion reconciliation preserves the capture and resumes V2 delivery without any RPC',async()=>{
 const p=await prepared('service-explicit-local-reconciliation/v1','ReadDataCurrentDay',130);
 await reviewServiceContract(root,p.loaded,p.review);
 const signal=new AbortController().signal;
 const failure=vi.spyOn(PostgresLargeBatchPersistence.prototype,'persist').mockRejectedValueOnce(new PersistenceError('LEASE_LOST'));
 await expect(deliverServiceCapture(root,p.loaded,p.intent,p.review,pools,signal)).rejects.toMatchObject({code:'LEASE_LOST'});failure.mockRestore();
 const spool=await LocalSpool.open(join(root,'cases',p.intent.attemptId,'import'),spoolLimits()),m=(await spool.active())!;
 await fixture.admin.query("UPDATE olos_export.source_locks SET lease_until=clock_timestamp()-interval '1 second' WHERE owner_run_id=$1",[m.identity.runId]);
 const [caseId]=await new PostgresPersistence(pools.ingest).recoverExpired();
 const generation=(await pools.ingest.query("SELECT generation FROM olos_export.source_locks WHERE source_table='AttemptsRawData'")).rows[0].generation;
 const a={version:'olos-local-ingestion-reconciliation/v1',authorizationId:randomUUID(),captureId:p.intent.attemptId,batchId:m.identity.batchId,runId:m.identity.runId,recoveryCaseId:caseId,manifestDigest:manifestDigest(m),expectedGeneration:generation,operatorReference:'TEST-LOCAL-ONLY',authorizedAt:new Date().toISOString(),expiresAt:new Date(Date.now()+3600000).toISOString(),decision:'AUTHORIZE_VERIFIED_LOCAL_REPLAY',remoteReplayAllowed:false};
 const base=join(root,'isolated-control');await mkdir(base,{mode:0o700});
 await writeFile(join(base,'remote-pending.json'),JSON.stringify({attemptId:p.intent.attemptId,workDir:root,configurationDigest:p.loaded.configurationDigest}),{mode:0o600});
 const options={base,root,configFile:join(root,'config.json'),intervalMs:120000};
 const result=await reconcileServiceIngestion(options,p.loaded,pools,a,signal,()=>{});
 expect(result.batchId).toBe(m.identity.batchId);expect(result.received).toBe(130);expect(result.acquisitionPerformed).toBe(false);
 expect(existsSync(join(base,'remote-pending.json'))).toBe(true); // only V2 handoff may archive it
 await reconcileServiceIngestion(options,p.loaded,pools,a,signal,()=>{}); // same proof and receipt
 const queue=createServiceCaptureQueue(options,pools,p.loaded,()=>{});await queue.initialize(signal);await queue.step(signal);
 expect((await pools.project.query("SELECT count(*)::int n FROM olos_export.projection_outbox WHERE batch_id=$1 AND state='confirmed'",[m.identity.batchId])).rows[0].n).toBe(64);
 const restarted=createServiceCaptureQueue(options,pools,p.loaded,()=>{});await restarted.initialize(signal);await restarted.step(signal);await restarted.step(signal);
 expect(restarted.snapshot().captureQueue.pending).toBe(0);
 expect((await pools.ingest.query('SELECT version FROM olos_export.checkpoints WHERE stream_id=$1',[m.identity.streamId])).rows[0].version).toBe('1');
 expect(JSON.parse(await readFile(join(root,'cases',p.intent.attemptId,'delivered.json'),'utf8')).batchId).toBe(m.identity.batchId);
 expect(existsSync(join(root,'cases',p.intent.attemptId,'remote-pending.captured.json'))).toBe(true);
},30000);
it('PostgreSQL unavailable before import resumes the complete spool locally without a new RPC',async()=>{
 const p=await prepared('service-local-outage/v1'),failure=vi.spyOn(pools.ingest,'query').mockRejectedValueOnce(Error('local postgres down'));
 await expect(deliverServiceCapture(root,p.loaded,p.intent,p.review,pools,new AbortController().signal)).rejects.toThrow();failure.mockRestore();
 const result=await deliverServiceCapture(root,p.loaded,p.intent,p.review,pools,new AbortController().signal);expect(result.received).toBe(1);
 expect((await fixture.admin.query('SELECT c.version::text FROM olos_export.checkpoints c WHERE c.stream_id=(SELECT stream_id FROM olos_export.runs WHERE contract_version=$1 ORDER BY started_at DESC LIMIT 1)',[p.review.plan.contract.version])).rows[0].version).toBe('1');
},30000);
it('receiver COMMIT without ACK reconciles its durable receipt before checkpoint, preserving an existing PK',async()=>{
 const p=await prepared('service-local-ack-loss/v1');
 await fixture.admin.query("INSERT INTO raw.olos_calls(internal_id_olos,olos_call_id) VALUES(1,'preserve-existing') ON CONFLICT(internal_id_olos) DO UPDATE SET olos_call_id='preserve-existing'");
 const original=pools.receiver.connect.bind(pools.receiver);let lost=false;
 const spy=vi.spyOn(pools.receiver,'connect').mockImplementation((async()=>{
  const client=await original();return new Proxy(client,{get(target,key){if(key==='query')return async(sql:string,...args:unknown[])=>{
   const result=await (target.query as (...a:unknown[])=>Promise<unknown>).call(target,sql,...args);
   if(sql==='COMMIT' && !lost){lost=true;throw Error('simulated lost commit ACK');}return result;
  };const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;}});
 }) as typeof pools.receiver.connect);
 const writes=vi.spyOn(IsolatedTdsCallsWriter.prototype,'writeInTransaction');
 await expect(deliverServiceCapture(root,p.loaded,p.intent,p.review,pools,new AbortController().signal)).rejects.toThrow();spy.mockRestore();
 expect(lost).toBe(true);expect((await fixture.admin.query('SELECT c.version::text FROM olos_export.checkpoints c WHERE c.stream_id=(SELECT stream_id FROM olos_export.runs WHERE contract_version=$1 ORDER BY started_at DESC LIMIT 1)',[p.review.plan.contract.version])).rows[0].version).toBe('0');
 const before=writes.mock.calls.length;
 const result=await deliverServiceCapture(root,p.loaded,p.intent,p.review,pools,new AbortController().signal);expect(result.received).toBe(1);expect(writes.mock.calls.length).toBe(before);
 expect((await fixture.admin.query('SELECT olos_call_id FROM raw.olos_calls WHERE internal_id_olos=1')).rows[0].olos_call_id).toBe('preserve-existing');
 const second=await prepared('service-local-ack-loss/v1');const duplicate=await deliverServiceCapture(root,second.loaded,second.intent,second.review,pools,new AbortController().signal);
 expect(duplicate.duplicates).toBe(1);
},30000);

it('ACTIVE promotion requires both real local deliveries and restart replays a complete pending batch without RPC',async()=>{
 const current=await prepared('service-active-current/v1');await reviewServiceContract(root,current.loaded,current.review);
 await deliverServiceCapture(root,current.loaded,current.intent,current.review,pools,new AbortController().signal);
 const previous=await prepared('service-active-previous/v1','ReadDataPreviousDays');await reviewServiceContract(root,previous.loaded,previous.review);
 const {grant,request}=await activationRequest(current,previous);
 await expect(promoteService(root,current.loaded,grant as never,request)).rejects.toThrow();
 await deliverServiceCapture(root,previous.loaded,previous.intent,previous.review,pools,new AbortController().signal);
 await promoteService(root,current.loaded,grant as never,request);
 const pending=await prepared('service-active-current/v1');pending.intent.contractReviewDigest=digest(current.review);
 await writeFile(join(root,`attempt-${pending.intent.attemptId}.json`),JSON.stringify(pending.intent),{mode:0o600});
 const base=join(root,'isolated-control');await mkdir(base,{mode:0o700});await writeFile(join(base,'remote-pending.json'),JSON.stringify({attemptId:pending.intent.attemptId,workDir:root,configurationDigest:pending.loaded.configurationDigest}),{mode:0o600});
 await writeFile(join(base,'log-inspection-active.json'),JSON.stringify({attemptId:'193c7869-6ba0-480f-a7c8-3fcd6e178394',workDir:join(base,'log-inspections','193c7869-6ba0-480f-a7c8-3fcd6e178394')}),{mode:0o600});
 const result=await serviceCycle({base,root,configFile:join(root,'config.json'),intervalMs:120000},pools,new AbortController().signal);
 expect(result.received).toBe(1);expect(result.acquisitionPerformed).toBe(false);
 expect(JSON.parse(await readFile(join(root,'cases',pending.intent.attemptId,'remote-pending.completed.json'),'utf8')).attemptId).toBe(pending.intent.attemptId);
 // ACTIVE uses the same local closure as the administrative BOOTSTRAP command.
 const id=randomUUID(),dir=join(root,'cases',id),intent={...pending.intent,attemptId:id};
 await mkdir(dir,{mode:0o700});
 const spool=await LocalSpool.open(join(dir,'capture'),spoolLimits()),capture=await spool.createResponseCapture(id);await capture.interrupt();
 const diagnostic={attemptId:id,...new InspectionDiagnostics().snapshot()};
 const proof={kind:'olos-local-not-dispatched/v1',attemptId:id,diagnosticDigest:digest(diagnostic),evidence:'CONTROLLED_RPC_HANDOFF_NOT_REACHED_AFTER_TRANSPORT_CLOSED'};
 await writeFile(join(root,`attempt-${id}.json`),JSON.stringify(intent),{mode:0o600});
 await writeFile(join(dir,'diagnostic.json'),JSON.stringify(diagnostic),{mode:0o600});
 await writeFile(join(dir,'not-dispatched.json'),JSON.stringify(proof),{mode:0o600});
 await writeFile(join(base,'remote-pending.json'),JSON.stringify({attemptId:id,workDir:root,configurationDigest:pending.loaded.configurationDigest}),{mode:0o600});
 expect(await serviceCycle({base,root,configFile:join(root,'config.json'),intervalMs:120000},pools,new AbortController().signal)).toEqual({received:0,persisted:0,duplicates:0,rejected:0,confirmedAt:null,acquisitionPerformed:false});
 expect(JSON.parse(await readFile(join(dir,'not-dispatched-closed.json'),'utf8'))).toEqual(proof);
 expect(JSON.parse(await readFile(join(dir,'remote-pending.not-dispatched.json'),'utf8')).attemptId).toBe(id);
 // The real acquisition adapter is replaced with a synthetic sealed capture only in this test.
 let acquisitions=0,release!:()=>void;
 const syntheticCapture:typeof observeServiceCapture=async(_base,_root,_loaded,_approval,activeGuard,procedure,_signal,review)=>{
  acquisitions++;activeGuard();if(acquisitions===1)await new Promise<void>(r=>{release=r;});
  const p=await prepared(review!.plan.contract.version,procedure),createdAt=`${captureDate}T11:00:00.000Z`,plan=planForCapture(review!,new Date(createdAt));
  p.intent={...p.intent,createdAt,plan,controlDate:plan.run.exportDate,contractReviewDigest:digest(review)};
  await writeFile(join(root,`attempt-${p.intent.attemptId}.json`),JSON.stringify(p.intent),{mode:0o600});
  await writeFile(join(base,'remote-pending.json'),JSON.stringify({attemptId:p.intent.attemptId,workDir:root,configurationDigest:p.loaded.configurationDigest}),{mode:0o600});
  return p.intent;
 };
 const options={base,root,configFile:join(root,'config.json'),intervalMs:120000,queueMaxCaptures:2};
 const queue=createServiceCaptureQueue(options,pools,current.loaded,()=>{},syntheticCapture),signal=new AbortController().signal;
 await queue.initialize(signal);const inFlight=queue.acquire(signal);
 for(let n=0;n<200 && !release;n++)await new Promise(r=>setTimeout(r,5));expect(acquisitions).toBe(1);
 await expect(queue.acquire(signal)).rejects.toThrow('CAPTURE_QUEUE_REQUIRES_REVIEW');release();expect((await inFlight).acquisitionPerformed).toBe(true);
 const defer=vi.spyOn(ProjectionDispatcher.prototype,'runBatch').mockResolvedValueOnce(0);await queue.step(signal);defer.mockRestore();
 expect(queue.snapshot().captureQueue.materialized).toBe(1);
 expect((await queue.acquire(signal)).acquisitionPerformed).toBe(true);expect(acquisitions).toBe(2);
 expect(queue.snapshot().captureQueue.pending).toBe(2);
 expect((await queue.acquire(signal)).acquisitionPerformed).toBe(false);expect(acquisitions).toBe(2);
 expect(queue.snapshot().captureQueue.admission?.reason).toBe('CAPTURE_QUEUE_COUNT_LIMIT');
 while(queue.snapshot().captureQueue.pending)await queue.step(signal);
},30000);

async function queuedFixture(version:string){
 const p=await prepared(version);await reviewServiceContract(root,p.loaded,p.review);
 const base=join(root,'control');await mkdir(base,{mode:0o700});
 await writeFile(join(base,'remote-pending.json'),JSON.stringify({attemptId:p.intent.attemptId,workDir:root,configurationDigest:p.loaded.configurationDigest}),{mode:0o600});
 const options={base,root,configFile:join(root,'config.json'),intervalMs:120000};
 return {...p,base,options};
}
it('queue adopts an interrupted legacy import, preserves its batch/receipts and closes locally without RPC',async()=>{
 const p=await queuedFixture('queued-legacy/v1');
 const stop=vi.spyOn(PostgresLargeBatchPersistence.prototype,'confirmDeliveryCheckpoint').mockRejectedValueOnce(Error('simulated shutdown after receipts'));
 await expect(deliverServiceCapture(root,p.loaded,p.intent,p.review,pools,new AbortController().signal)).rejects.toThrow();stop.mockRestore();
 const before=(await fixture.admin.query('SELECT id FROM olos_export.batches WHERE run_id IN (SELECT id FROM olos_export.runs WHERE contract_version=$1)',[p.review.plan.contract.version])).rows;
 const queue=createServiceCaptureQueue(p.options,pools,p.loaded,()=>{});await queue.initialize(new AbortController().signal);
 expect(queue.snapshot().captureQueue.pending).toBe(1);
 expect(JSON.parse(await readFile(join(root,'cases',p.intent.attemptId,'remote-pending.captured.json'),'utf8')).attemptId).toBe(p.intent.attemptId);
 await expect(readFile(join(p.base,'remote-pending.json'))).rejects.toMatchObject({code:'ENOENT'});
 const send=vi.spyOn(ProjectionDispatcher.prototype,'runBatch');await queue.step(new AbortController().signal);
 expect(queue.snapshot().captureQueue.pending).toBe(0);
 expect((await fixture.admin.query('SELECT id FROM olos_export.batches WHERE run_id IN (SELECT id FROM olos_export.runs WHERE contract_version=$1)',[p.review.plan.contract.version])).rows).toEqual(before);
 expect(send).toHaveBeenCalledOnce();expect(await send.mock.results[0]!.value).toBe(0);
 const restart=createServiceCaptureQueue(p.options,pools,p.loaded,()=>{});await restart.initialize(new AbortController().signal);expect(await restart.step(new AbortController().signal)).toBe(false);
 await expect(deliverServiceCapture(root,p.loaded,p.intent,p.review,pools,new AbortController().signal)).rejects.toThrow('CAPTURE_QUEUE_RECOVERY_REQUIRED');
},30000);
it('migration of a delivered pending uses the durable queue handoff and preserves its acknowledgment',async()=>{
 const p=await queuedFixture('queued-delivered-adoption/v1'),signal=new AbortController().signal;
 await deliverServiceCapture(root,p.loaded,p.intent,p.review,pools,signal);
 const dir=join(root,'cases',p.intent.attemptId),ack=await readFile(join(dir,'delivered.json'),'utf8');
 const queue=createServiceCaptureQueue(p.options,pools,p.loaded,()=>{});await queue.initialize(signal);
 expect(queue.snapshot().captureQueue.pending).toBe(0);expect(await readFile(join(dir,'delivered.json'),'utf8')).toBe(ack);
 expect(JSON.parse(await readFile(join(dir,'remote-pending.captured.json'),'utf8')).attemptId).toBe(p.intent.attemptId);
 await expect(readFile(join(p.base,'remote-pending.json'))).rejects.toMatchObject({code:'ENOENT'});
 await expect(readFile(join(dir,'remote-pending.completed.json'))).rejects.toMatchObject({code:'ENOENT'});
 const restart=createServiceCaptureQueue(p.options,pools,p.loaded,()=>{});await restart.initialize(signal);expect(await restart.step(signal)).toBe(false);
});

it('queue delivers a recent batch first, keeps the delivery checkpoint at the historical gap, and survives restart',async()=>{
 const p=await queuedFixture('queued-order/v1'),signal=new AbortController().signal;
 const first=createServiceCaptureQueue(p.options,pools,p.loaded,()=>{});await first.initialize(signal);
 const defer=vi.spyOn(ProjectionDispatcher.prototype,'runBatch').mockResolvedValueOnce(0);await first.step(signal);defer.mockRestore();
 expect(first.snapshot().captureQueue.pending).toBe(1);
 const newer=await prepared('queued-order/v1');newer.intent.createdAt=`${captureDate}T10:00:01.000Z`;newer.intent.contractReviewDigest=digest(p.review);
 await writeFile(join(root,`attempt-${newer.intent.attemptId}.json`),JSON.stringify(newer.intent),{mode:0o600});
 await writeFile(join(p.base,'remote-pending.json'),JSON.stringify({attemptId:newer.intent.attemptId,workDir:root,configurationDigest:p.loaded.configurationDigest}),{mode:0o600});
 const second=createServiceCaptureQueue(p.options,pools,p.loaded,()=>{});await second.initialize(signal);
 await second.step(signal);expect(second.snapshot().captureQueue.pending).toBe(1);
 const checkpoint=async()=>(await fixture.admin.query('SELECT version FROM olos_export.checkpoints WHERE stream_id=(SELECT stream_id FROM olos_export.runs WHERE contract_version=$1 LIMIT 1)',[p.review.plan.contract.version])).rows[0].version;
 expect(await checkpoint()).toBe('0');
 expect(JSON.parse(await readFile(join(root,'cases',newer.intent.attemptId,'delivered.json'),'utf8')).received).toBe(1);
 await expect(readFile(join(root,'cases',p.intent.attemptId,'delivered.json'))).rejects.toMatchObject({code:'ENOENT'});
 const restarted=createServiceCaptureQueue(p.options,pools,p.loaded,()=>{});await restarted.initialize(signal);
 const newerPending={attemptId:randomUUID(),workDir:root,configurationDigest:p.loaded.configurationDigest};
 await writeFile(join(p.base,'remote-pending.json'),JSON.stringify(newerPending),{mode:0o600});
 await restarted.step(signal);
 expect(JSON.parse(await readFile(join(p.base,'remote-pending.json'),'utf8'))).toEqual(newerPending);
 expect(restarted.snapshot().captureQueue.pending).toBe(0);expect(await checkpoint()).toBe('2');
},30000);
it.each(['partial','digest'])('queue refuses %s capture evidence without archiving the remote marker',async failure=>{
 const p=await queuedFixture(`queued-refusal-${failure}/v1`),dir=join(root,'cases',p.intent.attemptId,'capture','batches',p.intent.attemptId);
 if(failure==='partial'){
  const path=join(dir,'response.json'),metadata=JSON.parse(await readFile(path,'utf8'));metadata.rawMessageComplete=false;await writeFile(path,JSON.stringify(metadata),{mode:0o600});
 }else await writeFile(join(dir,'response.ready'),Buffer.from('corrupt'),{mode:0o600});
 const queue=createServiceCaptureQueue(p.options,pools,p.loaded,()=>{});await expect(queue.initialize(new AbortController().signal)).rejects.toThrow();
 expect(JSON.parse(await readFile(join(p.base,'remote-pending.json'),'utf8')).attemptId).toBe(p.intent.attemptId);
 await expect(queue.acquire(new AbortController().signal)).rejects.toThrow('CAPTURE_QUEUE_REQUIRES_REVIEW');
});

it.each(['none','migration','checkpoint-before','checkpoint-after','claim-contention'])('activates with a partially delivered bootstrap and survives %s failure without reingestion',async failure=>{
 const signal=new AbortController().signal,current=await prepared(`bootstrap-current-${failure}/v1`);
 await reviewServiceContract(root,current.loaded,current.review);
 await deliverServiceCapture(root,current.loaded,current.intent,current.review,pools,signal);
 const previous=await prepared(`bootstrap-previous-${failure}/v1`,'ReadDataPreviousDays',130);
 await reviewServiceContract(root,previous.loaded,previous.review);
 const base=join(root,'control');await mkdir(base,{mode:0o700});
 const marker={attemptId:previous.intent.attemptId,workDir:root,configurationDigest:previous.loaded.configurationDigest};
 await writeFile(join(base,'remote-pending.json'),JSON.stringify(marker),{mode:0o600});
 const runBatch=ProjectionDispatcher.prototype.runBatch;let groups=0;
 const partial=vi.spyOn(ProjectionDispatcher.prototype,'runBatch').mockImplementation(async function(this:ProjectionDispatcher,...args){if(groups)return 0;const count=await runBatch.apply(this,args);if(count)groups++;return count;});
 await expect(deliverServiceCapture(root,previous.loaded,previous.intent,previous.review,pools,signal)).rejects.toThrow('DELIVERY_REQUIRES_RECONCILIATION');partial.mockRestore();
 const batch=(await fixture.admin.query('SELECT * FROM olos_export.batches WHERE run_id IN (SELECT id FROM olos_export.runs WHERE contract_version=$1)',[previous.review.plan.contract.version])).rows[0];
 const dir=join(root,'cases',previous.intent.attemptId),manifestPath=join(dir,'import','batches',batch.id,'manifest.json'),manifest=await readFile(manifestPath,'utf8');
 const snapshot=async()=>(await fixture.admin.query('SELECT delivery_id,request_digest,state FROM olos_export.projection_outbox WHERE batch_id=$1 ORDER BY delivery_id',[batch.id])).rows;
 const before=await snapshot();expect(before.filter(r=>r.state==='confirmed')).toHaveLength(64);expect(before.filter(r=>r.state==='pending')).toHaveLength(66);
 const checkpoint=async()=>(await fixture.admin.query('SELECT version FROM olos_export.checkpoints WHERE stream_id=$1',[batch.stream_id])).rows[0].version;
 expect(await checkpoint()).toBe('0');
 const {grant,request}=await activationRequest(current,previous),options={base,root,configFile:join(root,'config.json'),intervalMs:120000};
 const migration={options,pools,signal,guard:()=>{}};
 await expect(readFile(join(root,'queue-mode.json'))).rejects.toMatchObject({code:'ENOENT'});
 if(failure==='migration'){
  // Fail after the durable handoff, before activation can be written.
  await expect(promoteService(root,current.loaded,grant as never,request,{...migration,guard:()=>{if(existsSync(join(root,'queue-mode.json')))throw Error('simulated migration crash');}})).rejects.toThrow('simulated migration crash');
  await expect(readFile(join(root,'activation.json'))).rejects.toMatchObject({code:'ENOENT'});
 }
 await promoteService(root,current.loaded,grant as never,request,migration);
 expect(await readFile(manifestPath,'utf8')).toBe(manifest);expect(await snapshot()).toEqual(before);expect(await checkpoint()).toBe('0');
 expect(JSON.parse(await readFile(join(dir,'remote-pending.captured.json'),'utf8'))).toEqual(marker);
 await expect(readFile(join(base,'remote-pending.json'))).rejects.toMatchObject({code:'ENOENT'});
 await expect(readFile(join(dir,'delivered.json'))).rejects.toMatchObject({code:'ENOENT'});
 const capture:typeof observeServiceCapture=async(_b,_r,_l,_a,guard,procedure,_signal,review)=>{
  guard();expect(procedure).toBe('ReadDataCurrentDay');
  const next=await prepared(review!.plan.contract.version,procedure);next.intent.contractReviewDigest=digest(review);
  await writeFile(join(root,`attempt-${next.intent.attemptId}.json`),JSON.stringify(next.intent),{mode:0o600});
  await writeFile(join(base,'remote-pending.json'),JSON.stringify({...marker,attemptId:next.intent.attemptId}),{mode:0o600});return next.intent;
 };
 const synthetic=vi.fn(capture),queue=createServiceCaptureQueue(options,pools,current.loaded,()=>{},synthetic);
 await queue.initialize(signal);
 if(failure==='claim-contention'){
  const busy=vi.spyOn(ProjectionOutbox.prototype,'claimBatch').mockRejectedValueOnce(preserveFailure(new PersistenceError('BUSY'),{code:'55P03'}));
  expect(await queue.step(signal)).toBe(false);busy.mockRestore();
  expect(queue.snapshot()).toMatchObject({lastError:null,queueFailure:null,deferredDeliveries:1,lastDeferred:{flow:'delivery',stage:'CLAIM',captureId:previous.intent.attemptId,batchId:batch.id,technicalCode:'55P03'}});
  expect(await snapshot()).toEqual(before);expect(await checkpoint()).toBe('0');
 }
 // assertActive accepts the persisted activation while the original 66 deliveries remain pending.
 expect((await queue.acquire(signal)).acquisitionPerformed).toBe(true);expect(synthetic).toHaveBeenCalledOnce();expect(await snapshot()).toEqual(before);
 let restarted=createServiceCaptureQueue(options,pools,current.loaded,()=>{});await restarted.initialize(signal);
 if(failure.startsWith('checkpoint-')){
  const confirm=PostgresLargeBatchPersistence.prototype.confirmDeliveryCheckpoint;
  const crash=vi.spyOn(PostgresLargeBatchPersistence.prototype,'confirmDeliveryCheckpoint').mockImplementation(async function(this:PostgresLargeBatchPersistence,m){if(m.identity.batchId!==batch.id)return confirm.call(this,m);if(failure==='checkpoint-after')await confirm.call(this,m);throw Error('simulated checkpoint crash');});
  await expect((async()=>{while(restarted.snapshot().captureQueue.pending)await restarted.step(signal);})()).rejects.toThrow('simulated checkpoint crash');crash.mockRestore();
  expect(await checkpoint()).toBe(failure==='checkpoint-after'?'1':'0');
  await expect(readFile(join(dir,'delivered.json'))).rejects.toMatchObject({code:'ENOENT'});
  await expect(restarted.acquire(signal)).rejects.toThrow('RECOVERY_REQUIRED');
  restarted=createServiceCaptureQueue(options,pools,current.loaded,()=>{});await restarted.initialize(signal);
 }
 while(restarted.snapshot().captureQueue.pending)await restarted.step(signal);
 expect(await checkpoint()).toBe('1');expect(await readFile(manifestPath,'utf8')).toBe(manifest);
 const after=await snapshot();expect(after.map(r=>r.delivery_id)).toEqual(before.map(r=>r.delivery_id));for(const r of before.filter(r=>r.state==='confirmed'))expect(after.find(a=>a.delivery_id===r.delivery_id)).toEqual(r);expect(after.every(r=>r.state==='confirmed')).toBe(true);
 expect((await fixture.admin.query('SELECT count(*)::int AS n FROM olos_export.delivery_receipts WHERE delivery_id=ANY($1::uuid[])',[before.map(r=>r.delivery_id)])).rows[0].n).toBe(130);
 expect((await fixture.admin.query('SELECT id FROM olos_export.batches WHERE run_id=$1',[batch.run_id])).rows).toEqual([{id:batch.id}]);
 // A duplicate bootstrap observation retains its own receipts, without duplicating business keys.
 expect((await fixture.admin.query('SELECT count(*)::int AS n FROM raw.olos_calls WHERE internal_id_olos BETWEEN 1 AND 130')).rows[0].n).toBe(130);
},30000);

it.each(['unpersisted','receipt','rows','position','uncertain'])('rejects bootstrap activation with %s evidence and never enables acquisition',async failure=>{
 const signal=new AbortController().signal,current=await prepared(`reject-current-${failure}/v1`);
 await reviewServiceContract(root,current.loaded,current.review);await deliverServiceCapture(root,current.loaded,current.intent,current.review,pools,signal);
 const previous=await prepared(`reject-previous-${failure}/v1`,'ReadDataPreviousDays');await reviewServiceContract(root,previous.loaded,previous.review);
 const base=join(root,'control');await mkdir(base,{mode:0o700});
 await writeFile(join(base,'remote-pending.json'),JSON.stringify({attemptId:previous.intent.attemptId,workDir:root,configurationDigest:previous.loaded.configurationDigest}),{mode:0o600});
 let restore=async()=>{};
 if(failure!=='unpersisted'){
  const defer=vi.spyOn(ProjectionDispatcher.prototype,'runBatch').mockResolvedValue(0);
  await expect(deliverServiceCapture(root,previous.loaded,previous.intent,previous.review,pools,signal)).rejects.toThrow('DELIVERY_REQUIRES_RECONCILIATION');defer.mockRestore();
  const batch=(await fixture.admin.query('SELECT * FROM olos_export.batches WHERE run_id IN (SELECT id FROM olos_export.runs WHERE contract_version=$1)',[previous.review.plan.contract.version])).rows[0];
  if(failure==='receipt'){
   const saved=(await fixture.admin.query('SELECT manifest_digest FROM olos_export.large_batch_receipts WHERE batch_id=$1',[batch.id])).rows[0].manifest_digest;
   await fixture.admin.query('UPDATE olos_export.large_batch_receipts SET manifest_digest=$2 WHERE batch_id=$1',[batch.id,'0'.repeat(64)]);
   restore=async()=>{await fixture.admin.query('UPDATE olos_export.large_batch_receipts SET manifest_digest=$2 WHERE batch_id=$1',[batch.id,saved]);};
  }else if(failure==='rows'){
   const path=join(root,'cases',previous.intent.attemptId,'import','batches',batch.id,'manifest.json'),saved=await readFile(path,'utf8');
   await writeFile(path,JSON.stringify({...JSON.parse(saved),rows:2}),{mode:0o600});restore=async()=>{await writeFile(path,saved,{mode:0o600});};
  }else if(failure==='position'){
   await fixture.admin.query('UPDATE olos_export.batches SET expected_version=2 WHERE id=$1',[batch.id]);
   restore=async()=>{await fixture.admin.query('UPDATE olos_export.batches SET expected_version=$2 WHERE id=$1',[batch.id,batch.expected_version]);};
  }else{
   const connect=pools.receiver.connect.bind(pools.receiver);let interrupted=false;
   const lost=vi.spyOn(pools.receiver,'connect').mockImplementation((async()=>{
    const client=await connect();return new Proxy(client,{get(target,key){if(key==='query')return async(sql:string,...args:unknown[])=>{
     if(sql==='COMMIT' && !interrupted){interrupted=true;await target.query('ROLLBACK');throw Error('simulated missing commit ACK');}
     return (target.query as (...a:unknown[])=>Promise<unknown>).call(target,sql,...args);
    };const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;}});
   }) as typeof pools.receiver.connect);
   await expect(deliverServiceCapture(root,previous.loaded,previous.intent,previous.review,pools,signal)).rejects.toThrow('DELIVERY_REQUIRES_RECONCILIATION');lost.mockRestore();expect(interrupted).toBe(true);
   expect((await fixture.admin.query('SELECT state FROM olos_export.projection_outbox WHERE batch_id=$1',[batch.id])).rows[0].state).toBe('uncertain');
   // Isolated fixture cleanup after the test proves refusal; never an operational recovery procedure.
   restore=async()=>{await fixture.admin.query("UPDATE olos_export.projection_outbox SET state='pending' WHERE batch_id=$1",[batch.id]);};
  }
 }
 const {grant,request}=await activationRequest(current,previous),options={base,root,configFile:join(root,'config.json'),intervalMs:120000};
 const sends=vi.spyOn(ProjectionDispatcher.prototype,'runBatch');
 await expect(promoteService(root,current.loaded,grant as never,request,{options,pools,signal,guard:()=>{}})).rejects.toThrow();
 expect(sends).not.toHaveBeenCalled();sends.mockRestore();
 await expect(readFile(join(root,'activation.json'))).rejects.toMatchObject({code:'ENOENT'});
 const synthetic=vi.fn<typeof observeServiceCapture>(),blocked=createServiceCaptureQueue(options,pools,current.loaded,()=>{},synthetic);
 await expect(blocked.acquire(signal)).rejects.toThrow('CAPTURE_QUEUE_REQUIRES_REVIEW');expect(synthetic).not.toHaveBeenCalled();
 await restore();const recovery=createServiceCaptureQueue(options,pools,current.loaded,()=>{});await recovery.initialize(signal);while(recovery.snapshot().captureQueue.pending)await recovery.step(signal);
},30000);

it('claim contention has bounded deferrals, never retries unknown errors, and restarts with the same batch',async()=>{
 const p=await queuedFixture('queued-claim-contention/v1'),signal=new AbortController().signal;
 const queue=createServiceCaptureQueue(p.options,pools,p.loaded,()=>{});await queue.initialize(signal);
 const busy=vi.spyOn(ProjectionOutbox.prototype,'claimBatch').mockImplementation(async()=>{throw preserveFailure(new PersistenceError('BUSY'),{code:'55P03'});});
 let now=Date.now();const clock=vi.spyOn(Date,'now').mockImplementation(()=>now);
 try{
  for(let n=0;n<3;n++){expect(await queue.step(signal)).toBe(false);expect(queue.snapshot().queueFailure).toBeNull();now+=31000;}
  await expect(queue.step(signal)).rejects.toMatchObject({code:'BUSY'});
  expect(queue.snapshot().queueFailure).toMatchObject({flow:'delivery',stage:'CLAIM',captureId:p.intent.attemptId,technicalCode:'55P03'});
  await expect(queue.acquire(signal)).rejects.toThrow('RECOVERY_REQUIRED');
 }finally{busy.mockRestore();clock.mockRestore();}
 const before=(await fixture.admin.query('SELECT id FROM olos_export.batches WHERE run_id IN (SELECT id FROM olos_export.runs WHERE contract_version=$1)',[p.review.plan.contract.version])).rows;
 const restarted=createServiceCaptureQueue(p.options,pools,p.loaded,()=>{});await restarted.initialize(signal);while(restarted.snapshot().captureQueue.pending)await restarted.step(signal);
 expect((await fixture.admin.query('SELECT id FROM olos_export.batches WHERE run_id IN (SELECT id FROM olos_export.runs WHERE contract_version=$1)',[p.review.plan.contract.version])).rows).toEqual(before);
});

it('queue restart reconciles confirmed receipts, but absent receipts block before acquisition and are never resent',async()=>{
 for(const committed of [true,false]){
  const p=await queuedFixture(`queued-unknown-${committed}/v1`),signal=new AbortController().signal;
  const queue=createServiceCaptureQueue(p.options,pools,p.loaded,()=>{});await queue.initialize(signal);
  const connect=pools.receiver.connect.bind(pools.receiver);let lost=false;
  const send=vi.spyOn(pools.receiver,'connect').mockImplementation((async()=>{
   const client=await connect();return new Proxy(client,{get(target,key){if(key==='query')return async(sql:string,...args:unknown[])=>{
    if(sql==='COMMIT' && !lost){lost=true;if(committed)await target.query('COMMIT');else await target.query('ROLLBACK');throw Error('simulated missing ACK');}
    return (target.query as (...a:unknown[])=>Promise<unknown>).call(target,sql,...args);
   };const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;}});
  }) as typeof pools.receiver.connect);
  await expect(queue.step(signal)).rejects.toThrow('DELIVERY_REQUIRES_RECONCILIATION');expect(lost).toBe(true);send.mockRestore();
  await expect(queue.acquire(signal)).rejects.toThrow('RECOVERY_REQUIRED');
  const before=(await fixture.admin.query("SELECT count(*)::int AS n FROM olos_export.projection_events WHERE event='dispatch_authorized'")).rows[0].n;
  const receiptReads=vi.spyOn(PostgresDeliveryTarget.prototype,'receipt');
  const restarted=createServiceCaptureQueue(p.options,pools,p.loaded,()=>{});
  if(committed){await restarted.initialize(signal);await restarted.step(signal);expect(restarted.snapshot().captureQueue.pending).toBe(0);}
  else {await expect(restarted.initialize(signal)).rejects.toThrow('DELIVERY_REQUIRES_RECONCILIATION');await expect(restarted.acquire(signal)).rejects.toThrow('CAPTURE_QUEUE_REQUIRES_REVIEW');}
  expect(receiptReads).toHaveBeenCalledOnce();receiptReads.mockRestore();
  expect((await fixture.admin.query("SELECT count(*)::int AS n FROM olos_export.projection_events WHERE event='dispatch_authorized'")).rows[0].n).toBe(before);
  // Reuse this test's filesystem only after the first confirmed exercise has been preserved in PostgreSQL.
  if(committed){await rm(root,{recursive:true,force:true});root=await mkdtemp(join(tmpdir(),'olos-service-delivery-'));}
 }
},30000);
