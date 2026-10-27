import {beforeEach,afterEach,it,expect,vi} from 'vitest';
import {mkdtemp,mkdir,writeFile,readFile,readdir,rm,rename} from 'node:fs/promises';
import {writeFileSync} from 'node:fs';
import {tmpdir} from 'node:os';
import {join,dirname} from 'node:path';
import {randomUUID} from 'node:crypto';
import {Pool} from 'pg';
import {isolatedPostgres} from './support/postgres.js';
import {exampleProfile} from '../src/offline/model.js';
import {APPROVED_COLUMNS} from '../src/offline/tds-writer.js';
import {serviceConfigSchema} from '../src/integration/attempts/activation.js';
import {AttemptsService} from '../src/integration/attempts/service.js';
import {ExactConnection} from '../src/olos/tds/connection.js';
import {IsolatedTdsSource} from '../src/olos/tds/source.js';
import {InspectionDiagnostics} from '../src/olos/tds/inspection-diagnostics.js';
import {digest} from '../src/persistence/validation.js';
import {LocalSpool} from '../adapters/local-spool.js';
import {spoolLimits} from '../src/large-batch/config.js';
import {PostgresPersistence} from '../src/persistence/store.js';
import {largePlan} from './support/large-batch-fixture.js';
import type {SpoolManifest} from '../src/large-batch/types.js';

const startRuntime=AttemptsService.prototype.start;

let runId:string,streamId:string,generation:string,manifest:SpoolManifest;

const state=vi.hoisted(()=>({home:'',settings:vi.fn(),pools:vi.fn()}));
vi.mock('node:os',async original=>({...await original<typeof import('node:os')>(),homedir:()=>state.home}));
vi.mock('../adapters/local-worker.js',()=>({localSettings:state.settings}));
vi.mock('../src/integration/attempts/pools.js',()=>({createLocalRuntimePools:state.pools}));
import {corporateCommand,parseCorporateArguments} from '../adapters/attempts-corporate.js';
import {captureIntentSchema,loadServiceConfiguration,createServiceCaptureQueue} from '../adapters/attempts-service.js';

let fixture:Awaited<ReturnType<typeof isolatedPostgres>>,base:string,root:string,configFile:string,id:string,dir:string,captureDir:string;
let loaded:Awaited<ReturnType<typeof loadServiceConfiguration>>,output:ReturnType<typeof vi.spyOn>,cancel:()=>void;
const save=async(path:string,value:unknown)=>{await mkdir(dirname(path),{recursive:true,mode:0o700});await writeFile(path,JSON.stringify(value),{mode:0o600});};
const json=async(path:string)=>JSON.parse(await readFile(path,'utf8'));
async function snapshot(path=state.home):Promise<Record<string,string>>{
 const files:Record<string,string>={};
 for(const entry of await readdir(path,{withFileTypes:true})){
  const p=join(path,entry.name);if(entry.isDirectory())Object.assign(files,await snapshot(p));else files[p]=await readFile(p,'base64');
 }return files;
}

beforeEach(async()=>{
 fixture=await isolatedPostgres();
 for(const f of ['migrations/003_projection_control.sql','permissions/projection.sql','migrations/004_delivery_receipts.sql','migrations/005_large_batch_control.sql','permissions/large-batch.sql','migrations/007_local_ingestion_reconciliation.sql','permissions/local-ingestion-reconciliation.sql','migrations/008_administrative_capture_discard.sql','permissions/administrative-capture-discard.sql','migrations/009_not_dispatched_closures.sql','permissions/not-dispatched-closures.sql'])await fixture.admin.query(await readFile(new URL('../'+f,import.meta.url),'utf8'));
 state.home=await mkdtemp(join(tmpdir(),'olos-close-not-dispatched-'));
 base=join(state.home,'.local/state/olos-worker/corporate-attempts');root=join(base,'service-run');
 configFile=join(state.home,'.config/olos-worker/attempts-service.json');
 const config=serviceConfigSchema.parse({version:'olos-service/v2',workerId:'synthetic-worker',endpoint:{server:'10.196.234.11',port:1433,database:'ExportDataV3',user:'OEC',password:'SYNTHETIC'},
  postgres:{host:fixture.connection.host,port:fixture.connection.port,database:'olos_worker'},pinApprovalFile:join(state.home,'missing-pin.json'),approvalFile:join(state.home,'approval.json'),
  profile:{...exampleProfile,phoneMatchKey:'normalized',columns:[...APPROVED_COLUMNS]},campaignFile:join(state.home,'campaign.json'),dispositionFile:join(state.home,'disposition.json')});
 await save(configFile,config);await save(config.campaignFile,[]);await save(config.dispositionFile,[]);
 loaded=await loadServiceConfiguration(configFile);
 await save(config.approvalFile,{scope:'OLOS_ATTEMPTS_SERVICE',approvalId:'synthetic',workerId:config.workerId,configurationDigest:loaded.configurationDigest,
  procedures:['ReadDataCurrentDay','ReadDataPreviousDays'],table:'AttemptsRawData',destination:'olos_worker.raw.olos_calls',operator:'test',reference:'synthetic',issuedAt:'2020-01-01T00:00:00.000Z',
  reviewPolicy:'REVOKE_BY_PRIVATE_FILE',revocationFile:join(state.home,'revoked.json'),bootstrapRecoveryEvidence:'synthetic fixture only'});
 state.settings.mockReset().mockResolvedValue({postgres:config.postgres,passwords:{}});
 state.pools.mockReset().mockImplementation((_passwords:unknown,onError:()=>void)=>{cancel=onError;return {ingest:fixture.worker(),project:new Pool(fixture.connection),receiver:new Pool(fixture.connection)};});
 // Only the runtime schema probe is stubbed; advisory locking uses a real, isolated PostgreSQL.
 vi.spyOn(AttemptsService.prototype,'probe').mockResolvedValue();
 vi.spyOn(AttemptsService.prototype,'start').mockImplementation(()=>{throw Error('SCHEDULER_FORBIDDEN');});
 vi.spyOn(ExactConnection.prototype,'connect').mockImplementation(()=>{throw Error('OLOS_CONNECTION_FORBIDDEN');});
 vi.spyOn(IsolatedTdsSource.prototype,'call').mockImplementation(()=>{throw Error('RPC_FORBIDDEN');});
 output=vi.spyOn(process.stdout,'write').mockImplementation(()=>true);
 id=randomUUID();dir=join(root,'cases',id);captureDir=join(dir,'capture','batches',id);
 await mkdir(dir,{recursive:true,mode:0o700});
 const spool=await LocalSpool.open(join(dir,'capture'),spoolLimits()),capture=await spool.createResponseCapture(id);await capture.interrupt();
 const diagnostic=new InspectionDiagnostics();diagnostic.observe({kind:'stage',stage:'TLS_HANDSHAKE_STARTED'});diagnostic.observe({kind:'failure',errorCode:'INVALID_TDS'});
 const data={attemptId:id,...diagnostic.snapshot()};
 await save(join(dir,'diagnostic.json'),data);
 await save(join(dir,'not-dispatched.json'),{kind:'olos-local-not-dispatched/v1',attemptId:id,diagnosticDigest:digest(data),evidence:'CONTROLLED_RPC_HANDOFF_NOT_REACHED_AFTER_TRANSPORT_CLOSED'});
 await save(join(dir,'remote-state.json'),{state:'REMOTE_OUTCOME_UNKNOWN',attemptId:id,retryRemote:false});
 await save(join(root,`attempt-${id}.json`),captureIntentSchema.parse({attemptId:id,procedure:'ReadDataCurrentDay',configurationDigest:loaded.configurationDigest,workDir:root,createdAt:new Date().toISOString(),controlDate:'2026-10-23',operator:'test',approvalReference:'synthetic',pinApprovalId:'unused',contractReviewDigest:null,plan:null,state:'PREPARED'}));
 await save(join(base,'remote-pending.json'),{attemptId:id,workDir:root,configurationDigest:loaded.configurationDigest});
 const plan=largePlan(),c=plan.contract;
 await fixture.admin.query('INSERT INTO olos_export.source_contracts(source_table,version,approved,fields,key_fields,layout_evidence,key_evidence) VALUES($1,$2,true,$3,$4,$5,$6)',[c.source_table,c.version,c.fields,c.key_fields,c.layout_evidence,c.key_evidence]);
 const handle=await new PostgresPersistence(fixture.admin).startRun(plan.run,new AbortController().signal);
 runId=handle.runId;streamId=handle.streamId;generation=handle.fencingToken;
 const spoolImport=await LocalSpool.open(join(dir,'import'),spoolLimits({minFreeBytes:0}));
 const writer=await spoolImport.create({format:'olos-spool/v1',batchId:randomUUID(),runId,streamId,fencingToken:generation,plan,expected:{version:'0',cursor:null}});await writer.interrupt();
 manifest=(await spoolImport.active())!;
 await change(join(root,`attempt-${id}.json`),{plan,controlDate:plan.run.exportDate});
 await fixture.admin.query("UPDATE olos_export.source_locks SET lease_until=clock_timestamp()-interval '1 second' WHERE owner_run_id=$1",[runId]);
},120000);
afterEach(async()=>{
 expect(ExactConnection.prototype.connect).not.toHaveBeenCalled();expect(IsolatedTdsSource.prototype.call).not.toHaveBeenCalled();expect(AttemptsService.prototype.start).not.toHaveBeenCalled();
 vi.restoreAllMocks();await rm(state.home,{recursive:true,force:true});await fixture?.stop();
});
const args=()=>['close-not-dispatched','--config',configFile,'--work-dir',root,'--attempt-id',id];
async function close(){await corporateCommand(args(),{SYNC_ENABLED:'false'});return JSON.parse(String(output.mock.calls.at(-1)?.[0]));}
async function change(path:string,patch:Record<string,unknown>){await save(path,{...await json(path),...patch});}
async function changeDiagnostic(patch:Record<string,unknown>){await change(join(dir,'diagnostic.json'),patch);await change(join(dir,'not-dispatched.json'),{diagnosticDigest:digest(await json(join(dir,'diagnostic.json')))});}
async function refused(code?:string){const before=await snapshot();await expect(close()).rejects.toThrow(code);expect(await snapshot()).toEqual(before);}

async function stats(){return (await fixture.admin.query(`SELECT
 (SELECT count(*)::int FROM olos_export.not_dispatched_closures) audits,
 (SELECT state FROM olos_export.runs WHERE id=$1) state,
 (SELECT generation FROM olos_export.source_locks WHERE source_table='AttemptsRawData') generation,
 (SELECT owner_run_id FROM olos_export.source_locks WHERE source_table='AttemptsRawData') owner,
 (SELECT count(*)::int FROM olos_export.recovery_cases) cases,
 (SELECT version FROM olos_export.checkpoints WHERE stream_id=$2) checkpoint`,[runId,streamId])).rows[0];}
function fault(when:(sql:string)=>'before'|'after'|undefined){
 const original=state.pools.getMockImplementation()!;let failed=false;
 state.pools.mockImplementation((...args)=>{
  const pools=original(...args),connect=pools.ingest.connect.bind(pools.ingest);
  vi.spyOn(pools.ingest,'connect').mockImplementation((async()=>{
   const client=await connect();return new Proxy(client,{get(target,key){if(key==='query')return async(sql:string,...params:unknown[])=>{
    const fail=failed?undefined:when(sql);if(fail==='before'){failed=true;throw Error('synthetic failure');}
    const result=await target.query(sql,...params);if(fail==='after'){failed=true;throw Error('synthetic lost acknowledgment');}return result;
   };const v=Reflect.get(target,key);return typeof v==='function'?v.bind(target):v;}});
  }) as typeof pools.ingest.connect);return pools;
 });
 return ()=>state.pools.mockImplementation(original);
}
it('atomically closes the exact run and fences it, with immutable audit and no checkpoint changes',async()=>{
 const result=await close();expect(result).toMatchObject({alreadyClosed:false,acquisitionPerformed:false,schedulerStarted:false,runClosure:{runId,runState:'failed',alreadyReconciled:false,releasedGeneration:'2'}});
 expect(await stats()).toEqual({audits:1,state:'failed',generation:'2',owner:null,cases:0,checkpoint:'0'});
 const evidence=(await fixture.admin.query('SELECT evidence FROM olos_export.not_dispatched_closures')).rows[0].evidence;
 expect(evidence).toMatchObject({captureId:id,manifestDigest:digest(manifest),diagnosticDigest:digest(await json(join(dir,'diagnostic.json')))});
 const p=fixture.worker();try{await expect(p.query('UPDATE olos_export.not_dispatched_closures SET evidence=evidence')).rejects.toMatchObject({code:'42501'});}finally{await p.end();}
 await expect(new PostgresPersistence(fixture.admin).renew({runId,streamId,fencingToken:generation,signal:new AbortController().signal})).rejects.toMatchObject({code:'LEASE_LOST'});
});
it('reconciles a closure already archived by the old CLI',async()=>{
 await save(join(dir,'not-dispatched-closed.json'),await json(join(dir,'not-dispatched.json')));
 await rename(join(base,'remote-pending.json'),join(dir,'remote-pending.not-dispatched.json'));
 const before=await snapshot();expect(await close()).toMatchObject({alreadyClosed:true,runClosure:{alreadyReconciled:false}});
 expect(await snapshot()).toEqual(before);expect((await stats()).state).toBe('failed');
});
it.each(['proof','digest','rpc','rpc-preparation','response','remote','binding','generation-binding','plan','records','sealed'])('rejects insufficient or changed local evidence: %s',async kind=>{
 if(kind==='proof')await rm(join(dir,'not-dispatched.json'));
 if(kind==='digest')await change(join(dir,'diagnostic.json'),{errorCode:'TIMEOUT'});
 if(kind==='rpc' || kind==='rpc-preparation')await changeDiagnostic({observedStages:['AUTHORIZATION_STARTED',kind==='rpc'?'RPC_HANDOFF_STARTED':'RPC_DISPATCH_STARTED']});
 if(kind==='response')await rm(join(captureDir,'response.json'));
 if(kind==='remote')await change(join(dir,'remote-state.json'),{attemptId:randomUUID()});
 if(kind==='binding')await rm(join(dir,'import'),{recursive:true});
 if(kind==='generation-binding' || kind==='plan'){
  const m=structuredClone(manifest);if(kind==='generation-binding')(m.identity as {fencingToken:string}).fencingToken='9';else (m.identity.plan.run as {exportDate:string}).exportDate='2020-01-01';
  await save(join(dir,'import','batches',m.identity.batchId,'manifest.json'),m);
 }
 if(kind==='records')await writeFile(join(dir,'import','batches',manifest.identity.batchId,'records.partial'),'unobserved bytes');
 if(kind==='sealed')await change(join(dir,'import','batches',manifest.identity.batchId,'manifest.json'),{sealed:true});
 await refused();expect((await stats()).audits).toBe(0);expect((await stats()).state).toBe('read_intent');
});
it.each(['active-lease','generation','owner','counter','batch','error','recovery','audit','other-run','state'])('refuses unsafe persisted state: %s',async kind=>{
 if(kind==='active-lease')await fixture.admin.query("UPDATE olos_export.source_locks SET lease_until=clock_timestamp()+interval '1 minute' WHERE owner_run_id=$1",[runId]);
 if(kind==='generation')await fixture.admin.query("UPDATE olos_export.source_locks SET generation=generation+1 WHERE owner_run_id=$1",[runId]);
 if(kind==='owner')await fixture.admin.query("UPDATE olos_export.source_locks SET owner_run_id=NULL,lease_until=NULL WHERE owner_run_id=$1",[runId]);
 if(kind==='counter')await fixture.admin.query('UPDATE olos_export.runs SET received_count=1 WHERE id=$1',[runId]);
 if(kind==='state')await fixture.admin.query("UPDATE olos_export.runs SET state='persisting' WHERE id=$1",[runId]);
 if(kind==='batch')await fixture.admin.query("INSERT INTO olos_export.batches(id,run_id,stream_id,source_table,digest,expected_version,state,received_count) VALUES($1,$2,$3,'AttemptsRawData',$4,0,'read_observed',0)",[manifest.identity.batchId,runId,streamId,'0'.repeat(64)]);
 if(kind==='error')await fixture.admin.query("INSERT INTO olos_export.sync_errors(id,run_id,code,outcome,received_count,rejected_count) VALUES($1,$2,'COMMIT_UNKNOWN','unknown',0,0)",[randomUUID(),runId]);
 if(kind==='recovery')await fixture.admin.query("INSERT INTO olos_export.recovery_cases(id,run_id,reason) VALUES($1,$2,'expired_read_intent')",[randomUUID(),runId]);
 if(kind==='audit')await fixture.admin.query("INSERT INTO olos_export.audit_events(id,run_id,event,received_count,persisted_count,duplicate_count) VALUES($1,$2,'reconciliation_required',0,0,0)",[randomUUID(),runId]);
 if(kind==='other-run')await fixture.admin.query("INSERT INTO olos_export.runs(id,stream_id,source_table,contract_version,fencing_token,state) SELECT $2,stream_id,source_table,contract_version,2,'read_intent' FROM olos_export.runs WHERE id=$1",[runId,randomUUID()]);
 await refused();expect((await stats()).audits).toBe(0);
});
it.each(['source','run','checkpoint'])('refuses concurrent %s writer without local changes',async target=>{
 const c=await fixture.admin.connect();try{
  await c.query('BEGIN');await c.query(target==='source'?"SELECT 1 FROM olos_export.source_locks WHERE source_table='AttemptsRawData' FOR UPDATE":target==='run'?'SELECT 1 FROM olos_export.runs WHERE id=$1 FOR UPDATE':'SELECT 1 FROM olos_export.checkpoints WHERE stream_id=$1 FOR UPDATE',target==='source'?[]:[target==='run'?runId:streamId]);
  await refused('BUSY');expect((await stats()).audits).toBe(0);
 }finally{await c.query('ROLLBACK');c.release();}
 await close();
});
it('serializes administrative competitors and repeats without touching a successor',async()=>{
 const results=await Promise.allSettled([close(),close()]);expect(results.some(r=>r.status==='fulfilled')).toBe(true);
 expect((await stats()).audits).toBe(1);
 const next=await new PostgresPersistence(fixture.admin).startRun(largePlan().run,new AbortController().signal);
 const before=await stats();expect(await close()).toMatchObject({alreadyClosed:true,runClosure:{alreadyReconciled:true}});
 expect(await stats()).toEqual(before);expect(before.owner).toBe(next.runId);expect(next.fencingToken).toBe('3');
});
it.each(['UPDATE olos_export.runs','UPDATE olos_export.source_locks'])('rolls back all writes on failure at %s',async point=>{
 const reset=fault(sql=>sql.startsWith(point)?'before':undefined);await refused('PERSISTENCE_FAILED');reset();
 expect(await stats()).toEqual({audits:0,state:'read_intent',generation:'1',owner:runId,cases:0,checkpoint:'0'});await close();
});
it.each(['before','after'] as const)('reconciles COMMIT_UNKNOWN with %s COMMIT failure without remote calls',async timing=>{
 const reset=fault(sql=>sql==='COMMIT'?timing:undefined);await refused('COMMIT_UNKNOWN');reset();
 expect((await stats()).audits).toBe(timing==='after'?1:0);
 expect(await close()).toMatchObject({runClosure:{alreadyReconciled:timing==='after'}});expect((await stats()).audits).toBe(1);
});
it('aborts and rolls back if the instance lock is lost after the terminal write',async()=>{
 const reset=fault(sql=>{if(sql.startsWith('UPDATE olos_export.runs'))cancel();return undefined;});await refused();reset();
 expect((await stats()).audits).toBe(0);expect((await stats()).state).toBe('read_intent');
});
it('revalidates local proof before commit and rolls back a concurrent evidence change',async()=>{
 const reset=fault(sql=>{if(sql.startsWith('UPDATE olos_export.runs'))writeFileSync(join(dir,'not-dispatched.json'),'{}');return undefined;});
 await expect(close()).rejects.toThrow();reset();expect((await stats()).audits).toBe(0);expect((await stats()).state).toBe('read_intent');
});
it('restart verifies the ledger, initializes the queue and arms a 120 second local scheduler',async()=>{
 await close();const pools={ingest:fixture.worker(),project:new Pool(fixture.connection),receiver:new Pool(fixture.connection)};
 const queue=createServiceCaptureQueue({base,root,configFile,intervalMs:120000},pools,loaded,()=>{});
 let called=0;
 // Only this post-administration phase invokes real runtime startup. Its acquisition
 // callback is local; administrative calls above retain the forbidden-start spy.
 const runtime=new AttemptsService(pools,120000,async()=>{called++;return {received:0,persisted:0,duplicates:0,rejected:0,acquisitionPerformed:false};},'isolated-fixture',undefined,queue);
 try{
  vi.useFakeTimers({toFake:['setTimeout','clearTimeout']});
  await startRuntime.call(runtime);expect(queue.snapshot().captureQueue.pending).toBe(0);expect(runtime.snapshotBaseArmed()).toBe(true);
  await vi.advanceTimersByTimeAsync(119999);expect(called).toBe(0);await vi.advanceTimersByTimeAsync(1);expect(called).toBe(1);expect(runtime.snapshotBaseArmed()).toBe(true);
  vi.useRealTimers();
  await fixture.admin.query("UPDATE olos_export.not_dispatched_closures SET evidence_digest=repeat('0',64)");
  await expect(createServiceCaptureQueue({base,root,configFile,intervalMs:120000},pools,loaded,()=>{}).initialize(new AbortController().signal)).rejects.toThrow('RECOVERY_REQUIRED');
 }finally{await runtime.stop();vi.useRealTimers();}
});
it('startup refuses an old local marker until explicit PostgreSQL reconciliation',async()=>{
 await save(join(dir,'not-dispatched-closed.json'),await json(join(dir,'not-dispatched.json')));await rename(join(base,'remote-pending.json'),join(dir,'remote-pending.not-dispatched.json'));
 const pools={ingest:fixture.worker(),project:new Pool(fixture.connection),receiver:new Pool(fixture.connection)};
 try{await expect(createServiceCaptureQueue({base,root,configFile,intervalMs:120000},pools,loaded,()=>{}).initialize(new AbortController().signal)).rejects.toThrow('RECOVERY_REQUIRED');}
 finally{await Promise.all(Object.values(pools).map(p=>p.end()));}
 expect((await stats()).state).toBe('read_intent');await close();
});
