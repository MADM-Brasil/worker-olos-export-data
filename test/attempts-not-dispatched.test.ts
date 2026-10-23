import {beforeAll,afterAll,beforeEach,afterEach,it,expect,vi} from 'vitest';
import {mkdtemp,mkdir,writeFile,readFile,readdir,rm} from 'node:fs/promises';
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

const state=vi.hoisted(()=>({home:'',settings:vi.fn(),pools:vi.fn()}));
vi.mock('node:os',async original=>({...await original<typeof import('node:os')>(),homedir:()=>state.home}));
vi.mock('../adapters/local-worker.js',()=>({localSettings:state.settings}));
vi.mock('../src/integration/attempts/pools.js',()=>({createLocalRuntimePools:state.pools}));
import {corporateCommand,parseCorporateArguments} from '../adapters/attempts-corporate.js';
import {captureIntentSchema,loadServiceConfiguration,operationalServiceStatus} from '../adapters/attempts-service.js';

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
beforeAll(async()=>{fixture=await isolatedPostgres();},120000);
afterAll(async()=>{await fixture?.stop();});
beforeEach(async()=>{
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
 state.pools.mockReset().mockImplementation((_passwords:unknown,onError:()=>void)=>{cancel=onError;return {ingest:new Pool(fixture.connection),project:new Pool(fixture.connection),receiver:new Pool(fixture.connection)};});
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
 await save(join(dir,'remote-state.json'),{state:'REMOTE_OUTCOME_UNKNOWN'});
 await save(join(root,`attempt-${id}.json`),captureIntentSchema.parse({attemptId:id,procedure:'ReadDataCurrentDay',configurationDigest:loaded.configurationDigest,workDir:root,createdAt:new Date().toISOString(),controlDate:'2026-10-23',operator:'test',approvalReference:'synthetic',pinApprovalId:'unused',contractReviewDigest:null,plan:null,state:'PREPARED'}));
 await save(join(base,'remote-pending.json'),{attemptId:id,workDir:root,configurationDigest:loaded.configurationDigest});
});
afterEach(async()=>{
 expect(ExactConnection.prototype.connect).not.toHaveBeenCalled();expect(IsolatedTdsSource.prototype.call).not.toHaveBeenCalled();expect(AttemptsService.prototype.start).not.toHaveBeenCalled();
 vi.restoreAllMocks();await rm(state.home,{recursive:true,force:true});
});
const args=()=>['close-not-dispatched','--config',configFile,'--work-dir',root,'--attempt-id',id];
async function close(){await corporateCommand(args(),{SYNC_ENABLED:'false'});return JSON.parse(String(output.mock.calls.at(-1)?.[0]));}
async function change(path:string,patch:Record<string,unknown>){await save(path,{...await json(path),...patch});}
async function changeDiagnostic(patch:Record<string,unknown>){await change(join(dir,'diagnostic.json'),patch);await change(join(dir,'not-dispatched.json'),{diagnosticDigest:digest(await json(join(dir,'diagnostic.json')))});}
async function refused(code?:string){const before=await snapshot();await expect(close()).rejects.toThrow(code);expect(await snapshot()).toEqual(before);}

it.each(['ReadDataCurrentDay','ReadDataPreviousDays'])('closes %s in BOOTSTRAP, preserves evidence bytes and leaves acquisition disabled',async procedure=>{
 await change(join(root,`attempt-${id}.json`),{procedure});
 const before=await snapshot(),pending=join(base,'remote-pending.json');
 expect(await close()).toEqual({event:'service_not_dispatched_closed',attemptId:id,alreadyClosed:false,acquisitionPerformed:false,schedulerStarted:false,retryRemote:false});
 const after=await snapshot();expect(after[join(dir,'remote-pending.not-dispatched.json')]).toBe(before[pending]);
 expect(after[join(dir,'not-dispatched-closed.json')]).toBe(before[join(dir,'not-dispatched.json')]);
 delete before[pending];delete after[join(dir,'remote-pending.not-dispatched.json')];delete after[join(dir,'not-dispatched-closed.json')];expect(after).toEqual(before);
 expect(AttemptsService.prototype.probe).toHaveBeenCalledOnce();
 expect(await operationalServiceStatus({base,root,configFile,intervalMs:120000})).toMatchObject({state:'BOOTSTRAP',pendingBatch:null});
});
it('repeated closure validates the same archived evidence without changing files',async()=>{
 await close();const before=await snapshot();expect(await close()).toMatchObject({alreadyClosed:true});expect(await snapshot()).toEqual(before);
 await changeDiagnostic({rpcDispatchState:'DRIVER_HANDOFF_STARTED'});await refused('RECOVERY_REQUIRED');
});
it('repeating A cannot close a new pending B',async()=>{
 await close();await save(join(base,'remote-pending.json'),{attemptId:randomUUID(),workDir:root,configurationDigest:loaded.configurationDigest});await refused('RECOVERY_REQUIRED');
});
it('resumes interruption after durable closure record, preserving that record',async()=>{
 const proof=await json(join(dir,'not-dispatched.json'));await save(join(dir,'not-dispatched-closed.json'),proof);
 expect(await close()).toMatchObject({alreadyClosed:false});expect(await json(join(dir,'not-dispatched-closed.json'))).toEqual(proof);
});
it.each(['digest','proof-id','diagnostic-id','intent-id','marker-id','marker-root','marker-digest','missing-proof','partial-bytes','sealed-response','descriptor-bytes','archive-conflict','closed-tampered'])('refuses %s with zero evidence changes',async kind=>{
 if(kind==='digest')await change(join(dir,'diagnostic.json'),{errorCode:'TIMEOUT'});
 if(kind==='proof-id')await change(join(dir,'not-dispatched.json'),{attemptId:randomUUID()});
 if(kind==='diagnostic-id')await changeDiagnostic({attemptId:randomUUID()});
 if(kind==='intent-id')await change(join(root,`attempt-${id}.json`),{attemptId:randomUUID()});
 if(kind==='marker-id')await change(join(base,'remote-pending.json'),{attemptId:randomUUID()});
 if(kind==='marker-root')await change(join(base,'remote-pending.json'),{workDir:dirname(root)});
 if(kind==='marker-digest')await change(join(base,'remote-pending.json'),{configurationDigest:'0'.repeat(64)});
 if(kind==='missing-proof')await rm(join(dir,'not-dispatched.json'));
 if(kind==='partial-bytes')await writeFile(join(captureDir,'response.partial'),Buffer.from([4,1]));
 if(kind==='sealed-response')await writeFile(join(captureDir,'response.ready'),Buffer.alloc(0),{mode:0o600});
 if(kind==='descriptor-bytes')await change(join(captureDir,'response.json'),{bytes:2});
 if(kind==='archive-conflict')await save(join(dir,'remote-pending.not-dispatched.json'),{attemptId:'preserve'});
 if(kind==='closed-tampered')await save(join(dir,'not-dispatched-closed.json'),{attemptId:'preserve'});
 await refused();
});
it.each([
 {rpcDispatchState:'UNKNOWN'}, {rpcDispatchState:'DRIVER_HANDOFF_STARTED'}, {remoteExecutionOutcome:'UNKNOWN'},
 {observedStages:['RPC_HANDOFF_STARTED']}, {observedStages:['RESPONSE_CAPTURE_STARTED']}, {observedStages:['RESPONSE_VALIDATED']}, {observedStages:null}
])('refuses unsafe diagnostics even with matching digest: %j',async patch=>{await changeDiagnostic(patch);await refused('RECOVERY_REQUIRED');});
it('requires the existing advisory lock and releases it after success',async()=>{
 const lock=await fixture.admin.connect();try{
  await lock.query('SELECT pg_advisory_lock(740219,1)');await refused('INSTANCE_BUSY');expect(AttemptsService.prototype.probe).not.toHaveBeenCalled();
  await lock.query('SELECT pg_advisory_unlock(740219,1)');await close();
  expect((await lock.query('SELECT pg_try_advisory_lock(740219,1) AS acquired')).rows[0].acquired).toBe(true);
 }finally{await lock.query('SELECT pg_advisory_unlock_all()');lock.release();}
});
it('retains existing PostgreSQL destination and runtime readiness checks',async()=>{
 state.settings.mockResolvedValueOnce({postgres:{...loaded.config.postgres,port:1},passwords:{}});await refused('POSTGRES_DESTINATION_CHANGED');expect(state.pools).not.toHaveBeenCalled();
 vi.mocked(AttemptsService.prototype.probe).mockRejectedValueOnce(Error('POSTGRES_RUNTIME_NOT_READY'));await refused('POSTGRES_RUNTIME_NOT_READY');
});
it('permits local closure despite runtime recoveryPending without starting recovery or acquisition',async()=>{
 const original=AttemptsService.prototype.snapshot;
 vi.spyOn(AttemptsService.prototype,'snapshot').mockImplementation(function(this:AttemptsService){return {...original.call(this),recoveryPending:true};});
 expect(await close()).toMatchObject({acquisitionPerformed:false,alreadyClosed:false});
});
it('loss of the instance lock aborts before any closure write',async()=>{
 vi.mocked(AttemptsService.prototype.probe).mockImplementationOnce(async()=>{cancel();});await refused('ABORTED');
});
it('configuration changes during the probe are revalidated before mutation',async()=>{
 const marker=await readFile(join(base,'remote-pending.json'));
 vi.mocked(AttemptsService.prototype.probe).mockImplementationOnce(async()=>{await save(loaded.config.campaignFile,[{CampaignId:'1',Description:'changed'}]);});
 await expect(close()).rejects.toThrow('SERVICE_CONFIGURATION_CHANGED');
 expect(await readFile(join(base,'remote-pending.json'))).toEqual(marker);expect(await readdir(dir)).not.toContain('not-dispatched-closed.json');
});
it('revocation cannot be bypassed by local recovery',async()=>{await save(join(state.home,'revoked.json'),{});await refused('SERVICE_AUTHORIZATION_REJECTED');expect(state.pools).not.toHaveBeenCalled();});
it('CLI requires an explicit UUID and refuses scheduler flags or weakened environments',()=>{
 for(const argv of [['close-not-dispatched'],['close-not-dispatched','--attempt-id','../unsafe'],[...args(),'--interval-ms','120000'],['service-status','--attempt-id',id]])
  expect(()=>parseCorporateArguments(argv,{SYNC_ENABLED:'false'})).toThrow('CORPORATE_COMMAND_REJECTED');
 expect(()=>parseCorporateArguments(args(),{SYNC_ENABLED:'true'})).toThrow('CORPORATE_COMMAND_REJECTED');
 expect(()=>parseCorporateArguments(args(),{NODE_TLS_REJECT_UNAUTHORIZED:'0'})).toThrow('CORPORATE_COMMAND_REJECTED');
 expect(parseCorporateArguments([...args(),'--confirm','LOCAL_RECOVERY_ONLY'],{SYNC_ENABLED:'false'}).action).toBe('close-not-dispatched');
});
