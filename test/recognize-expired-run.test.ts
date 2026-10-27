import {afterAll,afterEach,beforeAll,beforeEach,expect,it,vi} from 'vitest';
import {mkdtemp,mkdir,readFile,readdir,writeFile,rm} from 'node:fs/promises';
import {join,dirname} from 'node:path';
import {tmpdir} from 'node:os';
import {randomUUID} from 'node:crypto';
import pg from 'pg';
import {isolatedPostgres} from './support/postgres.js';
import {PostgresPersistence} from '../src/persistence/store.js';
import type {RunHandle} from '../src/persistence/types.js';
import type {ExpiredRunSelection} from '../src/persistence/expired-run-selection.js';
import {AttemptsService} from '../src/integration/attempts/service.js';
import {ExactConnection} from '../src/olos/tds/connection.js';
import {IsolatedTdsSource} from '../src/olos/tds/source.js';
import {LocalSpool} from '../adapters/local-spool.js';
import {serviceConfigSchema} from '../src/integration/attempts/activation.js';
import {exampleProfile} from '../src/offline/model.js';
import {APPROVED_COLUMNS} from '../src/offline/tds-writer.js';

const state=vi.hoisted(()=>({home:'',settings:vi.fn(),pools:vi.fn()}));
vi.mock('node:os',async original=>({...await original<typeof import('node:os')>(),homedir:()=>state.home}));
vi.mock('../adapters/local-worker.js',()=>({localSettings:state.settings}));
vi.mock('../src/integration/attempts/pools.js',()=>({createLocalRuntimePools:state.pools}));
import {corporateCommand,parseCorporateArguments} from '../adapters/attempts-corporate.js';
import {loadServiceConfiguration} from '../adapters/attempts-service.js';

let db:Awaited<ReturnType<typeof isolatedPostgres>>,pool:pg.Pool,store:PostgresPersistence,run:RunHandle;
let base:string,root:string,configFile:string,revocationFile:string,cancel:()=>void,output:ReturnType<typeof vi.spyOn>;
const plan={table:'AttemptsRawData' as const,contractVersion:'fixture-v1',readType:'investigation' as const,exportDate:'2026-10-27',pkRange:null};
const selection=():ExpiredRunSelection=>({runId:run.runId,source:'AttemptsRawData',expectedGeneration:run.fencingToken});
const originalRecover=PostgresPersistence.prototype.recoverExpired;
const save=async(path:string,value:unknown)=>{await mkdir(dirname(path),{recursive:true,mode:0o700});await writeFile(path,JSON.stringify(value),{mode:0o600});};
async function files(path=state.home):Promise<Record<string,string>>{
  const result:Record<string,string>={};for(const e of await readdir(path,{withFileTypes:true})){
    const p=join(path,e.name);if(e.isDirectory())Object.assign(result,await files(p));else result[p]=await readFile(p,'base64');
  }return result;
}
const tables=['source_locks','runs','batches','checkpoints','recovery_cases','recovery_attempts','audit_events','sync_errors','attempts_raw_data','projection_batches','projection_outbox','delivery_receipts'] as const;
async function snapshot(){const result:Record<string,string[]>={};for(const table of tables)result[table]=(await db.admin.query(`SELECT * FROM olos_export.${table}`)).rows.map(r=>JSON.stringify(r)).sort();return result;}
async function expire(){await db.admin.query("UPDATE olos_export.source_locks SET lease_until=clock_timestamp()-interval '1 minute' WHERE owner_run_id=$1",[run.runId]);}
const args=()=>['recognize-expired-run','--config',configFile,'--work-dir',root,'--run-id',run.runId,'--source','AttemptsRawData','--expected-generation',run.fencingToken,'--confirm','AUTHORIZE_EXPIRED_RUN_RECOGNITION'];
async function command(){await corporateCommand(args(),{SYNC_ENABLED:'false'});return JSON.parse(String(output.mock.calls.at(-1)?.[0]));}

beforeAll(async()=>{
  db=await isolatedPostgres();
  for(const file of ['migrations/003_projection_control.sql','permissions/projection.sql','migrations/004_delivery_receipts.sql'])
    await db.admin.query(await readFile(new URL('../'+file,import.meta.url),'utf8'));
},120000);
afterAll(async()=>{await db?.stop();});
beforeEach(async()=>{
  // Only the ephemeral database created above; never environment PostgreSQL credentials.
  await db.admin.query('TRUNCATE olos_export.source_contracts,olos_export.streams,olos_export.source_locks CASCADE');
  for(const source of ['AttemptsRawData','InboundRawData'])await db.admin.query(`INSERT INTO olos_export.source_contracts(source_table,version,approved,fields,key_fields,layout_evidence,key_evidence)
    VALUES($1,'fixture-v1',true,'{"FixtureId":{"type":"int64","nullable":false}}',ARRAY['FixtureId'],'synthetic','synthetic')`,[source]);
  await db.admin.query("INSERT INTO olos_export.source_locks(source_table,generation) VALUES('AttemptsRawData',7),('InboundRawData',0)");
  pool=db.worker();store=new PostgresPersistence(pool);run=await store.startRun(plan,new AbortController().signal);await expire();
  state.home=await mkdtemp(join(tmpdir(),'olos-selective-expiry-'));base=join(state.home,'.local/state/olos-worker/corporate-attempts');root=join(base,'service-run');configFile=join(state.home,'config.json');revocationFile=join(state.home,'revoked.json');
  const config=serviceConfigSchema.parse({version:'olos-service/v2',workerId:'synthetic-worker',endpoint:{server:'synthetic.invalid',port:1433,database:'ExportDataV3',user:'OEC',password:'SYNTHETIC'},
    postgres:{host:db.connection.host,port:db.connection.port,database:'olos_worker'},pinApprovalFile:join(state.home,'unused-pin.json'),approvalFile:join(state.home,'approval.json'),
    profile:{...exampleProfile,phoneMatchKey:'normalized',columns:[...APPROVED_COLUMNS]},campaignFile:join(state.home,'campaigns.json'),dispositionFile:join(state.home,'dispositions.json')});
  await save(configFile,config);await save(config.campaignFile,[]);await save(config.dispositionFile,[]);
  const loaded=await loadServiceConfiguration(configFile);
  await save(config.approvalFile,{scope:'OLOS_ATTEMPTS_SERVICE',approvalId:'fixture',workerId:config.workerId,configurationDigest:loaded.configurationDigest,
    procedures:['ReadDataCurrentDay','ReadDataPreviousDays'],table:'AttemptsRawData',destination:'olos_worker.raw.olos_calls',operator:'test',reference:'synthetic',issuedAt:'2020-01-01T00:00:00.000Z',
    reviewPolicy:'REVOKE_BY_PRIVATE_FILE',revocationFile,bootstrapRecoveryEvidence:'synthetic fixture only'});
  // Uninterpreted fixture bytes: discovery/replay would fail. Neither capture may be touched.
  await save(join(root,'cases','unrelated','diagnostic.json'),{invalid:'must not be inspected'});
  await save(join(root,'cases','retained','import','manifest.json'),{sealed:true,state:'uncertain',rows:200000});
  await writeFile(join(root,'cases','retained','import','records.ready'),'retained synthetic spool bytes',{mode:0o600});
  state.settings.mockReset().mockResolvedValue({postgres:config.postgres,passwords:{}});
  state.pools.mockReset().mockImplementation((_passwords:unknown,onError:()=>void)=>{cancel=onError;return {ingest:db.worker(),project:new pg.Pool(db.connection),receiver:new pg.Pool(db.connection)};});
  vi.spyOn(AttemptsService.prototype,'probe').mockResolvedValue(); // Schema probe has its own regression suite.
  vi.spyOn(AttemptsService.prototype,'start').mockImplementation(()=>{throw Error('SCHEDULER_FORBIDDEN');});
  vi.spyOn(ExactConnection.prototype,'connect').mockImplementation(()=>{throw Error('OLOS_FORBIDDEN');});
  vi.spyOn(IsolatedTdsSource.prototype,'call').mockImplementation(()=>{throw Error('RPC_FORBIDDEN');});
  vi.spyOn(LocalSpool,'open').mockImplementation(()=>{throw Error('SPOOL_REPLAY_FORBIDDEN');});
  vi.spyOn(PostgresPersistence.prototype,'recoverExpired').mockImplementation(()=>{throw Error('GLOBAL_RECOVERY_FORBIDDEN');});
  output=vi.spyOn(process.stdout,'write').mockReturnValue(true);
});
afterEach(async()=>{
  for(const mock of [AttemptsService.prototype.start,ExactConnection.prototype.connect,IsolatedTdsSource.prototype.call,LocalSpool.open,PostgresPersistence.prototype.recoverExpired])expect(mock).not.toHaveBeenCalled();
  vi.restoreAllMocks();await pool?.end();await rm(state.home,{recursive:true,force:true});
});

it('recognizes exactly run generation 8, creates one case/audit, and requires reconciliation before any new acquisition',async()=>{
  const before=await files(),result=await store.recognizeExpiredRun(selection());
  expect(result).toMatchObject({...selection(),runState:'needs_reconciliation',releasedGeneration:'9',recoveryState:'open',recoveryReason:'expired_read_intent',alreadyRecognized:false});
  expect((await db.admin.query('SELECT state,fencing_token,received_count,persisted_count FROM olos_export.runs WHERE id=$1',[run.runId])).rows[0]).toEqual({state:'needs_reconciliation',fencing_token:'8',received_count:'0',persisted_count:'0'});
  expect((await db.admin.query("SELECT generation,owner_run_id,lease_until FROM olos_export.source_locks WHERE source_table='AttemptsRawData'")).rows[0]).toEqual({generation:'9',owner_run_id:null,lease_until:null});
  expect((await db.admin.query('SELECT id,run_id,state,reason FROM olos_export.recovery_cases')).rows).toEqual([{id:result.recoveryCaseId,run_id:run.runId,state:'open',reason:'expired_read_intent'}]);
  await expect(store.startRun(plan,new AbortController().signal)).rejects.toMatchObject({code:'RECOVERY_REQUIRED'});
  await expect(store.renew(run)).rejects.toMatchObject({code:'LEASE_LOST'});
  await expect(store.checkpoint(run)).rejects.toMatchObject({code:'LEASE_LOST'});
  expect(await files()).toEqual(before);
});
it('is idempotent after restart without duplicate cases/audit or another generation increment',async()=>{
  const first=await store.recognizeExpiredRun(selection()),before=await snapshot();
  expect(await new PostgresPersistence(pool).recognizeExpiredRun(selection())).toEqual({...first,alreadyRecognized:true});
  expect(await snapshot()).toEqual(before);
});
it('recognizes a result produced by the existing recoverExpired protocol',async()=>{
  const [caseId]=await originalRecover.call(store),before=await snapshot();
  expect(await store.recognizeExpiredRun(selection())).toMatchObject({alreadyRecognized:true,recoveryCaseId:caseId,releasedGeneration:'9'});
  expect(await snapshot()).toEqual(before);
});
it('does not recover another expired source or change its owner/generation',async()=>{
  const other=await store.startRun({...plan,table:'InboundRawData'},new AbortController().signal);
  await db.admin.query("UPDATE olos_export.source_locks SET lease_until=clock_timestamp()-interval '1 minute' WHERE owner_run_id=$1",[other.runId]);
  const readOther=async()=>({run:(await db.admin.query('SELECT * FROM olos_export.runs WHERE id=$1',[other.runId])).rows,lock:(await db.admin.query("SELECT * FROM olos_export.source_locks WHERE source_table='InboundRawData'")).rows});
  const before=await readOther();await store.recognizeExpiredRun(selection());expect(await readOther()).toEqual(before);
  expect((await db.admin.query('SELECT run_id FROM olos_export.recovery_cases')).rows).toEqual([{run_id:run.runId}]);
});
it.each(['read_observed','committed'] as const)('preserves %s batches, checkpoints, payloads and pending delivery exactly',async batchState=>{
  const batchId=randomUUID(),deliveryId=randomUUID();
  await db.admin.query(`INSERT INTO olos_export.batches(id,run_id,stream_id,source_table,digest,expected_version,state,received_count,persisted_count,committed_at)
    VALUES($1,$2,$3,'AttemptsRawData',repeat('a',64),0,$4,1,$5,$6)`,[batchId,run.runId,run.streamId,batchState,batchState==='committed'?1:0,batchState==='committed'?new Date():null]);
  if(batchState==='committed'){
    await db.admin.query('UPDATE olos_export.checkpoints SET version=1,cursor_value=$2,last_batch_id=$3 WHERE stream_id=$1',[run.streamId,'fixture:1',batchId]);
    await db.admin.query("INSERT INTO olos_export.attempts_raw_data(source_key,contract_version,payload,last_batch_id) VALUES('fixture','fixture-v1','{\"FixtureId\":\"1\"}',$1)",[batchId]);
    await db.admin.query(`INSERT INTO olos_export.projection_batches(batch_id,destination_id,source_table,source_namespace,source_contract_version,field_definitions,destination_version,mapping_version,config_digest,snapshot_digest,row_count)
      VALUES($1,'fixture','AttemptsRawData','fixture','fixture-v1','{}','fixture','fixture',repeat('a',64),repeat('a',64),1)`,[batchId]);
    await db.admin.query("INSERT INTO olos_export.projection_outbox(delivery_id,batch_id,destination_id,source_key,payload) VALUES($1,$2,'fixture','fixture','{}')",[deliveryId,batchId]);
  }
  const before=await snapshot();await store.recognizeExpiredRun(selection());const after=await snapshot();
  for(const table of tables.filter(t=>!['source_locks','runs','recovery_cases','audit_events'].includes(t)))expect(after[table]).toEqual(before[table]);
});
it.each(['active-lease','owner','lock-generation','run-generation','persisting','succeeded','failed','unproven-reconciliation','finished','case-exists','missing-run','wrong-source'] as const)('refuses unexpected %s with zero writes',async change=>{
  let target=selection();
  if(change==='active-lease')await db.admin.query("UPDATE olos_export.source_locks SET lease_until=clock_timestamp()+interval '1 hour' WHERE owner_run_id=$1",[run.runId]);
  if(change==='owner')await db.admin.query("UPDATE olos_export.source_locks SET owner_run_id=NULL,lease_until=NULL WHERE source_table='AttemptsRawData'");
  if(change==='lock-generation')await db.admin.query("UPDATE olos_export.source_locks SET generation=9 WHERE source_table='AttemptsRawData'");
  if(change==='run-generation')target={...target,expectedGeneration:'7'};
  if(['persisting','succeeded','failed','unproven-reconciliation'].includes(change))await db.admin.query('UPDATE olos_export.runs SET state=$2 WHERE id=$1',[run.runId,change==='unproven-reconciliation'?'needs_reconciliation':change]);
  if(change==='finished')await db.admin.query('UPDATE olos_export.runs SET finished_at=clock_timestamp() WHERE id=$1',[run.runId]);
  if(change==='case-exists')await db.admin.query("INSERT INTO olos_export.recovery_cases(id,run_id,reason) VALUES($1,$2,'expired_read_intent')",[randomUUID(),run.runId]);
  if(change==='missing-run')target={...target,runId:randomUUID()};
  if(change==='wrong-source'){const other=await store.startRun({...plan,table:'InboundRawData'},new AbortController().signal);target={...target,runId:other.runId,expectedGeneration:other.fencingToken};}
  const before=await snapshot();await expect(store.recognizeExpiredRun(target)).rejects.toMatchObject({code:'RECOVERY_REQUIRED'});expect(await snapshot()).toEqual(before);
});
it.each(['resolved','reason','generation','missing-audit','duplicate-audit','run-state','new-owner'] as const)('refuses idempotent retry after incompatible %s change',async change=>{
  const result=await store.recognizeExpiredRun(selection());
  if(change==='resolved')await db.admin.query("UPDATE olos_export.recovery_cases SET state='resolved',resolved_at=clock_timestamp() WHERE id=$1",[result.recoveryCaseId]);
  if(change==='reason')await db.admin.query("UPDATE olos_export.recovery_cases SET reason='persistence_unknown' WHERE id=$1",[result.recoveryCaseId]);
  if(change==='generation')await db.admin.query("UPDATE olos_export.source_locks SET generation=10 WHERE source_table='AttemptsRawData'");
  if(change==='missing-audit')await db.admin.query("DELETE FROM olos_export.audit_events WHERE run_id=$1",[run.runId]);
  if(change==='duplicate-audit')await db.admin.query("INSERT INTO olos_export.audit_events(id,run_id,event,received_count,persisted_count,duplicate_count) VALUES($1,$2,'reconciliation_required',0,0,0)",[randomUUID(),run.runId]);
  if(change==='run-state')await db.admin.query("UPDATE olos_export.runs SET state='failed' WHERE id=$1",[run.runId]);
  if(change==='new-owner'){
    const id=randomUUID();await db.admin.query("INSERT INTO olos_export.runs(id,stream_id,source_table,contract_version,fencing_token,state) VALUES($1,$2,'AttemptsRawData','fixture-v1',10,'read_intent')",[id,run.streamId]);
    await db.admin.query("UPDATE olos_export.source_locks SET generation=10,owner_run_id=$1,lease_until=clock_timestamp()+interval '1 minute' WHERE source_table='AttemptsRawData'",[id]);
  }
  const before=await snapshot();await expect(store.recognizeExpiredRun(selection())).rejects.toMatchObject({code:'RECOVERY_REQUIRED'});expect(await snapshot()).toEqual(before);
});
it.each(['source','run'] as const)('refuses a concurrent %s lock and then succeeds without partial writes',async resource=>{
  const c=await db.admin.connect();try{
    await c.query('BEGIN');await c.query(resource==='source'?"SELECT 1 FROM olos_export.source_locks WHERE source_table='AttemptsRawData' FOR UPDATE":'SELECT 1 FROM olos_export.runs WHERE id=$1 FOR UPDATE',resource==='source'?[]:[run.runId]);
    const before=await snapshot();await expect(store.recognizeExpiredRun(selection())).rejects.toMatchObject({code:'BUSY'});expect(await snapshot()).toEqual(before);
  }finally{await c.query('ROLLBACK');c.release();}
  await expect(store.recognizeExpiredRun(selection())).resolves.toMatchObject({alreadyRecognized:false});
});
it.each(['selective','global'] as const)('serializes two competing recoveries (%s)',async kind=>{
  const results=await Promise.allSettled([store.recognizeExpiredRun(selection()),kind==='selective'?new PostgresPersistence(pool).recognizeExpiredRun(selection()):originalRecover.call(new PostgresPersistence(pool))]);
  expect(results.some(r=>r.status==='fulfilled')).toBe(true);
  for(const r of results)if(r.status==='rejected')expect(r.reason.code).toBe('BUSY');
  expect((await db.admin.query('SELECT * FROM olos_export.recovery_cases')).rowCount).toBe(1);
  expect((await db.admin.query('SELECT * FROM olos_export.audit_events')).rowCount).toBe(1);
  expect(await store.recognizeExpiredRun(selection())).toMatchObject({alreadyRecognized:true,releasedGeneration:'9'});
});
function fault(point:string,after=false){let hit=false;return new PostgresPersistence({connect:async()=>{
  const c=await pool.connect();return new Proxy(c,{get(target,key){
    if(key==='query')return async(sql:string,...args:unknown[])=>{
      const fail=!hit && sql.startsWith(point);if(fail){hit=true;if(!after)throw Error('synthetic fault');}
      const result=await (target.query.bind(target) as (...args:unknown[])=>Promise<unknown>)(sql,...args);
      if(fail)throw Error('synthetic lost acknowledgment');return result;
    };const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;
  }});
}});}
it.each(['UPDATE olos_export.runs','INSERT INTO olos_export.recovery_cases','INSERT INTO olos_export.audit_events','UPDATE olos_export.source_locks'])('rolls back everything after %s failure',async point=>{
  const before=await snapshot();await expect(fault(point,true).recognizeExpiredRun(selection())).rejects.toMatchObject({code:'PERSISTENCE_FAILED'});expect(await snapshot()).toEqual(before);
});
it.each([false,true])('resolves COMMIT_UNKNOWN (commit happened=%s) by repeating exact identity/generation',async committed=>{
  await expect(fault('COMMIT',committed).recognizeExpiredRun(selection())).rejects.toMatchObject({code:'COMMIT_UNKNOWN',outcome:'unknown'});
  expect(await store.recognizeExpiredRun(selection())).toMatchObject({alreadyRecognized:committed,releasedGeneration:'9'});
  expect((await db.admin.query('SELECT * FROM olos_export.audit_events')).rowCount).toBe(1);
});
it('rechecks authorization just before commit and rolls back a revocation',async()=>{
  const before=await snapshot();let checks=0;
  await expect(store.recognizeExpiredRun(selection(),()=>{if(++checks===3)throw Error('REVOKED');})).rejects.toThrow();expect(await snapshot()).toEqual(before);
});
it('CLI uses explicit authorization, instance lock and scoped selection without visiting any capture or spool',async()=>{
  const before=await files();expect(await command()).toMatchObject({event:'expired_run_recognized',...selection(),releasedGeneration:'9',alreadyRecognized:false,acquisitionPerformed:false,imported:0,schedulerStarted:false,retryRemote:false});
  expect(await command()).toMatchObject({alreadyRecognized:true});expect(await files()).toEqual(before);expect(AttemptsService.prototype.probe).toHaveBeenCalledTimes(2);
});
it('CLI cannot execute while another worker owns the instance lock',async()=>{
  const c=await db.admin.connect();try{
    await c.query('SELECT pg_advisory_lock(740219,1)');const before=await snapshot();await expect(command()).rejects.toThrow('INSTANCE_BUSY');expect(await snapshot()).toEqual(before);
  }finally{await c.query('SELECT pg_advisory_unlock_all()');c.release();}
});
it.each(['revoked','probe','cancelled','configuration'] as const)('CLI refuses %s before mutation',async failure=>{
  if(failure==='revoked')await save(revocationFile,{});
  if(failure==='probe')vi.mocked(AttemptsService.prototype.probe).mockRejectedValueOnce(Error('POSTGRES_RUNTIME_NOT_READY'));
  if(failure==='cancelled')vi.mocked(AttemptsService.prototype.probe).mockImplementationOnce(async()=>{cancel();});
  if(failure==='configuration')vi.mocked(AttemptsService.prototype.probe).mockImplementationOnce(async()=>{await save(join(state.home,'campaigns.json'),[{CampaignId:'1',Description:'changed'}]);});
  const before=await snapshot();await expect(command()).rejects.toThrow();expect(await snapshot()).toEqual(before);
});
it('CLI rejects missing/ambiguous identity, wrong source, overflow and unrelated flags',()=>{
  for(const flag of ['--run-id','--source','--expected-generation','--confirm']){
    const a=args(),at=a.indexOf(flag);a.splice(at,2);expect(()=>parseCorporateArguments(a,{SYNC_ENABLED:'false'})).toThrow();
  }
  for(const value of ['0','-1','01','1.0','1e1','x','9223372036854775807','9223372036854775808']){
    const a=args();a[a.indexOf('--expected-generation')+1]=value;expect(()=>parseCorporateArguments(a,{SYNC_ENABLED:'false'})).toThrow('CORPORATE_COMMAND_REJECTED');
  }
  for(const [flag,value] of [['--run-id','4aca20bf'],['--source','InboundRawData'],['--confirm','LOCAL_RECOVERY_ONLY']]){
    const a=args();a[a.indexOf(flag!)+1]=value!;expect(()=>parseCorporateArguments(a,{SYNC_ENABLED:'false'})).toThrow();
  }
  for(const extra of [['--interval-ms','120000'],['--capture-id',randomUUID()],['--run-id',run.runId],['--queue-max-captures','8']])expect(()=>parseCorporateArguments([...args(),...extra],{SYNC_ENABLED:'false'})).toThrow();
  expect(()=>parseCorporateArguments(args(),{SYNC_ENABLED:'true'})).toThrow();
  expect(()=>parseCorporateArguments(['status','--run-id',run.runId],{SYNC_ENABLED:'false'})).toThrow();
  expect(state.pools).not.toHaveBeenCalled();
});
