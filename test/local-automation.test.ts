import {beforeAll,afterAll,describe,it,expect,vi} from 'vitest';
import {readFile,mkdtemp,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import pg from 'pg';
import {isolatedPostgres} from './support/postgres.js';
import {largePlan,rawSource} from './support/large-batch-fixture.js';
import {LocalSpool} from '../adapters/local-spool.js';
import {spoolLimits} from '../src/large-batch/config.js';
import {APPROVED_COLUMNS} from '../src/offline/tds-writer.js';
import {exampleProfile,columnTypes,type OfflineProfile} from '../src/offline/model.js';
import {createAttemptsPipeline,createAutomatedAttemptsService,type AttemptsBinding} from '../src/integration/attempts/pipeline.js';
import {AttemptsService,type RuntimePools} from '../src/integration/attempts/service.js';
import {AttemptsRuntimeControl} from '../src/integration/attempts/runtime-control.js';
import {annotateFailure} from '../src/diagnostics/worker-failure.js';
import {PersistenceError} from '../src/persistence/types.js';
import {randomUUID} from 'node:crypto';
import {buildApp} from '../src/app.js';
import {loadConfig} from '../src/config/env.js';
const profile:OfflineProfile={...exampleProfile,columns:[...APPROVED_COLUMNS],phoneMatchKey:'normalized'};
const config=()=>loadConfig({LOG_LEVEL:'silent',STATUS_TOKEN:'synthetic-local-status-token-32-characters'});
const counts={received:1,persisted:1,duplicates:0,rejected:0,acquisitionPerformed:true};
async function until(fn:()=>boolean){const end=Date.now()+10000;while(!fn()){if(Date.now()>end)throw Error('TEST_TIMED_OUT');await new Promise(r=>setTimeout(r,20));}}

it('control uses the existing 60 second runner and stops after a sanitized failure',async()=>{
 vi.useFakeTimers();const c=new AttemptsRuntimeControl();let calls=0;
 try{c.start(async()=>{if(++calls===2)throw Error('SENSITIVE_NOT_LOGGED');return counts;});expect(c.snapshot().enabled).toBe(false);
  await vi.advanceTimersByTimeAsync(60000);expect(c.snapshot()).toMatchObject({enabled:true,completedCycles:1,inFlight:false});
  await vi.advanceTimersByTimeAsync(180000);expect(calls).toBe(2);expect(c.snapshot().state).toBe('needs_reconciliation');expect(JSON.stringify(c.snapshot())).not.toContain('SENSITIVE_NOT_LOGGED');
 }finally{await c.stop();vi.useRealTimers();}
});
it('shutdown aborts and drains a running cycle without another read',async()=>{
 vi.useFakeTimers();const c=new AttemptsRuntimeControl();let aborted=false;
 try{c.start(async signal=>{await new Promise<void>(r=>signal.addEventListener('abort',()=>{aborted=true;r();},{once:true}));});await vi.advanceTimersByTimeAsync(60000);await c.stop();expect(aborted).toBe(true);expect(c.snapshot().state).toBe('stopped');}
 finally{await c.stop();vi.useRealTimers();}
});

describe('HTTP + actual pipeline with three isolated PostgreSQL logins, no Olos',()=>{
 let db:Awaited<ReturnType<typeof isolatedPostgres>>,dir:string;
 const limits=spoolLimits({minFreeBytes:0,chunkRows:2,bufferBytes:1024});
 const plan=largePlan();
 const binding=(source:ReturnType<typeof rawSource>):AttemptsBinding=>({kind:'isolated-fixture',tlsVerified:true,recoveryValidated:true,plan:()=>plan,source:()=>source,
  destinationId:'local.automation.fixture',mappingVersion:'fixture-v1',sourceNamespace:'synthetic-local-source',profile,catalogs:{agents:new Map(),campaigns:new Map(),dispositions:new Map()}});
 const pools=():RuntimePools=>{
  const make=(user:string)=>new pg.Pool({...db.connection,user,password:'synthetic_fixture_password',options:'',query_timeout:10000});
  return {ingest:make('olos_ingest_login'),project:make('olos_project_login'),receiver:make('olos_receiver_login')};
 };
 beforeAll(async()=>{
  db=await isolatedPostgres();dir=await mkdtemp(join(tmpdir(),'olos-automation-'));
  for(const file of ['migrations/003_projection_control.sql','migrations/004_delivery_receipts.sql','migrations/005_large_batch_control.sql','permissions/projection.sql','permissions/receiver.sql','permissions/large-batch.sql'])await db.admin.query(await readFile(file,'utf8'));
  await db.admin.query(`CREATE SCHEMA raw;CREATE TABLE raw.olos_calls(${APPROVED_COLUMNS.map(c=>`"${c}" ${columnTypes[c]}${c==='internal_id_olos'?' PRIMARY KEY':''}`).join(',')})`);
  await db.admin.query('GRANT USAGE ON SCHEMA raw TO olos_export_receiver;GRANT SELECT,INSERT,UPDATE ON raw.olos_calls TO olos_export_receiver');
  for(const [login,role]of [['olos_ingest_login','olos_export_worker'],['olos_project_login','olos_export_projector'],['olos_receiver_login','olos_export_receiver']])await db.admin.query(`CREATE ROLE ${login} LOGIN PASSWORD 'synthetic_fixture_password' NOSUPERUSER;GRANT ${role} TO ${login}`);
  const c=plan.contract;await db.admin.query('INSERT INTO olos_export.source_contracts(source_table,version,approved,fields,key_fields,layout_evidence,key_evidence) VALUES($1,$2,true,$3::jsonb,$4,$5,$6)',[c.source_table,c.version,JSON.stringify(c.fields),c.key_fields,c.layout_evidence,c.key_evidence]);
  await db.admin.query("INSERT INTO raw.olos_calls(internal_id_olos,telefone,nome_agente) VALUES(1,'synthetic historical value','synthetic historical agent')");
 },60000);
 afterAll(async()=>{await db?.stop();if(dir)await rm(dir,{recursive:true,force:true});},30000);
 it('safe HTTP is alive and blocked, with actual destination counts and no invented checkpoint',async()=>{
  const runtime=new AttemptsService(pools());const app=buildApp(config(),[],runtime);
  try{await app.listen({host:'127.0.0.1',port:0});await until(()=>runtime.snapshot().schemaReady===true);
   expect((await app.inject('/health/live')).statusCode).toBe(200);expect((await app.inject('/health/ready')).json()).toMatchObject({syncEnabled:false,reason:'acquisition_blocked'});
   expect(runtime.snapshot()).toMatchObject({state:'blocked',sourceAuthorized:false,totalCalls:'1',confirmedDeliveries:'0',checkpoints:[]});
   expect((await app.inject('/status')).statusCode).toBe(401);
  }finally{await app.close();}
 });
 it('one fixture owner runs acquisition/spool/transactions/projection; migrated rows remain untouched',async()=>{
  const source=rawSource(3),spool=await LocalSpool.open(join(dir,'success'),limits);
  const rejected=pools();try{expect(()=>createAttemptsPipeline(rejected.ingest,rejected.project,rejected.receiver,spool,limits,{...binding(source),kind:'corporate'} as unknown as AttemptsBinding)).toThrow('ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED');}
  finally{await Promise.all(Object.values(rejected).map(p=>p.end()));}
  const runtime=createAutomatedAttemptsService(pools(),spool,limits,binding(source),1000);
  const second=createAutomatedAttemptsService(pools(),spool,limits,binding(rawSource(1)),1000);
  const app=buildApp(config(),[],runtime);
  try{await app.listen({host:'127.0.0.1',port:0});await until(()=>runtime.snapshot().state==='running');await second.start();expect(second.snapshot().state).toBe('blocked');await second.stop();
   await until(()=>runtime.snapshot().completedCycles===1);expect(source.calls).toBe(1);expect((await app.inject('/health/ready')).statusCode).toBe(200);
   expect(runtime.snapshot()).toMatchObject({sourceKind:'isolated-fixture',totalCalls:'3',confirmedDeliveries:'3',received:3,persisted:3});
   expect((await db.admin.query('SELECT telefone,nome_agente FROM raw.olos_calls WHERE internal_id_olos=1')).rows[0]).toEqual({telefone:'synthetic historical value',nome_agente:'synthetic historical agent'});
   expect((await db.admin.query('SELECT version FROM olos_export.checkpoints')).rows[0].version).toBe('1');
  }finally{await app.close();await second.stop();}
 },15000);
 it('one lock owner acquires serially while delivery waits; shutdown holds the lock until both flows drain',async()=>{
  let deliveries=0,activeRpc=0,maxRpc=0,calls=0,release!:()=>void,deliveryAborted=false;
  const work={initialize:vi.fn(async()=>{}),step:async(signal:AbortSignal)=>{
   deliveries++;signal.addEventListener('abort',()=>{deliveryAborted=true;},{once:true});await new Promise<void>(r=>{release=r;});return false;
  }};
  const runtime=new AttemptsService(pools(),30,async()=>{calls++;maxRpc=Math.max(maxRpc,++activeRpc);await new Promise(r=>setTimeout(r,40));activeRpc--;return counts;},'isolated-fixture',undefined,work);
  const second=new AttemptsService(pools(),30,async()=>counts,'isolated-fixture',undefined,{initialize:vi.fn(async()=>{}),step:vi.fn(async()=>false)});
  try{
   await runtime.start();await until(()=>Number(runtime.snapshot().completedCycles)>=2);
   expect(calls).toBe(2);expect(deliveries).toBe(1);expect(maxRpc).toBe(1);expect(work.initialize).toHaveBeenCalledOnce();
   let closed=false;const stopping=runtime.stop().then(()=>{closed=true;});await until(()=>deliveryAborted);expect(closed).toBe(false);
   await second.start();expect(second.snapshot().state).toBe('blocked');
   release();await stopping;expect(activeRpc).toBe(0);expect(calls).toBe(2);
  }finally{release?.();await runtime.stop();await second.stop();}
 },15000);
 it('a delivery failure cancels the active acquisition and prevents any automatic consumptive retry',async()=>{
  let started!:()=>void;const sourceStarted=new Promise<void>(r=>{started=r;});let aborted=false,calls=0;
  const runtime=new AttemptsService(pools(),10,async signal=>{
   calls++;started();await new Promise<void>(r=>signal.addEventListener('abort',()=>{aborted=true;r();},{once:true}));throw Error('INCOMPLETE');
  },'isolated-fixture',undefined,{initialize:async()=>{},step:async()=>{await sourceStarted;throw Error('STORAGE_FAILED');}});
  try{await runtime.start();await until(()=>aborted);await new Promise(r=>setTimeout(r,60));expect(calls).toBe(1);expect(runtime.snapshot().state).toBe('needs_reconciliation');}
  finally{await runtime.stop();}
 },15000);
 it('a failed acquisition cancels local delivery and initialization failure cannot arm either flow',async()=>{
  let deliveryCancelled=false,calls=0;
  const runtime=new AttemptsService(pools(),20,async()=>{calls++;throw Error('INCOMPLETE');},'isolated-fixture',undefined,{
   initialize:async()=>{},step:async signal=>{await new Promise<void>(r=>signal.addEventListener('abort',()=>{deliveryCancelled=true;r();},{once:true}));return false;}
  });
  const source=vi.fn(async()=>counts),step=vi.fn(async()=>false);
  const refused=new AttemptsService(pools(),20,source,'isolated-fixture',undefined,{initialize:async()=>{throw Error('RECOVERY_REQUIRED');},step});
  try{await runtime.start();await until(()=>deliveryCancelled);expect(calls).toBe(1);await runtime.stop();
   await refused.start();await new Promise(r=>setTimeout(r,60));expect(source).not.toHaveBeenCalled();expect(step).not.toHaveBeenCalled();expect(refused.snapshot().armed).toBe(false);
  }finally{await runtime.stop();await refused.stop();}
 },15000);
 it.each(['delivery','acquisition'] as const)('runtime retains the original %s stage and batch after the other flow is aborted',async flow=>{
  const captureId=randomUUID(),batchId=randomUUID();let begun!:()=>void;const started=new Promise<void>(r=>{begun=r;});
  const source=vi.fn(async(signal:AbortSignal)=>{
   begun();if(flow==='acquisition')throw annotateFailure(Error('INCOMPLETE'),{flow,stage:'CAPTURE',captureId});
   await new Promise<void>(r=>signal.addEventListener('abort',()=>r(),{once:true}));throw Error('STOPPED');
  });
  const runtime=new AttemptsService(pools(),20,source,'isolated-fixture',undefined,{initialize:async()=>{},step:async signal=>{
   await started;if(flow==='delivery')throw annotateFailure(new PersistenceError('LEASE_LOST'),{flow,stage:'SETTLE',captureId,batchId});
   if(!signal.aborted)await new Promise<void>(r=>signal.addEventListener('abort',()=>r(),{once:true}));throw Error('STOPPED');
  }});
  const output=vi.spyOn(process.stderr,'write').mockReturnValue(true);
  try{await runtime.start();await until(()=>runtime.snapshot().state==='needs_reconciliation');await new Promise(r=>setTimeout(r,80));
   expect(source).toHaveBeenCalledOnce();expect(runtime.snapshot().lastFailure).toMatchObject({flow,stage:flow==='delivery'?'SETTLE':'CAPTURE',code:flow==='delivery'?'LEASE_LOST':'INCOMPLETE',captureId});
   if(flow==='delivery')expect(runtime.snapshot().lastFailure).toMatchObject({batchId});
   expect(output.mock.calls.filter(([line])=>String(line).includes('worker_needs_reconciliation'))).toHaveLength(1);
   expect(runtime.snapshot().armed).toBe(false);
  }finally{await runtime.stop();output.mockRestore();}
 },15000);
 it('destination failure stops consumption; restart drains pending deliveries locally before another source call',async()=>{
  await db.admin.query("CREATE FUNCTION raw.fail_target() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'fixture destination failure'; END $$;CREATE TRIGGER fail_target BEFORE INSERT ON raw.olos_calls FOR EACH ROW EXECUTE FUNCTION raw.fail_target()");
  const source=rawSource(3),spool=await LocalSpool.open(join(dir,'destination-failure'),limits);
  const runtime=createAutomatedAttemptsService(pools(),spool,limits,binding(source),30);
  try{await runtime.start();await until(()=>runtime.snapshot().state==='needs_reconciliation');await new Promise(r=>setTimeout(r,100));expect(source.calls).toBe(1);expect(runtime.ready()).toBe(false);expect(runtime.snapshot().recoveryPending).toBe(true);}
  finally{await runtime.stop();await db.admin.query('DROP TRIGGER fail_target ON raw.olos_calls;DROP FUNCTION raw.fail_target()');}
  await db.admin.query("UPDATE olos_export.projection_outbox SET available_at=clock_timestamp() WHERE state='pending'");
  const owned=pools(),notCalled=rawSource(1),pipeline=createAttemptsPipeline(owned.ingest,owned.project,owned.receiver,spool,limits,binding(notCalled));
  try{await pipeline.cycle(new AbortController().signal);expect(notCalled.calls).toBe(0);expect((await db.admin.query("SELECT count(*)::int AS n FROM olos_export.projection_outbox WHERE state<>'confirmed'")).rows[0].n).toBe(0);}
  finally{await Promise.all(Object.values(owned).map(p=>p.end()));}
 },15000);
 it('partial response preserves a spool and a restart never creates another remote read',async()=>{
  const source=rawSource(3,{failAt:1}),spool=await LocalSpool.open(join(dir,'partial'),limits),owned=pools();
  const pipeline=createAttemptsPipeline(owned.ingest,owned.project,owned.receiver,spool,limits,binding(source));
  try{await expect(pipeline.cycle(new AbortController().signal)).rejects.toThrow();expect(source.calls).toBe(1);expect((await spool.active())?.sealed).toBe(false);
   const other=rawSource(3),restarted=createAttemptsPipeline(owned.ingest,owned.project,owned.receiver,spool,limits,binding(other));
   await expect(restarted.cycle(new AbortController().signal)).rejects.toThrow('RECOVERY_REQUIRED');expect(other.calls).toBe(0);
  }finally{await Promise.all(Object.values(owned).map(p=>p.end()));}
 },15000);
});
