import {afterEach,expect,it,vi} from 'vitest';
import type {Pool} from 'pg';
import {AttemptsService} from '../src/integration/attempts/service.js';
import {AttemptsRuntimeControl} from '../src/integration/attempts/runtime-control.js';
import {APPROVED_COLUMNS} from '../src/offline/tds-writer.js';
import {failureDetails} from '../src/diagnostics/worker-failure.js';

const interval=10*60*1000;
const metrics=/SELECT count\(|SELECT coalesce\(|SELECT c.version/;
function fixture(){
  const state={failure:undefined as {match:RegExp;error:Error}|undefined,denied:false,badColumnPrivilege:false,badRole:false,superuser:false,badSchema:false,badPk:false,
    calls:'12',pendingRuns:false,pendingOutbox:false,wait:undefined as Promise<void>|undefined};
  const make=(login:string)=>{
    const query=vi.fn(async(sql:string)=>{
      if(state.failure?.match.test(sql))throw state.failure.error;
      if(sql.startsWith('SELECT count(') && state.wait)await state.wait;
      if(sql.includes('session_user'))return {rows:[{login:state.badRole?'unexpected':login,superuser:state.superuser?'on':'off'}]};
      if(sql.includes('has_column_privilege'))return {rows:[{allowed:!state.badColumnPrivilege}]};
      if(sql.includes('has_table_privilege'))return {rows:[{allowed:!state.denied}]};
      if(sql.includes('information_schema.columns'))return {rows:state.badSchema?[]:APPROVED_COLUMNS.map((column_name,i)=>({column_name,is_nullable:i===0?'NO':'YES',is_generated:'NEVER',is_identity:'NO',udt_name:i===0 || i===15?'int8':i===2?'timestamp':i===13?'int4':'text',datetime_precision:6}))};
      if(sql.includes('pg_constraint'))return {rows:[{keys:state.badPk?[]:['internal_id_olos']}]};
      if(sql.includes('SELECT EXISTS'))return {rows:[{pending:sql.includes('projection_outbox')?state.pendingOutbox:state.pendingRuns}]};
      if(sql.startsWith('SELECT count('))return {rows:[sql.includes('raw.olos_calls')?{totalCalls:state.calls,withCpf:'2',withEmail:'3',withAgentName:'4'}:{pendingDeliveries:'0',confirmedDeliveries:'9'}]};
      if(sql.startsWith('SELECT coalesce('))return {rows:[{received:'12',persisted:'12',duplicates:'0',unresolved:'0'}]};
      if(sql.includes('SELECT c.version'))return {rows:[{version:'1',last_batch_id:'fixture-batch'}]};
      if(sql.includes('pg_try_advisory_lock'))return {rows:[{acquired:true}]};
      if(['BEGIN READ ONLY','COMMIT','ROLLBACK'].includes(sql) || sql.includes('pg_advisory_unlock'))return {rows:[]};
      throw Error(`Unexpected SQL: ${sql}`);
    });
    const client={query,release:vi.fn(),on:vi.fn()};
    return {query,connect:vi.fn(async()=>client),end:vi.fn(async()=>{})};
  };
  const mocks={ingest:make('olos_ingest_login'),project:make('olos_project_login'),receiver:make('olos_receiver_login')};
  const pools=Object.fromEntries(Object.entries(mocks).map(([k,v])=>[k,v as unknown as Pool])) as {ingest:Pool;project:Pool;receiver:Pool};
  const source=vi.fn(async()=>({received:0,persisted:0,duplicates:0,rejected:0}));
  const service=new AttemptsService(pools,120000,source);
  const metricCalls=()=>Object.values(mocks).flatMap(m=>m.query.mock.calls).filter(([sql])=>metrics.test(sql));
  return {state,mocks,service,source,metricCalls};
}
async function settled(service:AttemptsService){await vi.waitFor(()=>expect(service.snapshot().statisticsStatus).not.toBe('refreshing'));}
afterEach(()=>{vi.restoreAllMocks();vi.useRealTimers();});

it.each([
  [/SELECT count\(.*projection_outbox/,Error('Query read timeout'),'PG_QUERY_TIMEOUT'],
  [/SELECT count\(.*projection_outbox/,Object.assign(Error('private SQL'),{code:'57014'}),'57014'],
  [/SELECT count\(.*raw.olos_calls/,Error('Query read timeout'),'PG_QUERY_TIMEOUT'],
  [/SELECT coalesce\(/,Error('Query read timeout'),'PG_QUERY_TIMEOUT'],
  [/SELECT c.version/,Error('private driver detail'),'EXECUTION_FAILED'],
] as const)('keeps readiness independent of optional metric failure (%s, %s)',async(match,error,code)=>{
  const f=fixture();f.state.failure={match,error};
  // Model an armed runtime without creating a scheduler or invoking a source.
  const snapshot=f.service.snapshot();
  vi.spyOn(AttemptsRuntimeControl.prototype,'snapshot').mockReturnValue({...snapshot,state:'running',enabled:true,armed:true} as ReturnType<AttemptsRuntimeControl['snapshot']>);
  const fail=vi.spyOn(AttemptsRuntimeControl.prototype,'requireReconciliation');
  try{
    await expect(f.service.probe()).resolves.toBeUndefined();await settled(f.service);
    expect(f.service.ready()).toBe(true);
    expect(f.service.snapshot()).toMatchObject({schemaReady:true,databaseState:'up',statisticsAvailable:false,statisticsStatus:'unavailable',statisticsError:{code},statisticsCheckedAt:null,totalCalls:null,pendingDeliveries:null,checkpoints:null,recoveryPending:null});
    expect(JSON.stringify(f.service.snapshot())).not.toContain('private');
    expect(fail).not.toHaveBeenCalled();expect(f.source).not.toHaveBeenCalled();
  }finally{await f.service.stop();}
});

it('does not await metrics at startup, shares one attempt and drains it before closing pools',async()=>{
  const f=fixture();let release!:()=>void;f.state.wait=new Promise<void>(r=>{release=r;});
  // start() is exercised with the scheduler entry point stubbed: no timer or acquisition runs.
  const start=vi.spyOn(AttemptsRuntimeControl.prototype,'start').mockImplementation(()=>{});
  try{
    await f.service.start();expect(start).toHaveBeenCalledOnce();
    await Promise.all([f.service.probe(),f.service.probe(),f.service.probe()]);
    expect(f.service.snapshot()).toMatchObject({schemaReady:true,statisticsStatus:'refreshing',statisticsAvailable:false});
    expect(f.metricCalls()).toHaveLength(1);expect(f.source).not.toHaveBeenCalled();
    const closing=f.service.stop();await Promise.resolve();expect(f.mocks.receiver.end).not.toHaveBeenCalled();
    release();await closing;expect(f.mocks.receiver.end).toHaveBeenCalledOnce();
  }finally{release();await f.service.stop();}
});

it('caches complete dated samples, masks stale values and throttles failures across 120s cycles',async()=>{
  vi.useFakeTimers({toFake:['Date']});vi.setSystemTime('2026-10-27T00:00:00Z');
  const f=fixture();
  try{
    await f.service.probe();await settled(f.service);
    const checked=f.service.snapshot().statisticsCheckedAt;
    expect(f.service.snapshot()).toMatchObject({statisticsStatus:'cached',statisticsAvailable:true,totalCalls:'12',confirmedDeliveries:'9',checkpoints:[{version:'1',last_batch_id:'fixture-batch'}]});
    expect(f.metricCalls()).toHaveLength(4);
    f.state.calls='999';
    for(let n=1;n<5;n++){
      vi.setSystemTime(Date.parse('2026-10-27T00:00:00Z')+n*120000);await f.service.probe();
      expect(f.metricCalls()).toHaveLength(4);expect(f.service.snapshot()).toMatchObject({statisticsStatus:'cached',statisticsCheckedAt:checked,totalCalls:'12'});
    }
    vi.setSystemTime('2026-10-27T00:10:00Z');
    expect(f.service.snapshot()).toMatchObject({statisticsStatus:'stale',statisticsAvailable:false,totalCalls:null,checkpoints:null});
    f.state.failure={match:/SELECT count\(.*projection_outbox/,error:Error('Query read timeout')};
    await f.service.probe();await settled(f.service);
    expect(f.service.snapshot()).toMatchObject({schemaReady:true,statisticsStatus:'unavailable',statisticsCheckedAt:checked,totalCalls:null,confirmedDeliveries:null});
    const calls=f.metricCalls().length;
    vi.setSystemTime('2026-10-27T00:12:00Z');await f.service.probe();expect(f.metricCalls()).toHaveLength(calls);
    f.state.failure=undefined;vi.setSystemTime(Date.now()+interval);await f.service.probe();await settled(f.service);
    expect(f.service.snapshot()).toMatchObject({statisticsAvailable:true,statisticsStatus:'cached',statisticsError:null,totalCalls:'999'});
    expect(f.service.snapshot().statisticsCheckedAt).not.toBe(checked);expect(f.source).not.toHaveBeenCalled();
  }finally{await f.service.stop();}
});

it.each(['connection','role','superuser','privilege','columnPrivilege','schema','primaryKey','essentialTimeout'] as const)('still rejects essential %s failures before collecting metrics',async point=>{
  const f=fixture();
  if(point==='connection')f.mocks.ingest.connect.mockRejectedValueOnce(Error('connection unavailable'));
  if(point==='role')f.state.badRole=true;
  if(point==='superuser')f.state.superuser=true;
  if(point==='privilege')f.state.denied=true;
  if(point==='columnPrivilege')f.state.badColumnPrivilege=true;
  if(point==='schema')f.state.badSchema=true;
  if(point==='primaryKey')f.state.badPk=true;
  if(point==='essentialTimeout')f.state.failure={match:/information_schema.columns/,error:Error('Query read timeout')};
  try{
    await expect(f.service.probe()).rejects.toThrow('POSTGRES_RUNTIME_NOT_READY');
    expect(f.service.snapshot()).toMatchObject({schemaReady:false,statisticsAvailable:false});
    expect(f.service.ready()).toBe(false);expect(f.metricCalls()).toHaveLength(0);expect(f.source).not.toHaveBeenCalled();
  }finally{await f.service.stop();}
});

it('rechecks essentials during cooldown and retains fatal timeout diagnostics for an armed runtime',async()=>{
  const f=fixture();const output=vi.spyOn(process.stderr,'write').mockReturnValue(true);
  try{
    await f.service.probe();await settled(f.service);
    const snapshot=new AttemptsRuntimeControl().snapshot();
    vi.spyOn(AttemptsRuntimeControl.prototype,'snapshot').mockReturnValue({...snapshot,state:'running',enabled:true,armed:true});
    f.state.failure={match:/has_column_privilege/,error:Error('Query read timeout')};
    const error=await f.service.probe().catch(e=>e);
    expect(failureDetails(error)).toMatchObject({code:'POSTGRES_RUNTIME_NOT_READY',technicalCode:'PG_QUERY_TIMEOUT',stage:'PROBE'});
    expect(f.service.ready()).toBe(false);expect(f.metricCalls()).toHaveLength(4);
    expect(output).toHaveBeenCalledOnce();expect(f.source).not.toHaveBeenCalled();
  }finally{await f.service.stop();}
});

it('blocks startup on an essential failure without arming the scheduler',async()=>{
  const f=fixture();f.state.badRole=true;
  const start=vi.spyOn(AttemptsRuntimeControl.prototype,'start').mockImplementation(()=>{});
  vi.spyOn(process.stderr,'write').mockReturnValue(true);
  try{
    await f.service.start();
    expect(f.service.snapshot()).toMatchObject({armed:false,schemaReady:false,lastFailure:{code:'POSTGRES_RUNTIME_NOT_READY',technicalCode:'ROLE_REJECTED'}});
    expect(start).not.toHaveBeenCalled();expect(f.source).not.toHaveBeenCalled();
    expect(f.metricCalls()).toHaveLength(0);
  }finally{await f.service.stop();}
});

it('does not authorize acquisition when metrics fail on a runtime with no authorized source',async()=>{
  const f=fixture();f.state.failure={match:/SELECT count\(/,error:Error('Query read timeout')};
  const service=new AttemptsService(f.service.pools),start=vi.spyOn(AttemptsRuntimeControl.prototype,'start').mockImplementation(()=>{});
  try{
    await service.start();await settled(service);
    expect(service.snapshot()).toMatchObject({schemaReady:true,statisticsAvailable:false,sourceAuthorized:false,armed:false,enabled:false});
    expect(service.ready()).toBe(false);expect(start).not.toHaveBeenCalled();
  }finally{await service.stop();}
});

it('can check administrative safety despite unavailable metrics without treating a query failure as clearance',async()=>{
  const f=fixture();f.state.failure={match:/SELECT count\(/,error:Error('Query read timeout')};
  try{
    await f.service.probe();await settled(f.service);
    expect(f.service.snapshot()).toMatchObject({statisticsAvailable:false,recoveryPending:null});
    await expect(f.service.checkRecoveryPending()).resolves.toBe(false);
    f.state.pendingRuns=true;await expect(f.service.checkRecoveryPending()).resolves.toBe(true);
    f.state.failure={match:/SELECT EXISTS.*projection_outbox/,error:Error('Query read timeout')};
    await expect(f.service.checkRecoveryPending()).rejects.toThrow('Query read timeout');
    expect(f.source).not.toHaveBeenCalled();
  }finally{await f.service.stop();}
});

it.each(['runs','outbox'] as const)('checks administrative %s pending state freshly even with cached zero counts',async kind=>{
  const f=fixture();
  try{
    await f.service.probe();await settled(f.service);expect(f.service.snapshot().recoveryPending).toBe(false);
    if(kind==='runs')f.state.pendingRuns=true;else f.state.pendingOutbox=true;
    await expect(f.service.checkRecoveryPending()).resolves.toBe(true);
    f.state.pendingRuns=false;f.state.pendingOutbox=false;
    await expect(f.service.checkRecoveryPending()).resolves.toBe(false);
    f.state.failure={match:/SELECT EXISTS/,error:Error('Query read timeout')};
    await expect(f.service.checkRecoveryPending()).rejects.toThrow('Query read timeout');
    expect(f.metricCalls()).toHaveLength(4);expect(f.source).not.toHaveBeenCalled();
  }finally{await f.service.stop();}
});
