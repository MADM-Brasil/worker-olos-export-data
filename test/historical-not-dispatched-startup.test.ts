import {afterAll,afterEach,beforeAll,beforeEach,describe,expect,it,vi} from 'vitest';
import {mkdtemp,mkdir,readFile,readdir,writeFile,rm} from 'node:fs/promises';
import {mkdirSync,unlinkSync} from 'node:fs';
import {join,dirname} from 'node:path';
import {tmpdir} from 'node:os';
import {randomUUID} from 'node:crypto';
import pg from 'pg';
import {isolatedPostgres} from './support/postgres.js';
import {LocalSpool} from '../adapters/local-spool.js';
import {spoolLimits} from '../src/large-batch/config.js';
import {captureIntentSchema,closeNotDispatchedServiceCase,createServiceCaptureQueue,loadServiceConfiguration} from '../adapters/attempts-service.js';
import {serviceConfigSchema} from '../src/integration/attempts/activation.js';
import {exampleProfile} from '../src/offline/model.js';
import {APPROVED_COLUMNS} from '../src/offline/tds-writer.js';
import {InspectionDiagnostics} from '../src/olos/tds/inspection-diagnostics.js';
import {ExactConnection} from '../src/olos/tds/connection.js';
import {IsolatedTdsSource} from '../src/olos/tds/source.js';
import {digest} from '../src/persistence/validation.js';
import {failureDetails} from '../src/diagnostics/worker-failure.js';
import {largePlan} from './support/large-batch-fixture.js';
import {PostgresPersistence} from '../src/persistence/store.js';

// Frozen pre-e53ad7b wire shape, deliberately independent of today's producer.
const legacy=(attemptId:string)=>({attemptId,version:'olos-log-inspection-diagnostic/v1',phase:'TLS_HANDSHAKE',
  lastObservedStage:'TLS_HANDSHAKE_STARTED',errorCode:'INVALID_TDS',
  observedStages:['AUTHORIZATION_STARTED','PERMIT_ISSUED','CONNECTION_INIT_STARTED','SOCKET_CONNECTED','TLS_HANDSHAKE_STARTED'],
  rpcDispatchState:'NOT_OBSERVED',remoteExecutionOutcome:'NOT_OBSERVED',retryRemote:false});
const save=async(path:string,value:unknown)=>{await mkdir(dirname(path),{recursive:true,mode:0o700});await writeFile(path,JSON.stringify(value),{mode:0o600});};
const json=async(path:string)=>JSON.parse(await readFile(path,'utf8'));
async function files(path:string):Promise<Record<string,string>>{
  const result:Record<string,string>={};
  for(const e of await readdir(path,{withFileTypes:true})){
    const p=join(path,e.name);if(e.isDirectory())Object.assign(result,await files(p));else result[p]=await readFile(p,'base64');
  }return result;
}

describe.each([false,true])('real queue initialization, closure ledger installed=%s',ledger=>{
  let db:Awaited<ReturnType<typeof isolatedPostgres>>,base:string,root:string,configFile:string;
  let pools:{ingest:pg.Pool;project:pg.Pool;receiver:pg.Pool},loaded:Awaited<ReturnType<typeof loadServiceConfiguration>>;
  beforeAll(async()=>{
    db=await isolatedPostgres(); // Disposable localhost container only; no installed database is used.
    const schema=['migrations/003_projection_control.sql','permissions/projection.sql'];
    if(ledger)schema.push('migrations/004_delivery_receipts.sql','migrations/005_large_batch_control.sql','migrations/007_local_ingestion_reconciliation.sql',
      'migrations/008_administrative_capture_discard.sql','permissions/administrative-capture-discard.sql','migrations/009_not_dispatched_closures.sql','permissions/not-dispatched-closures.sql');
    for(const file of schema)await db.admin.query(await readFile(new URL('../'+file,import.meta.url),'utf8'));
  },120000);
  afterAll(async()=>{await db?.stop();});
  beforeEach(async()=>{
    base=await mkdtemp(join(tmpdir(),'olos-historical-closure-'));root=join(base,'service-run');configFile=join(base,'config.json');
    const config=serviceConfigSchema.parse({version:'olos-service/v2',workerId:'synthetic-worker',endpoint:{server:'synthetic.invalid',port:1433,database:'ExportDataV3',user:'OEC',password:'SYNTHETIC'},
      postgres:{host:db.connection.host,port:db.connection.port,database:'olos_worker'},pinApprovalFile:join(base,'unused-pin.json'),approvalFile:join(base,'unused-approval.json'),
      profile:{...exampleProfile,phoneMatchKey:'normalized',columns:[...APPROVED_COLUMNS]},campaignFile:join(base,'campaigns.json'),dispositionFile:join(base,'dispositions.json')});
    await save(configFile,config);await save(config.campaignFile,[]);await save(config.dispositionFile,[]);loaded=await loadServiceConfiguration(configFile);
    await save(join(root,'queue-mode.json'),{kind:'olos-independent-delivery/v1',configurationDigest:loaded.configurationDigest});
    pools={ingest:db.worker(),project:new pg.Pool(db.connection),receiver:new pg.Pool(db.connection)};
    vi.spyOn(ExactConnection.prototype,'connect').mockImplementation(()=>{throw Error('REMOTE_CONNECTION_FORBIDDEN');});
    vi.spyOn(IsolatedTdsSource.prototype,'call').mockImplementation(()=>{throw Error('RPC_FORBIDDEN');});
    vi.spyOn(process.stderr,'write').mockReturnValue(true);
  });
  afterEach(async()=>{
    expect(ExactConnection.prototype.connect).not.toHaveBeenCalled();expect(IsolatedTdsSource.prototype.call).not.toHaveBeenCalled();
    vi.restoreAllMocks();await Promise.all(Object.values(pools).map(p=>p.end()));await rm(base,{recursive:true,force:true});
  });
  const options=()=>({base,root,configFile,intervalMs:120000});
  const queue=()=>createServiceCaptureQueue(options(),pools,loaded,()=>{});
  async function closed(kind:'legacy'|'v1-transport'|'v2'='legacy'){
    const id=randomUUID(),dir=join(root,'cases',id);
    const spool=await LocalSpool.open(join(dir,'capture'),spoolLimits({minFreeBytes:0}));
    await (await spool.createResponseCapture(id)).interrupt();
    let diagnostic:Record<string,unknown>=legacy(id);
    if(kind!=='legacy'){
      const d=new InspectionDiagnostics();for(const stage of ['PERMIT_ISSUED','CONNECTION_INIT_STARTED','SOCKET_CONNECTED','TLS_HANDSHAKE_STARTED'] as const)d.observe({kind:'stage',stage});
      d.observe({kind:'transport',reason:'TLS_HANDSHAKE_FAILED',errorCode:'INVALID_TDS',nativeErrorType:'Error'});
      diagnostic={attemptId:id,...d.snapshot()};if(kind==='v1-transport')diagnostic.version='olos-log-inspection-diagnostic/v1';
    }
    await save(join(dir,'diagnostic.json'),diagnostic);
    const proof={kind:'olos-local-not-dispatched/v1',attemptId:id,diagnosticDigest:digest(diagnostic),evidence:'CONTROLLED_RPC_HANDOFF_NOT_REACHED_AFTER_TRANSPORT_CLOSED'};
    await save(join(dir,'not-dispatched.json'),proof);await save(join(dir,'not-dispatched-closed.json'),proof);
    await save(join(dir,'remote-state.json'),{state:'REMOTE_OUTCOME_UNKNOWN',attemptId:id,retryRemote:false});
    await save(join(dir,'remote-pending.not-dispatched.json'),{attemptId:id,workDir:root,configurationDigest:loaded.configurationDigest});
    await save(join(root,`attempt-${id}.json`),captureIntentSchema.parse({attemptId:id,procedure:'ReadDataCurrentDay',configurationDigest:loaded.configurationDigest,workDir:root,
      createdAt:'2026-10-22T00:00:00.000Z',controlDate:'2026-10-22',operator:'test',approvalReference:'synthetic',pinApprovalId:'unused',contractReviewDigest:null,plan:null,state:'PREPARED'}));
    return {id,dir,diagnostic};
  }
  // Fixture forgeries only. The production reader never writes any diagnostic/digest/proof.
  async function replaceDiagnostic(dir:string,diagnostic:Record<string,unknown>,resign=false){
    await save(join(dir,'diagnostic.json'),diagnostic);
    if(resign)for(const name of ['not-dispatched.json','not-dispatched-closed.json']){
      const p=join(dir,name);await save(p,{...await json(p),diagnosticDigest:digest(diagnostic)});
    }
  }
  async function reject(){const before=await files(base);await expect(queue().initialize(new AbortController().signal)).rejects.toThrow();expect(await files(base)).toEqual(before);}
  async function accept(){
    const before=await files(base),lock=(await db.admin.query("SELECT * FROM olos_export.source_locks WHERE source_table='AttemptsRawData'")).rows;
    for(let n=0;n<2;n++){const q=queue();await q.initialize(new AbortController().signal);expect(q.snapshot().captureQueue.pending).toBe(0);}
    expect(await files(base)).toEqual(before);
    expect((await db.admin.query("SELECT * FROM olos_export.source_locks WHERE source_table='AttemptsRawData'")).rows).toEqual(lock);
    expect((await db.admin.query('SELECT * FROM olos_export.runs')).rows).toEqual([]);
    if(ledger)expect((await db.admin.query('SELECT * FROM olos_export.not_dispatched_closures')).rows).toEqual([]);
  }

  it('the producer explicitly versions the required transport contract',()=>{
    expect(new InspectionDiagnostics().snapshot()).toMatchObject({version:'olos-log-inspection-diagnostic/v2',transport:[]});
  });
  it.each(['legacy','v1-transport','v2'] as const)('accepts %s and restarts without rewriting evidence or touching fencing',async kind=>{await closed(kind);await accept();});
  it.each(['v1-transport','v2'] as const)('accepts %s with a present empty transport array',async kind=>{
    const c=await closed(kind);await replaceDiagnostic(c.dir,{...c.diagnostic,transport:[]},true);await accept();
  });
  it.each(['AUTHORIZATION_STARTED','CONNECTION_INIT_STARTED','SQL_LOGIN_STARTED'] as const)('accepts the historical format at %s',async stage=>{
    const c=await closed();
    const stages=stage==='AUTHORIZATION_STARTED'?['AUTHORIZATION_STARTED']:stage==='CONNECTION_INIT_STARTED'?['AUTHORIZATION_STARTED','PERMIT_ISSUED',stage]:
      ['AUTHORIZATION_STARTED','PERMIT_ISSUED','CONNECTION_INIT_STARTED','SOCKET_CONNECTED','TLS_HANDSHAKE_STARTED','TLS_VALIDATED','SQL_LOGIN_PREPARATION',stage];
    await replaceDiagnostic(c.dir,{...c.diagnostic,observedStages:stages,lastObservedStage:stage,
      phase:stage==='AUTHORIZATION_STARTED'?'AUTHORIZATION':stage==='CONNECTION_INIT_STARTED'?'CONNECTION_INIT':'SQL_LOGIN',errorCode:stage==='SQL_LOGIN_STARTED'?'ELOGIN':'CONNECTION_FAILED'},true);
    await accept();
  });
  it('validates every historical capture in a mixed queue on repeated initialization',async()=>{
    for(const kind of ['legacy','v2','legacy','v1-transport','legacy'] as const)await closed(kind);
    await accept();
  });
  it('one invalid historical case blocks initialization even among multiple valid closures',async()=>{
    await closed();const bad=await closed();await closed('v2');await closed();
    await replaceDiagnostic(bad.dir,{...bad.diagnostic,errorCode:'TIMEOUT'});await reject();
  });
  it.each(['v1-transport','v2'] as const)('rejects %s with transport removed, even if only its proof is replaced',async kind=>{
    const c=await closed(kind);delete c.diagnostic.transport;await replaceDiagnostic(c.dir,c.diagnostic);await reject();
    const p=join(c.dir,'not-dispatched.json');await save(p,{...await json(p),diagnosticDigest:digest(c.diagnostic)});await reject();
  });
  it('rejects v2 without transport even with matching forged proof and closure',async()=>{
    const c=await closed('v2');delete c.diagnostic.transport;await replaceDiagnostic(c.dir,c.diagnostic,true);await reject();
  });
  it.each(['v1-transport','v2'] as const)('preserves strict transport validation for %s',async kind=>{
    const c=await closed(kind),valid=structuredClone(c.diagnostic);
    for(const transport of [null,{},[{phase:'TLS_HANDSHAKE',lastObservedStage:'RPC_HANDOFF_STARTED',reason:'TLS_HANDSHAKE_FAILED'}],
      [{phase:'SQL_LOGIN',lastObservedStage:'TLS_HANDSHAKE_STARTED',reason:'TLS_HANDSHAKE_FAILED'}],
      [{phase:'TLS_HANDSHAKE',lastObservedStage:'TLS_HANDSHAKE_STARTED',reason:'UNKNOWN_REASON'}],
      [{phase:'TLS_HANDSHAKE',lastObservedStage:'TLS_HANDSHAKE_STARTED',reason:'TLS_HANDSHAKE_FAILED',extra:'forged'}]]){
      await replaceDiagnostic(c.dir,{...valid,transport},true);await reject();
    }
  });
  it.each(['new-field','new-stage','new-error','phase','stage-order','stage-duplicate','last-stage','rpc','outcome','retry','version','diagnostic-id'] as const)('rejects incompatible/forged legacy %s even with matching digests',async kind=>{
    const c=await closed(),d=c.diagnostic;
    if(kind==='new-field')d.transportFormat='invented';
    if(kind==='new-stage'){d.observedStages=['AUTHORIZATION_STARTED','PRELOGIN_SENT'];d.lastObservedStage='PRELOGIN_SENT';d.phase='PRELOGIN';}
    if(kind==='new-error')d.errorCode='ERR_SSL_BAD_RECORD_TYPE';
    if(kind==='phase')d.phase='CONNECTION_INIT';
    if(kind==='stage-order')d.observedStages=['AUTHORIZATION_STARTED','SOCKET_CONNECTED','CONNECTION_INIT_STARTED','TLS_HANDSHAKE_STARTED'];
    if(kind==='stage-duplicate')(d.observedStages as string[]).push('TLS_HANDSHAKE_STARTED');
    if(kind==='last-stage')d.lastObservedStage='SOCKET_CONNECTED';
    if(kind==='rpc'){(d.observedStages as string[]).push('RPC_HANDOFF_STARTED');d.lastObservedStage='RPC_HANDOFF_STARTED';d.rpcDispatchState='DRIVER_HANDOFF_STARTED';}
    if(kind==='outcome')d.remoteExecutionOutcome='UNKNOWN';
    if(kind==='retry')d.retryRemote=true;
    if(kind==='version')d.version='olos-log-inspection-diagnostic/v3';
    if(kind==='diagnostic-id')d.attemptId=randomUUID();
    await replaceDiagnostic(c.dir,d,true);await reject();
  });
  it.each(['closure','archive','proof','remote-state','response-descriptor','import','queued','delivered','response-bytes','response-ready','marker','intent','pending'] as const)('rejects missing or conflicting historical evidence: %s',async kind=>{
    const c=await closed(),capture=join(c.dir,'capture','batches',c.id);
    if(kind==='closure')await rm(join(c.dir,'not-dispatched-closed.json'));
    if(kind==='archive')await rm(join(c.dir,'remote-pending.not-dispatched.json'));
    if(kind==='proof')await rm(join(c.dir,'not-dispatched.json'));
    if(kind==='remote-state')await rm(join(c.dir,'remote-state.json'));
    if(kind==='response-descriptor')await rm(join(capture,'response.json'));
    if(kind==='import')await mkdir(join(c.dir,'import'),{mode:0o700});
    if(kind==='queued' || kind==='delivered')await save(join(c.dir,`${kind}.json`),{});
    if(kind==='response-bytes')await writeFile(join(capture,'response.partial'),'x');
    if(kind==='response-ready')await writeFile(join(capture,'response.ready'),'');
    if(kind==='marker')await save(join(c.dir,'remote-pending.not-dispatched.json'),{attemptId:c.id,workDir:root,configurationDigest:'0'.repeat(64)});
    if(kind==='intent'){const p=join(root,`attempt-${c.id}.json`);await save(p,{...await json(p),workDir:base});}
    if(kind==='pending')await save(join(base,'remote-pending.json'),await json(join(c.dir,'remote-pending.not-dispatched.json')));
    await reject();
  });
  it('cannot create a new closure using the historical exception',async()=>{
    const c=await closed(),before=await files(base);
    await expect(closeNotDispatchedServiceCase(options(),c.id,loaded.configurationDigest,()=>{},new AbortController().signal,pools,false)).rejects.toThrow('RECOVERY_REQUIRED');
    expect(await files(base)).toEqual(before);
  });
  it.each(['closure-removed','import-created'] as const)('revalidates historical preconditions inside the transaction: %s',async change=>{
    const c=await closed(),client=await pools.ingest.connect(),query=client.query.bind(client);
    vi.spyOn(pools.ingest,'connect').mockImplementationOnce((async()=>client) as typeof pools.ingest.connect);
    vi.spyOn(client,'query').mockImplementation((async(sql:string,...args:unknown[])=>{
      const result=await (query as (...args:unknown[])=>Promise<unknown>)(sql,...args);
      if(sql.includes('FROM olos_export.source_locks')){
        if(change==='closure-removed')unlinkSync(join(c.dir,'not-dispatched-closed.json'));
        else mkdirSync(join(c.dir,'import'),{mode:0o700});
      }
      return result;
    }) as typeof client.query);
    await expect(closeNotDispatchedServiceCase(options(),c.id,loaded.configurationDigest,()=>{},new AbortController().signal,pools,true)).rejects.toThrow();
    expect((await db.admin.query('SELECT * FROM olos_export.runs')).rows).toEqual([]);
    if(ledger)expect((await db.admin.query('SELECT * FROM olos_export.not_dispatched_closures')).rows).toEqual([]);
  });
  it('does not clear an unrelated pending run after validating historical closures',async()=>{
    const c=await closed(),plan=largePlan(),contract=plan.contract;
    await db.admin.query('INSERT INTO olos_export.source_contracts(source_table,version,approved,fields,key_fields,layout_evidence,key_evidence) VALUES($1,$2,true,$3,$4,$5,$6)',
      [contract.source_table,contract.version,contract.fields,contract.key_fields,contract.layout_evidence,contract.key_evidence]);
    const run=await new PostgresPersistence(db.admin).startRun(plan.run,new AbortController().signal);
    const before=(await db.admin.query('SELECT * FROM olos_export.runs WHERE id=$1',[run.runId])).rows;
    await expect(closeNotDispatchedServiceCase(options(),c.id,loaded.configurationDigest,()=>{},new AbortController().signal,pools,true)).resolves.toMatchObject({alreadyClosed:true});
    const error=await queue().initialize(new AbortController().signal).catch(e=>e);
    expect(failureDetails(error)).toMatchObject({code:'CAPTURE_QUEUE_REQUIRES_REVIEW',stage:'INITIALIZE'});
    expect((await db.admin.query('SELECT * FROM olos_export.runs WHERE id=$1',[run.runId])).rows).toEqual(before);
    expect((await db.admin.query("SELECT owner_run_id FROM olos_export.source_locks WHERE source_table='AttemptsRawData'")).rows[0].owner_run_id).toBe(run.runId);
    if(ledger){
      await db.admin.query(`INSERT INTO olos_export.not_dispatched_closures(capture_id,run_id,batch_id,stream_id,previous_generation,released_generation,evidence,evidence_digest)
        VALUES($1,$2,$3,$4,1,2,'{}',repeat('0',64))`,[c.id,run.runId,randomUUID(),run.streamId]);
      const conflicting=await queue().initialize(new AbortController().signal).catch(e=>e);
      expect(failureDetails(conflicting)).toMatchObject({code:'RECOVERY_REQUIRED',stage:'VALIDATE_BATCH',captureId:c.id});
      expect((await db.admin.query('SELECT * FROM olos_export.runs WHERE id=$1',[run.runId])).rows).toEqual(before);
    }
    // This is the last test in each isolated database; the entire fixture is destroyed.
  });
});
