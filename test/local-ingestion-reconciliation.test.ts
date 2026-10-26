import {setTimeout as delay} from 'node:timers/promises';
import {ChunkedProjectionCapture} from '../src/projection/chunked-capture.js';
import {beforeAll,afterAll,beforeEach,afterEach,it,expect,vi} from 'vitest';
import {readFile,mkdtemp,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {randomUUID} from 'node:crypto';
import type {Pool} from 'pg';
import {isolatedPostgres} from './support/postgres.js';
import {largePlan,rawSource} from './support/large-batch-fixture.js';
import {LocalSpool} from '../adapters/local-spool.js';
import {spoolLimits} from '../src/large-batch/config.js';
import {LargeBatchCoordinator} from '../src/large-batch/coordinator.js';
import {PostgresPersistence} from '../src/persistence/store.js';
import {PostgresLargeBatchPersistence,manifestDigest} from '../src/persistence/large-batch.js';
import {reconcileLocalIngestion,type LocalReconciliationApproval} from '../src/persistence/local-ingestion-reconciliation.js';
import {PersistenceError} from '../src/persistence/types.js';
import type {SpoolManifest} from '../src/large-batch/types.js';
import {parseCorporateArguments} from '../adapters/attempts-corporate.js';

let fixture:Awaited<ReturnType<typeof isolatedPostgres>>,pool:Pool,root:string,spool:LocalSpool,core:PostgresPersistence,writer:PostgresLargeBatchPersistence,m:SpoolManifest,a:LocalReconciliationApproval;
const limits=spoolLimits({minFreeBytes:0,chunkRows:2,bufferBytes:1024}),signal=()=>new AbortController().signal;
const projection={sourceNamespace:'olos.ExportDataV3.AttemptsRawData',destinationId:'local.olos_worker.raw.olos_calls',mappingVersion:'local-approved-16/v1'};
beforeAll(async()=>{
 fixture=await isolatedPostgres();
 for(const f of ['migrations/003_projection_control.sql','permissions/projection.sql','migrations/004_delivery_receipts.sql','migrations/005_large_batch_control.sql','permissions/large-batch.sql','migrations/007_local_ingestion_reconciliation.sql','permissions/local-ingestion-reconciliation.sql'])await fixture.admin.query(await readFile(new URL('../'+f,import.meta.url),'utf8'));
},150000);
afterAll(async()=>{await fixture?.stop();},30000);
beforeEach(async()=>{
 await fixture.admin.query('TRUNCATE olos_export.source_contracts,olos_export.source_locks,olos_export.streams CASCADE');
 const c=largePlan().contract;
 await fixture.admin.query('INSERT INTO olos_export.source_contracts(source_table,version,approved,fields,key_fields,layout_evidence,key_evidence) VALUES($1,$2,true,$3,$4,$5,$6)',[c.source_table,c.version,c.fields,c.key_fields,c.layout_evidence,c.key_evidence]);
 await fixture.admin.query("INSERT INTO olos_export.source_locks(source_table,generation) VALUES('AttemptsRawData',3)");
 pool=fixture.worker();core=new PostgresPersistence(pool);writer=new PostgresLargeBatchPersistence(pool,limits,300000,projection,true,true);
 root=await mkdtemp(join(tmpdir(),'olos-local-proof-'));spool=await LocalSpool.open(root,limits);
 const source=rawSource(7,{duplicate:true});
 const target={preflight:writer.preflight.bind(writer),persist:async()=>{throw new PersistenceError('LEASE_LOST');}};
 await expect(new LargeBatchCoordinator(core,spool,target,limits).receive(largePlan(),source,signal())).rejects.toMatchObject({code:'LEASE_LOST'});
 expect(source.calls).toBe(1);m=(await spool.active())!;
 // Actually execute partial writes in a transaction, then fail: every effect must roll back.
 const h={runId:m.identity.runId,streamId:m.identity.streamId,fencingToken:m.identity.fencingToken,signal:signal()};
 await expect(writer.persist(m,h,async function*(){let n=0;for await(const r of spool.records(m)){if(n++===4)throw Error('synthetic stream interruption');yield r;}})).rejects.toMatchObject({code:'PERSISTENCE_FAILED'});
 await fixture.admin.query("UPDATE olos_export.source_locks SET lease_until=clock_timestamp()-interval '1 second'");
 const [caseId]=await core.recoverExpired();
 a={version:'olos-local-ingestion-reconciliation/v1',authorizationId:randomUUID(),captureId:randomUUID(),batchId:m.identity.batchId,runId:m.identity.runId,recoveryCaseId:caseId!,manifestDigest:manifestDigest(m),expectedGeneration:'5',operatorReference:'TEST-LOCAL-ONLY',authorizedAt:new Date().toISOString(),expiresAt:new Date(Date.now()+3600000).toISOString(),decision:'AUTHORIZE_VERIFIED_LOCAL_REPLAY',remoteReplayAllowed:false};
});
afterEach(async()=>{vi.restoreAllMocks();await pool?.end();await rm(root,{recursive:true,force:true});});
const prepare=()=>reconcileLocalIngestion(pool,spool,m,a,signal());
const stats=async()=> (await fixture.admin.query(`SELECT (SELECT count(*)::int FROM olos_export.batches) batches,(SELECT count(*)::int FROM olos_export.attempts_raw_data) envelopes,(SELECT count(*)::int FROM olos_export.projection_outbox) deliveries,(SELECT count(*)::int FROM olos_export.local_ingestion_reconciliations) proofs,(SELECT version::text FROM olos_export.checkpoints) checkpoint`)).rows[0];
const replay=async(h:Awaited<ReturnType<typeof prepare>>)=>writer.persistReconciled(m,h,()=>spool.records(m));
function loseCommitAck(){
 const connect=pool.connect.bind(pool);let lost=false;
 return vi.spyOn(pool,'connect').mockImplementation((async()=>{
  const c=await connect();return new Proxy(c,{get(target,key){if(key==='query')return async(sql:string,...args:unknown[])=>{
   const result=await (target.query as (...args:unknown[])=>Promise<unknown>).call(target,sql,...args);
   if(sql==='COMMIT' && !lost){lost=true;throw Error('synthetic commit ACK lost');}return result;
  };const v=Reflect.get(target,key);return typeof v==='function'?v.bind(target):v;}});
 }) as typeof pool.connect);
}
it('proves actual partial rollback, preserves original batch/manifest/token, and deduplicates local replay',async()=>{
 expect(await stats()).toEqual({batches:0,envelopes:0,deliveries:0,proofs:0,checkpoint:'0'});
 const before=await readFile(join(root,'batches',a.batchId,'manifest.json'),'utf8');const h=await prepare();
 expect(h.runId).not.toBe(a.runId);expect(h.fencingToken).toBe('6');
 await expect(core.startRun(largePlan().run,signal())).rejects.toMatchObject({code:'BUSY'});
 expect(await replay(h)).toEqual({received:7,persisted:1,duplicates:6,rejected:0});
 expect(await stats()).toEqual({batches:1,envelopes:1,deliveries:1,proofs:1,checkpoint:'0'});
 const batch=(await pool.query('SELECT * FROM olos_export.batches')).rows[0];expect(batch.id).toBe(a.batchId);expect(batch.run_id).toBe(a.runId);
 expect((await pool.query('SELECT fencing_token FROM olos_export.runs WHERE id=$1',[a.runId])).rows[0].fencing_token).toBe('4');
 expect((await pool.query('SELECT state FROM olos_export.recovery_cases WHERE id=$1',[a.recoveryCaseId])).rows[0].state).toBe('resolved');
 expect(await readFile(join(root,'batches',a.batchId,'manifest.json'),'utf8')).toBe(before);
 await expect(writer.confirmDeliveryCheckpoint(m)).rejects.toMatchObject({code:'RECOVERY_REQUIRED'});
 await replay(await prepare());expect((await stats()).batches).toBe(1);
 const evidence=(await pool.query('SELECT original_manifest FROM olos_export.local_ingestion_reconciliations')).rows[0];expect(evidence.original_manifest).toEqual(m);
});
it('normal replay remains fenced even after proof; only the linked local execution can write',async()=>{
 const h=await prepare();await expect(writer.persist(m,{...h,runId:a.runId,fencingToken:'4'},()=>spool.records(m))).rejects.toMatchObject({code:'LEASE_LOST'});
 await expect(writer.persistReconciled(m,{...h,runId:randomUUID()},()=>spool.records(m))).rejects.toMatchObject({code:'RECOVERY_REQUIRED'});
 expect((await stats()).batches).toBe(0);
});
it('requires operational approval, exact generation and manifest; never allocates on refusal',async()=>{
 for(const bad of [{...a,expectedGeneration:'4'},{...a,manifestDigest:'0'.repeat(64)},{...a,expiresAt:'2020-01-01T00:00:00.000Z'},{...a,remoteReplayAllowed:true}])await expect(reconcileLocalIngestion(pool,spool,m,bad,signal())).rejects.toThrow();
 expect((await stats()).proofs).toBe(0);
 expect(()=>parseCorporateArguments(['reconcile-ingestion','--reconciliation-file','/private/approval.json'],{})).toThrow('CORPORATE_AUTHORIZATION_REQUIRED');
 expect(parseCorporateArguments(['reconcile-ingestion','--reconciliation-file','/private/approval.json','--confirm','AUTHORIZE_VERIFIED_LOCAL_REPLAY'],{}).action).toBe('reconcile-ingestion');
});
it('absent receipt is insufficient when any original batch exists',async()=>{
 await fixture.admin.query(`INSERT INTO olos_export.batches(id,run_id,stream_id,source_table,digest,expected_version,state,received_count) VALUES($1,$2,$3,'AttemptsRawData',$4,0,'read_observed',7)`,[a.batchId,a.runId,m.identity.streamId,manifestDigest(m)]);
 await expect(prepare()).rejects.toMatchObject({code:'RECOVERY_REQUIRED'});expect((await stats()).proofs).toBe(0);
});
it('checkpoint divergence blocks proof and blocks replay if it changes after proof',async()=>{
 await fixture.admin.query('UPDATE olos_export.checkpoints SET version=1');await expect(prepare()).rejects.toMatchObject({code:'CHECKPOINT_CONFLICT'});
 await fixture.admin.query('UPDATE olos_export.checkpoints SET version=0');const h=await prepare();
 await fixture.admin.query('UPDATE olos_export.checkpoints SET version=1');await expect(replay(h)).rejects.toMatchObject({code:'CHECKPOINT_CONFLICT'});
 expect((await stats()).batches).toBe(0);
});
it('cannot decide rollback while another backend holds the source row lock',async()=>{
 const c=await fixture.admin.connect();try{
  await c.query('BEGIN');await c.query('SELECT * FROM olos_export.source_locks FOR UPDATE');
  await expect(prepare()).rejects.toMatchObject({code:'BUSY'});expect((await stats()).proofs).toBe(0);
 }finally{await c.query('ROLLBACK');c.release();}
 expect((await prepare()).fencingToken).toBe('6');
});
it('concurrent authorizations cannot allocate two recovery executions',async()=>{
 const result=await Promise.allSettled([prepare(),reconcileLocalIngestion(pool,spool,m,{...a,authorizationId:randomUUID()},signal())]);
 expect(result.filter(r=>r.status==='fulfilled')).toHaveLength(1);expect((await stats()).proofs).toBe(1);
});
it('crash after proof restarts with the same identity while lease is live',async()=>{
 const first=await prepare();const restarted=await LocalSpool.open(root,limits);
 const second=await reconcileLocalIngestion(pool,restarted,m,a,signal());expect(second.runId).toBe(first.runId);expect((await stats()).proofs).toBe(1);
 await replay(second);expect((await stats()).batches).toBe(1);
});
it('expired recovery requires a fresh explicit authorization and proof; never reuses its token',async()=>{
 const first=await prepare();await fixture.admin.query("UPDATE olos_export.source_locks SET lease_until=clock_timestamp()-interval '1 second'");
 await expect(prepare()).rejects.toMatchObject({code:'RECOVERY_REQUIRED'});await expect(replay(first)).rejects.toMatchObject({code:'LEASE_LOST'});
 await core.recoverExpired();a={...a,authorizationId:randomUUID(),expectedGeneration:'7'};
 const next=await prepare();expect(next.fencingToken).toBe('8');expect(next.runId).not.toBe(first.runId);
 await replay(next);expect((await stats()).proofs).toBe(2);expect((await pool.query("SELECT 1 FROM olos_export.recovery_cases WHERE state='open'")).rowCount).toBe(0);
 await expect(replay(first)).resolves.toMatchObject({received:7}); // exact committed receipt is read only
});
it('unknown proof COMMIT is resolved by the immutable ledger, without another execution',async()=>{
 const spy=loseCommitAck();await expect(prepare()).rejects.toMatchObject({code:'COMMIT_UNKNOWN',outcome:'unknown'});spy.mockRestore();
 expect((await stats()).proofs).toBe(1);await replay(await prepare());expect((await stats()).proofs).toBe(1);
});
it('unknown ingestion COMMIT and crash before filesystem ACK return the original receipt without replay',async()=>{
 const h=await prepare(),spy=loseCommitAck();await expect(replay(h)).rejects.toMatchObject({code:'COMMIT_UNKNOWN',outcome:'unknown'});spy.mockRestore();
 expect((await stats()).batches).toBe(1);expect((await spool.active())?.state).toBe('uncertain');
 const records=vi.fn(async function*(){throw Error('must not replay committed ingestion');});
 await writer.persistReconciled(m,await prepare(),records);expect(records).not.toHaveBeenCalled();
 await spool.mark(m,'committed');expect(await spool.active()).toBeNull();expect((await stats()).checkpoint).toBe('0');
});
it('failure halfway through recovery preserves the proof and open case; restart deduplicates once',async()=>{
 const h=await prepare();await expect(writer.persistReconciled(m,h,async function*(){let n=0;for await(const r of spool.records(m)){if(n++===4)throw Error('crash');yield r;}})).rejects.toThrow();
 expect(await stats()).toEqual({batches:0,envelopes:0,deliveries:0,proofs:1,checkpoint:'0'});
 expect((await pool.query('SELECT state FROM olos_export.recovery_cases WHERE id=$1',[a.recoveryCaseId])).rows[0].state).toBe('open');
 await replay(await prepare());expect((await stats()).envelopes).toBe(1);
});
it('a successor owned by another execution is never modified',async()=>{
 const id=randomUUID();await fixture.admin.query(`INSERT INTO olos_export.runs(id,stream_id,source_table,contract_version,fencing_token,state) VALUES($1,$2,'AttemptsRawData',$3,6,'read_intent')`,[id,m.identity.streamId,m.identity.plan.contract.version]);
 await fixture.admin.query("UPDATE olos_export.source_locks SET owner_run_id=$1,generation=6,lease_until=clock_timestamp()-interval '1 second'",[id]);
 a={...a,expectedGeneration:'6'};await expect(prepare()).rejects.toMatchObject({code:'RECOVERY_REQUIRED'});
 expect((await pool.query('SELECT owner_run_id FROM olos_export.source_locks')).rows[0].owner_run_id).toBe(id);expect((await stats()).proofs).toBe(0);
});

it('materialization may exceed 15 seconds with active queries and chunk lease renewal',async()=>{
 const h=await prepare();
 const shortLeaseWriter=new PostgresLargeBatchPersistence(pool,limits,10000,projection,true,true);
 const start=Date.now();let checked=false;
 await shortLeaseWriter.persistReconciled(m,h,async function*(){for await(const r of spool.records(m)){
  await delay(2300);if(!checked){checked=true;expect(await core.recoverExpired()).toEqual([]);}yield r;
 }});
 expect(Date.now()-start).toBeGreaterThan(15000);expect((await stats()).batches).toBe(1);
},30000);
it('lease expiring during projection finalization rolls back instead of committing late',async()=>{
 const h=await prepare(),finish=ChunkedProjectionCapture.prototype.finish;
 vi.spyOn(ChunkedProjectionCapture.prototype,'finish').mockImplementation(async function(this:ChunkedProjectionCapture){await finish.call(this);await delay(300);});
 const shortLeaseWriter=new PostgresLargeBatchPersistence(pool,limits,200,projection,true,true);
 await expect(shortLeaseWriter.persistReconciled(m,h,()=>spool.records(m))).rejects.toMatchObject({code:'LEASE_LOST'});
 expect((await stats()).batches).toBe(0);expect((await stats()).checkpoint).toBe('0');
});



it.each([
  'receipt-count-mismatch',
  'wrong-mapping',
  'extra-projection',
  'broken-sequence'
])('refuses version one when prior ingestion has %s',async fault=>{
  const first=m;
  const h=await prepare();
  await replay(h);
  await spool.mark(first,'committed');

  const secondSpool=await LocalSpool.open(join(root,'negative-second'),limits);
  const control={
    startRun:core.startRun.bind(core),
    checkpoint:writer.ingestionCheckpoint.bind(writer),
    renew:core.renew.bind(core),
    recoverExpired:core.recoverExpired.bind(core)
  };
  const failing={
    preflight:writer.preflight.bind(writer),
    persist:async()=>{throw new PersistenceError('LEASE_LOST');}
  };

  await expect(
    new LargeBatchCoordinator(control,secondSpool,failing,limits)
      .receive(largePlan(),rawSource(7,{duplicate:true}),signal())
  ).rejects.toMatchObject({code:'LEASE_LOST'});

  spool=secondSpool;
  m=(await spool.active())!;
  expect(m.identity.expected.version).toBe('1');

  await fixture.admin.query(
    "UPDATE olos_export.source_locks SET lease_until=clock_timestamp()-interval '1 second'"
  );
  const [caseId]=await core.recoverExpired();

  const generation=(await fixture.admin.query(
    "SELECT generation::text FROM olos_export.source_locks WHERE source_table='AttemptsRawData'"
  )).rows[0].generation as string;

  a={
    ...a,
    authorizationId:randomUUID(),
    captureId:randomUUID(),
    batchId:m.identity.batchId,
    runId:m.identity.runId,
    recoveryCaseId:caseId!,
    manifestDigest:manifestDigest(m),
    expectedGeneration:generation,
    authorizedAt:new Date().toISOString(),
    expiresAt:new Date(Date.now()+3600000).toISOString()
  };

  if(fault==='receipt-count-mismatch'){
    // Simula inconsistência entre o contador do lote e o recibo.
    await fixture.admin.query(
      'UPDATE olos_export.large_batch_receipts SET record_count=record_count+1 WHERE batch_id=$1',
      [first.identity.batchId]
    );
  }

  if(fault==='wrong-mapping'){
    // Mantém intactas as chaves estrangeiras; altera somente
    // a identidade de mapeamento para um valor não aprovado.
    await fixture.admin.query(
      'UPDATE olos_export.projection_batches SET mapping_version=$2 WHERE batch_id=$1',
      [first.identity.batchId,'unapproved.mapping/v1']
    );
  }

  if(fault==='extra-projection'){
    // Segunda projeção válida para outro destino: deve ser recusada.
    await fixture.admin.query(`
      INSERT INTO olos_export.projection_batches(
        batch_id,destination_id,source_table,source_namespace,
        source_contract_version,field_definitions,destination_version,
        mapping_version,config_digest,snapshot_digest,row_count
      )
      SELECT batch_id,'unapproved.extra.destination',source_table,
             source_namespace,source_contract_version,field_definitions,
             destination_version,mapping_version,config_digest,
             snapshot_digest,row_count
      FROM olos_export.projection_batches
      WHERE batch_id=$1
    `,[first.identity.batchId]);
  }

  if(fault==='broken-sequence'){
    await fixture.admin.query(
      'UPDATE olos_export.batches SET expected_version=5 WHERE id=$1',
      [first.identity.batchId]
    );
  }

  await expect(prepare()).rejects.toMatchObject({code:'RECOVERY_REQUIRED'});
  expect((await fixture.admin.query(
    'SELECT count(*)::int AS total FROM olos_export.local_ingestion_reconciliations'
  )).rows[0].total).toBe(1);
});

it('recovers version one after a prior committed batch without advancing delivery checkpoint',async()=>{
  // Primeiro lote: materializado, mas ainda não entregue.
  const firstManifest=m;
  const firstHandle=await prepare();
  await replay(firstHandle);
  await spool.mark(firstManifest,'committed');

  const originalBatch=(await fixture.admin.query(
    'SELECT * FROM olos_export.batches WHERE id=$1',
    [firstManifest.identity.batchId]
  )).rows[0];

  const originalReceipt=(await fixture.admin.query(
    'SELECT * FROM olos_export.large_batch_receipts WHERE batch_id=$1',
    [firstManifest.identity.batchId]
  )).rows[0];

  expect((await stats()).checkpoint).toBe('0');

  // Segunda captura usa o tip de ingestão, não o checkpoint de entrega.
  const secondSpool=await LocalSpool.open(join(root,'second-capture'),limits);
  const control={
    startRun:core.startRun.bind(core),
    checkpoint:writer.ingestionCheckpoint.bind(writer),
    renew:core.renew.bind(core),
    recoverExpired:core.recoverExpired.bind(core)
  };

  const failing={
    preflight:writer.preflight.bind(writer),
    persist:async()=>{throw new PersistenceError('LEASE_LOST');}
  };

  await expect(
    new LargeBatchCoordinator(control,secondSpool,failing,limits)
      .receive(largePlan(),rawSource(7,{duplicate:true}),signal())
  ).rejects.toMatchObject({code:'LEASE_LOST'});

  spool=secondSpool;
  m=(await spool.active())!;

  expect(m.identity.expected.version).toBe('1');
  expect(m.identity.expected.cursor).toBeNull();

  await fixture.admin.query(
    "UPDATE olos_export.source_locks SET lease_until=clock_timestamp()-interval '1 second'"
  );

  const [caseId]=await core.recoverExpired();
  expect(caseId).toBeTruthy();

  const generation=(await fixture.admin.query(
    "SELECT generation::text FROM olos_export.source_locks WHERE source_table='AttemptsRawData'"
  )).rows[0].generation as string;

  a={
    ...a,
    authorizationId:randomUUID(),
    captureId:randomUUID(),
    batchId:m.identity.batchId,
    runId:m.identity.runId,
    recoveryCaseId:caseId!,
    manifestDigest:manifestDigest(m),
    expectedGeneration:generation,
    authorizedAt:new Date().toISOString(),
    expiresAt:new Date(Date.now()+3600000).toISOString()
  };

  // Nenhuma prova deve avançar a entrega.
  const handle=await prepare();
  expect(handle.runId).not.toBe(m.identity.runId);
  expect((await stats()).checkpoint).toBe('0');

  await replay(handle);

  const secondBatch=(await fixture.admin.query(
    'SELECT * FROM olos_export.batches WHERE id=$1',
    [m.identity.batchId]
  )).rows[0];

  expect(secondBatch.state).toBe('committed');
  expect(secondBatch.expected_version).toBe('1');
  expect((await stats()).checkpoint).toBe('0');

  // Lote anterior permanece intacto.
  expect((await fixture.admin.query(
    'SELECT * FROM olos_export.batches WHERE id=$1',
    [firstManifest.identity.batchId]
  )).rows[0]).toEqual(originalBatch);

  expect((await fixture.admin.query(
    'SELECT * FROM olos_export.large_batch_receipts WHERE batch_id=$1',
    [firstManifest.identity.batchId]
  )).rows[0]).toEqual(originalReceipt);
});
