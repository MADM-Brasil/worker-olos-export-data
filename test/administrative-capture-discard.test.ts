import {beforeEach,afterEach,it,expect,vi} from 'vitest';
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
import {PersistenceError} from '../src/persistence/types.js';
import type {SpoolManifest} from '../src/large-batch/types.js';
import {discardAdministrativeCapture,assertAdministrativeDiscard,type AdministrativeCaptureDiscardApproval,type DiscardLocalEvidence} from '../src/persistence/administrative-capture-discard.js';
import {parseCorporateArguments} from '../adapters/attempts-corporate.js';

let fixture:Awaited<ReturnType<typeof isolatedPostgres>>,pool:Pool,root:string,spool:LocalSpool,core:PostgresPersistence,writer:PostgresLargeBatchPersistence,m:SpoolManifest,a:AdministrativeCaptureDiscardApproval,local:DiscardLocalEvidence;
const limits=spoolLimits({minFreeBytes:0,chunkRows:2,bufferBytes:1024}),signal=()=>new AbortController().signal;
const projection={sourceNamespace:'olos.ExportDataV3.AttemptsRawData',destinationId:'local.olos_worker.raw.olos_calls',mappingVersion:'local-approved-16/v1'};
beforeEach(async()=>{
 // Each case owns a disposable database/container. No SQL DELETE/TRUNCATE of application data.
 fixture=await isolatedPostgres();
 for(const f of ['migrations/003_projection_control.sql','permissions/projection.sql','migrations/004_delivery_receipts.sql','migrations/005_large_batch_control.sql','permissions/large-batch.sql','migrations/007_local_ingestion_reconciliation.sql','permissions/local-ingestion-reconciliation.sql','migrations/008_administrative_capture_discard.sql','permissions/administrative-capture-discard.sql'])await fixture.admin.query(await readFile(new URL('../'+f,import.meta.url),'utf8'));
 const c=largePlan().contract;
 await fixture.admin.query('INSERT INTO olos_export.source_contracts(source_table,version,approved,fields,key_fields,layout_evidence,key_evidence) VALUES($1,$2,true,$3,$4,$5,$6)',[c.source_table,c.version,c.fields,c.key_fields,c.layout_evidence,c.key_evidence]);
 pool=fixture.worker();core=new PostgresPersistence(pool);writer=new PostgresLargeBatchPersistence(pool,limits,300000,projection,true,true);
 root=await mkdtemp(join(tmpdir(),'olos-discard-'));spool=await LocalSpool.open(join(root,'interrupted'),limits);
 const control={startRun:core.startRun.bind(core),checkpoint:writer.ingestionCheckpoint.bind(writer),renew:core.renew.bind(core),recoverExpired:core.recoverExpired.bind(core)};
 await new LargeBatchCoordinator(control,await LocalSpool.open(join(root,'previous'),limits),writer,limits).receive(largePlan(),rawSource(3),signal());
 await expect(new LargeBatchCoordinator(control,spool,{preflight:writer.preflight.bind(writer),persist:async()=>{throw new PersistenceError('LEASE_LOST');}},limits).receive(largePlan(),rawSource(7),signal())).rejects.toMatchObject({code:'LEASE_LOST'});
 m=(await spool.active())!;expect(m.identity.expected.version).toBe('1');
 await fixture.admin.query("UPDATE olos_export.source_locks SET lease_until=clock_timestamp()-interval '1 second' WHERE owner_run_id=$1",[m.identity.runId]);
 const [caseId]=await core.recoverExpired();
 a={version:'olos-administrative-capture-discard/v1',authorizationId:randomUUID(),captureId:randomUUID(),batchId:m.identity.batchId,runId:m.identity.runId,streamId:m.identity.streamId,recoveryCaseId:caseId!,manifestDigest:manifestDigest(m),expectedGeneration:'3',operatorReference:'TEST-DISCARD',reason:'OPERATOR_DECLINED_HISTORICAL_IMPORT',authorizedAt:new Date(Date.now()-1000).toISOString(),expiresAt:new Date(Date.now()+3500000).toISOString(),decision:'AUTHORIZE_ADMINISTRATIVE_CAPTURE_DISCARD',remoteReplayAllowed:false};
 local={captureId:a.captureId,configurationDigest:'1'.repeat(64),captureSha256:'2'.repeat(64),intentDigest:'3'.repeat(64),reviewDigest:'4'.repeat(64),importStartedDigest:'5'.repeat(64)};
},150000);
afterEach(async()=>{vi.restoreAllMocks();await pool?.end();await fixture?.stop();if(root)await rm(root,{recursive:true,force:true});},30000);
const discard=(input:unknown=a)=>discardAdministrativeCapture(pool,m,input,signal(),local);
const stats=async()=> (await pool.query(`SELECT (SELECT count(*)::int FROM olos_export.administrative_capture_discards) proofs,
 (SELECT count(*)::int FROM olos_export.batches) batches,(SELECT count(*)::int FROM olos_export.projection_outbox WHERE state='pending') pending,
 (SELECT version::text FROM olos_export.checkpoints) checkpoint,(SELECT state FROM olos_export.runs WHERE id=$1) run,
 (SELECT state FROM olos_export.recovery_cases WHERE id=$2) recovery,(SELECT generation FROM olos_export.source_locks WHERE source_table='AttemptsRawData') generation`,[a.runId,a.recoveryCaseId])).rows[0];
function intercept(query:(sql:string)=>'before'|'after'|undefined){
 const connect=pool.connect.bind(pool);let failed=false;
 return vi.spyOn(pool,'connect').mockImplementation((async()=>{
  const c=await connect();return new Proxy(c,{get(target,key){if(key==='query')return async(sql:string,...args:unknown[])=>{
   const when=failed?undefined:query(sql);if(when==='before'){failed=true;throw Error('synthetic interrupted write');}
   const result=await (target.query as (...args:unknown[])=>Promise<unknown>).call(target,sql,...args);
   if(when==='after'){failed=true;throw Error('synthetic lost ACK');}return result;
  };const v=Reflect.get(target,key);return typeof v==='function'?v.bind(target):v;}});
 }) as typeof pool.connect);
}
it('discards only the interrupted batch, leaves earlier pending delivery, checkpoints and fencing unchanged',async()=>{
 const before=await readFile(join(spool.root,'batches',a.batchId,'manifest.json'),'utf8');
 expect(await discard()).toMatchObject({discarded:true,alreadyDiscarded:false});
 expect(await stats()).toEqual({proofs:1,batches:1,pending:3,checkpoint:'0',run:'failed',recovery:'resolved',generation:'3'});
 expect(await discard()).toMatchObject({alreadyDiscarded:true});await assertAdministrativeDiscard(pool,m,local);
 expect(await readFile(join(spool.root,'batches',a.batchId,'manifest.json'),'utf8')).toBe(before);
 const next=await core.startRun(largePlan().run,signal());expect(next.fencingToken).toBe('4');
 expect(await writer.ingestionCheckpoint(next)).toEqual({version:'1',cursor:null});
 await expect(writer.persist(m,{runId:a.runId,streamId:a.streamId,fencingToken:m.identity.fencingToken,signal:signal()},()=>spool.records(m))).rejects.toMatchObject({code:'LEASE_LOST'});
});
it.each(['missing','expired','future','capture','batch','run','stream','case','digest','generation','decision'])('rejects invalid authorization: %s',async kind=>{
 const bad:Record<string,unknown>={...a};
 if(kind==='expired')bad.expiresAt=new Date(Date.now()-1).toISOString();
 else if(kind==='future')bad.authorizedAt=new Date(Date.now()+60000).toISOString();
 else if(kind==='digest')bad.manifestDigest='0'.repeat(64);
 else if(kind==='generation')bad.expectedGeneration='2';
 else if(kind==='decision')bad.decision='IMPLICIT';
 else if(kind!=='missing')bad[({capture:'captureId',batch:'batchId',run:'runId',stream:'streamId',case:'recoveryCaseId'} as Record<string,string>)[kind]!]=randomUUID();
 await expect(discardAdministrativeCapture(pool,m,kind==='missing'?undefined:bad,signal(),local)).rejects.toThrow();expect((await stats()).proofs).toBe(0);
});
it.each(['lease','other_run','other_case','counter','batch','unknown','checkpoint','materialized'])('refuses unsafe database evidence: %s',async kind=>{
 if(kind==='lease')await fixture.admin.query("UPDATE olos_export.source_locks SET owner_run_id=$1,lease_until=clock_timestamp()+interval '1 minute' WHERE source_table='AttemptsRawData'",[a.runId]);
 if(kind==='other_run' || kind==='other_case'){
  const id=randomUUID();await fixture.admin.query(`INSERT INTO olos_export.runs(id,stream_id,source_table,contract_version,fencing_token,state) VALUES($1,$2,'AttemptsRawData',$3,4,$4)`,[id,a.streamId,m.identity.plan.contract.version,kind==='other_case'?'failed':'read_intent']);
  if(kind==='other_case')await fixture.admin.query("INSERT INTO olos_export.recovery_cases(id,run_id,reason) VALUES($1,$2,'expired_read_intent')",[randomUUID(),id]);
 }
 if(kind==='counter')await fixture.admin.query('UPDATE olos_export.runs SET received_count=1 WHERE id=$1',[a.runId]);
 if(kind==='batch')await fixture.admin.query(`INSERT INTO olos_export.batches(id,run_id,stream_id,source_table,digest,expected_version,state,received_count) VALUES($1,$2,$3,'AttemptsRawData',$4,1,'read_observed',7)`,[a.batchId,a.runId,a.streamId,a.manifestDigest]);
 if(kind==='unknown')await fixture.admin.query("INSERT INTO olos_export.sync_errors(id,run_id,code,outcome,received_count,rejected_count) VALUES($1,$2,'COMMIT_UNKNOWN','unknown',0,0)",[randomUUID(),a.runId]);
 if(kind==='checkpoint')await fixture.admin.query('UPDATE olos_export.checkpoints SET version=1 WHERE stream_id=$1',[a.streamId]);
 if(kind==='materialized')m={...m,state:'committed'};
 await expect(discard()).rejects.toThrow();expect((await stats()).proofs).toBe(0);
});
it('serializes competing discards and accepts only the identical committed decision',async()=>{
 const results=await Promise.allSettled([discard(),discard()]);expect(results.some(r=>r.status==='fulfilled')).toBe(true);
 expect((await stats()).proofs).toBe(1);expect(await discard()).toMatchObject({alreadyDiscarded:true});
 await expect(discard({...a,operatorReference:'CHANGED'})).rejects.toThrow();
 const anotherCapture=randomUUID();
 await expect(discardAdministrativeCapture(pool,m,{...a,captureId:anotherCapture},signal(),{...local,captureId:anotherCapture})).rejects.toThrow();
});
it('cannot race materialization while its backend still owns the source transaction lock',async()=>{
 const c=await fixture.admin.connect();try{
  await c.query('BEGIN');await c.query("SELECT * FROM olos_export.source_locks WHERE source_table='AttemptsRawData' FOR UPDATE");
  await expect(discard()).rejects.toMatchObject({code:'BUSY'});expect((await stats()).proofs).toBe(0);
 }finally{await c.query('ROLLBACK');c.release();}
 await discard();
});
it('failure before commit rolls back audit and both terminal transitions',async()=>{
 const spy=intercept(sql=>sql.startsWith('UPDATE olos_export.recovery_cases')?'before':undefined);
 await expect(discard()).rejects.toThrow();spy.mockRestore();
 expect(await stats()).toMatchObject({proofs:0,run:'needs_reconciliation',recovery:'open',checkpoint:'0'});await discard();
});
it('COMMIT_UNKNOWN and restart reconcile the ledger without duplicating effects, even after expiry',async()=>{
 const spy=intercept(sql=>sql==='COMMIT'?'after':undefined);
 await expect(discard()).rejects.toMatchObject({code:'COMMIT_UNKNOWN',outcome:'unknown'});spy.mockRestore();
 vi.spyOn(Date,'now').mockReturnValue(Date.parse(a.expiresAt)+1000);
 expect(await discard()).toMatchObject({alreadyDiscarded:true});await assertAdministrativeDiscard(pool,m,local);
 expect((await stats()).proofs).toBe(1);
});
it.each(['proof','state','local'])('restart refuses inconsistent confirmed evidence: %s',async kind=>{
 await discard();
 if(kind==='proof')await fixture.admin.query("UPDATE olos_export.administrative_capture_discards SET proof_digest=repeat('0',64)");
 if(kind==='state')await fixture.admin.query("UPDATE olos_export.recovery_cases SET state='open',resolved_at=NULL WHERE id=$1",[a.recoveryCaseId]);
 if(kind==='local')local={...local,captureSha256:'0'.repeat(64)};
 await expect(assertAdministrativeDiscard(pool,m,local)).rejects.toThrow();await expect(discard()).rejects.toThrow();
});
it('append-only privileges and CLI confirmation are enforced',async()=>{
 await discard();await expect(pool.query('UPDATE olos_export.administrative_capture_discards SET reason=reason')).rejects.toMatchObject({code:'42501'});
 expect(()=>parseCorporateArguments(['discard-capture','--discard-file','/private/approval.json'],{})).toThrow('CORPORATE_AUTHORIZATION_REQUIRED');
 expect(parseCorporateArguments(['discard-capture','--discard-file','/private/approval.json','--confirm','AUTHORIZE_ADMINISTRATIVE_CAPTURE_DISCARD'],{}).action).toBe('discard-capture');
 expect((await pool.query('SELECT version FROM olos_export.schema_versions ORDER BY version')).rows).toEqual([{version:1},{version:2}]);
});
