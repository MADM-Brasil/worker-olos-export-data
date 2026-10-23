import {afterAll,afterEach,beforeAll,beforeEach,it,expect,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import type pg from 'pg';
import {projectionLoadFixture} from './support/projection-load-fixture.js';
import {isolatedTdsDelivery} from '../src/offline/tds-delivery.js';
import {projectTdsAttempts} from '../src/offline/tds-projection.js';
import {requestDigest} from '../src/projection/request.js';
import type {DeliveryClaim} from '../src/projection/outbox.js';
import type {DeliveryRequest} from '../src/projection/dispatcher.js';
import {PostgresDeliveryTarget} from '../src/projection/postgres-target.js';
import {IsolatedTdsCallsWriter} from '../src/offline/tds-writer.js';
let f:Awaited<ReturnType<typeof projectionLoadFixture>>;
const signal=()=>new AbortController().signal;
const states=async()=> (await f.admin.query('SELECT state,count(*)::int n FROM olos_export.projection_outbox GROUP BY state ORDER BY state')).rows;
const counts=async()=> (await f.admin.query('SELECT (SELECT count(*)::int FROM raw.olos_calls) AS business,(SELECT count(*)::int FROM olos_export.delivery_receipts) AS receipts')).rows[0];
const expire=()=>f.admin.query("UPDATE olos_export.projection_outbox SET lease_until=CASE WHEN lease_owner IS NULL THEN NULL ELSE clock_timestamp()-interval '1 second' END,available_at=clock_timestamp()-interval '1 second'");
function delivery(project=f.project,receiver=f.receiver){return isolatedTdsDelivery(project,receiver,f.destinationId,f.profile,f.catalogs,'2026-01-01T00:00:00',true);}
function row(c:DeliveryClaim){return projectTdsAttempts(c.message.payload,f.catalogs,f.profile,'2026-01-01T00:00:00').row;}
function requests(claims:DeliveryClaim[]):DeliveryRequest[]{return claims.map(c=>({deliveryId:c.id,destinationId:f.destinationId,
 contractVersion:c.message.destination_version,mappingVersion:c.message.mapping_version,row:row(c),
 requestDigest:requestDigest(row(c),f.destinationId,c.message.destination_version,c.message.mapping_version)}));}
function loseCommit(pool:pg.Pool,number:number,committed:boolean):pg.Pool{
 let commits=0;
 return new Proxy(pool,{get(target,key){if(key==='connect')return async()=>{
  const client=await target.connect();return new Proxy(client,{get(c,k){if(k==='query')return async(sql:string,...args:unknown[])=>{
   if(sql==='COMMIT' && ++commits===number){if(committed)await c.query(sql);throw Error('SYNTHETIC_COMMIT_ACK_LOST');}
   return (c.query as (...a:unknown[])=>Promise<unknown>).call(c,sql,...args);
  };const v=Reflect.get(c,k);return typeof v==='function'?v.bind(c):v;}});
 };const v=Reflect.get(target,key);return typeof v==='function'?v.bind(target):v;}});
}
beforeAll(async()=>{f=await projectionLoadFixture();},120000);
beforeEach(async()=>{await f.seed(8);});
afterEach(()=>vi.restoreAllMocks());
afterAll(async()=>{await f?.stop();});

it('delivers the same exact 16 columns as runOnce, preserving existing IDs and individual audit records',async()=>{
 const old=delivery();for(let i=0;i<8;i++)expect(await old.dispatcher.runOnce()).toBe('confirmed');
 const before=(await f.admin.query('SELECT * FROM raw.olos_calls ORDER BY internal_id_olos')).rows;
 await f.seed(8);
 await f.admin.query("INSERT INTO raw.olos_calls(internal_id_olos,olos_call_id) VALUES(1,'preserved')");
 expect(await delivery().dispatcher.runBatch(signal())).toBe(8);
 const after=(await f.admin.query('SELECT * FROM raw.olos_calls ORDER BY internal_id_olos')).rows;
 expect(after.slice(1)).toEqual(before.slice(1));expect(after[0].olos_call_id).toBe('preserved');
 expect(await counts()).toEqual({business:8,receipts:8});expect(await states()).toEqual([{state:'confirmed',n:8}]);
 expect((await f.admin.query('SELECT event,count(*)::int n FROM olos_export.projection_events GROUP BY event ORDER BY event')).rows)
  .toEqual([{event:'claimed',n:8},{event:'confirmed',n:8},{event:'dispatch_authorized',n:8}]);
 expect(await delivery().dispatcher.runBatch(signal())).toBe(0);
});
it('legacy confirmed and uncertain deliveries resume through the same receipt protocol without rewriting them',async()=>{
 const d=delivery();expect(await d.dispatcher.runOnce()).toBe('confirmed');
 const claim=(await d.outbox.claim(randomUUID()))!;
 const auth=await d.outbox.authorize(claim,row(claim));await d.target.deliver({...requests([claim])[0]!,row:auth.row,requestDigest:auth.requestDigest});
 const before=(await f.admin.query('SELECT internal_id_olos::text,xmin::text FROM raw.olos_calls ORDER BY internal_id_olos')).rows;
 await expire();expect(await delivery().dispatcher.reconcile(claim.id)).toBe(true);
 expect(await delivery().dispatcher.runBatch(signal())).toBe(6);
 expect((await f.admin.query('SELECT internal_id_olos::text,xmin::text FROM raw.olos_calls WHERE internal_id_olos=ANY($1::bigint[]) ORDER BY internal_id_olos',[before.map(r=>r.internal_id_olos)])).rows).toEqual(before);
 expect(await counts()).toEqual({business:8,receipts:8});
});
it('pending batch claims survive a restart and are reclaimable only after the existing lease expires',async()=>{
 expect(await delivery().outbox.claimBatch(randomUUID())).toHaveLength(8);
 expect(await delivery().dispatcher.runBatch(signal())).toBe(0);
 await expire();expect(await delivery().dispatcher.runBatch(signal())).toBe(8);
});
it('a crash after group authorization without receipt remains uncertain after restart, never resent',async()=>{
 const d=delivery(),claims=await d.outbox.claimBatch(randomUUID());
 await d.outbox.authorizeBatch(claims.map(claim=>({claim,row:row(claim)})));await expire();
 const resumed=delivery();for(const c of claims)expect(await resumed.dispatcher.reconcile(c.id)).toBe(false);
 expect(await resumed.dispatcher.runBatch(signal())).toBe(0);
 expect(await counts()).toEqual({business:0,receipts:0});expect(await states()).toEqual([{state:'uncertain',n:8}]);
});
it.each([true,false])('ambiguous claim commit (committed=%s) never sends any row',async committed=>{
 const d=delivery(loseCommit(f.project,1,committed));
 await expect(d.dispatcher.runBatch(signal())).rejects.toThrow();expect(await counts()).toEqual({business:0,receipts:0});
 expect(await states()).toEqual([{state:'pending',n:8}]);await expire();expect(await delivery().dispatcher.runBatch(signal())).toBe(8);
});
it.each([true,false])('ambiguous group authorization commit (committed=%s) never sends any row',async committed=>{
 const d=delivery(loseCommit(f.project,2,committed));
 await expect(d.dispatcher.runBatch(signal())).rejects.toThrow();expect(await counts()).toEqual({business:0,receipts:0});
 expect(await states()).toEqual([{state:committed?'uncertain':'pending',n:8}]);
});
it.each([true,false])('receiver commit ambiguity (committed=%s) is reconciled individually without automatic retry',async committed=>{
 const d=delivery(f.project,loseCommit(f.receiver,1,committed));
 await expect(d.dispatcher.runBatch(signal())).rejects.toThrow();expect(await states()).toEqual([{state:'uncertain',n:8}]);
 const resumed=delivery(),spy=vi.spyOn(resumed.target,'deliverBatch');
 const ids=(await f.admin.query('SELECT delivery_id FROM olos_export.projection_outbox')).rows;
 for(const r of ids)expect(await resumed.dispatcher.reconcile(r.delivery_id)).toBe(committed);
 expect(await resumed.dispatcher.runBatch(signal())).toBe(0);expect(spy).not.toHaveBeenCalled();
 expect(await counts()).toEqual({business:committed?8:0,receipts:committed?8:0});
});
it.each([true,false])('settlement ACK loss (committed=%s) preserves receipts and resumes without destination writes',async committed=>{
 await expect(delivery(loseCommit(f.project,3,committed)).dispatcher.runBatch(signal())).rejects.toThrow();
 expect(await counts()).toEqual({business:8,receipts:8});await expire();
 const resumed=delivery(),spy=vi.spyOn(resumed.target,'deliverBatch');
 for(const r of (await f.admin.query("SELECT delivery_id FROM olos_export.projection_outbox WHERE state='uncertain'")).rows)
  expect(await resumed.dispatcher.reconcile(r.delivery_id)).toBe(true);
 expect(await resumed.dispatcher.runBatch(signal())).toBe(0);expect(spy).not.toHaveBeenCalled();
 expect(await states()).toEqual([{state:'confirmed',n:8}]);
});
it('a business SQL error rolls back every receipt and row in the group',async()=>{
 await f.admin.query('ALTER TABLE raw.olos_calls ADD CONSTRAINT synthetic_reject CHECK(internal_id_olos<>4)');
 try{await expect(delivery().dispatcher.runBatch(signal())).rejects.toThrow();expect(await counts()).toEqual({business:0,receipts:0});
  expect(await states()).toEqual([{state:'pending',n:8}]);
 }finally{await f.admin.query('ALTER TABLE raw.olos_calls DROP CONSTRAINT synthetic_reject');}
 await expire();expect(await delivery().dispatcher.runBatch(signal())).toBe(8);
});
it('one stale fence rolls back all authorizations and cannot modify the successor claim',async()=>{
 const d=delivery(),a=await d.outbox.claimBatch(randomUUID());await expire();const b=await d.outbox.claimBatch(randomUUID());
 const mixed=[a[0]!,...b.slice(1)];
 await expect(d.outbox.authorizeBatch(mixed.map(claim=>({claim,row:row(claim)})))).rejects.toMatchObject({code:'LEASE_LOST'});
 expect(await states()).toEqual([{state:'pending',n:8}]);
 expect((await f.admin.query("SELECT count(*)::int n FROM olos_export.projection_events WHERE event='dispatch_authorized'")).rows[0].n).toBe(0);
 expect((await f.admin.query('SELECT DISTINCT lease_owner FROM olos_export.projection_outbox')).rows).toEqual([{lease_owner:b[0]!.owner}]);
});
it('mixed existing/new receipts deduplicate and a conflicting digest rolls back all new writes',async()=>{
 const d=delivery(),claims=await d.outbox.claimBatch(randomUUID()),input=requests(claims);
 expect(await d.target.deliver(input[0]!)).toBe('confirmed');
 const altered={...input[0]!,row:{...input[0]!.row,olos_call_id:'changed'}};
 altered.requestDigest=requestDigest(altered.row,altered.destinationId,altered.contractVersion,altered.mappingVersion);
 await expect(d.target.deliverBatch!([altered,...input.slice(1)])).rejects.toThrow();
 expect(await counts()).toEqual({business:1,receipts:1});
 expect(await d.target.deliverBatch!(input)).toEqual(Array(8).fill('confirmed'));
 expect(await d.target.deliverBatch!(input)).toEqual(Array(8).fill('confirmed'));expect(await counts()).toEqual({business:8,receipts:8});
});
it('SIGTERM drains an already authorized group before returning; no next group is started',async()=>{
 const d=delivery(),abort=new AbortController();let entered!:()=>void,release!:()=>void,finished=false;
 const began=new Promise<void>(r=>{entered=r;}),gate=new Promise<void>(r=>{release=r;}),original=d.target.deliverBatch!.bind(d.target);
 vi.spyOn(d.target,'deliverBatch').mockImplementation(async r=>{entered();await gate;return original(r);});
 const running=d.dispatcher.runBatch(abort.signal).finally(()=>{finished=true;});const rejected=expect(running).rejects.toThrow('STOPPED');
 await began;abort.abort();await Promise.resolve();expect(finished).toBe(false);release();await rejected;
 expect(await counts()).toEqual({business:8,receipts:8});expect(await states()).toEqual([{state:'confirmed',n:8}]);
});
it('a group never claims deliveries from two import batches',async()=>{
 const next=randomUUID();
 await f.admin.query('INSERT INTO olos_export.batches SELECT $1,run_id,stream_id,source_table,digest,expected_version,previous_cursor,next_cursor,state,received_count,persisted_count,duplicate_count,rejected_count,observed_at,committed_at FROM olos_export.batches',[next]);
 await f.admin.query('INSERT INTO olos_export.projection_batches SELECT $1,destination_id,source_table,source_namespace,source_contract_version,field_definitions,destination_version,mapping_version,config_digest,snapshot_digest,row_count,captured_at FROM olos_export.projection_batches',[next]);
 await f.admin.query('INSERT INTO olos_export.projection_outbox(delivery_id,batch_id,destination_id,source_key,payload) SELECT gen_random_uuid(),$1,destination_id,source_key,payload FROM olos_export.projection_outbox LIMIT 1',[next]);
 expect(await delivery().dispatcher.runBatch(signal())).toBe(8);
 expect((await f.admin.query('SELECT state,lease_owner FROM olos_export.projection_outbox WHERE batch_id=$1',[next])).rows).toEqual([{state:'pending',lease_owner:null}]);
 expect(await delivery().dispatcher.runBatch(signal())).toBe(1);expect(await counts()).toEqual({business:8,receipts:9});
});
it('one expired settlement fence rolls back every state change while preserving destination receipts',async()=>{
 const d=delivery(),claims=await d.outbox.claimBatch(randomUUID());
 const auth=await d.outbox.authorizeBatch(claims.map(claim=>({claim,row:row(claim)})));
 await d.target.deliverBatch!(requests(claims));
 await f.admin.query("UPDATE olos_export.projection_outbox SET lease_until=clock_timestamp()-interval '1 second' WHERE delivery_id=$1",[claims[0]!.id]);
 await expect(d.outbox.settleBatch(claims.map((claim,i)=>({claim,attemptId:auth[i]!.attemptId,state:'confirmed'})))).rejects.toMatchObject({code:'LEASE_LOST'});
 expect(await states()).toEqual([{state:'uncertain',n:8}]);expect(await counts()).toEqual({business:8,receipts:8});
 await expire();for(const c of claims)expect(await delivery().dispatcher.reconcile(c.id)).toBe(true);
});
it('concurrent duplicate group receipts remain idempotent',async()=>{
 const d=delivery(),input=requests(await d.outbox.claimBatch(randomUUID()));
 expect(await Promise.all([d.target.deliverBatch!(input),delivery().target.deliverBatch!([...input].reverse())]))
  .toEqual([Array(8).fill('confirmed'),Array(8).fill('confirmed')]);
 expect(await counts()).toEqual({business:8,receipts:8});
});
it('batch SQL retains BIGINT beyond safe JS numbers and SQL DATETIME microseconds',async()=>{
 await f.admin.query(`UPDATE olos_export.projection_outbox SET payload=payload||jsonb_build_object('AttemptsRawDataID','9007199254740993') WHERE source_key='1'`);
 expect(await delivery().dispatcher.runBatch(signal())).toBe(8);
 expect((await f.admin.query(`SELECT internal_id_olos::text,to_char(data_ligacao,'YYYY-MM-DD HH24:MI:SS.US') AS civil
  FROM raw.olos_calls WHERE internal_id_olos=9007199254740993`)).rows[0])
  .toEqual({internal_id_olos:'9007199254740993',civil:'1900-01-01 00:00:00.003333'});
});
it('group retry retains original persisted request snapshots despite later catalog changes',async()=>{
 await f.admin.query('ALTER TABLE raw.olos_calls ADD CONSTRAINT synthetic_reject CHECK(false)');
 try{await expect(delivery().dispatcher.runBatch(signal())).rejects.toThrow();}
 finally{await f.admin.query('ALTER TABLE raw.olos_calls DROP CONSTRAINT synthetic_reject');}
 const digests=(await f.admin.query('SELECT delivery_id,request_digest FROM olos_export.projection_outbox ORDER BY delivery_id')).rows;
 f.catalogs.campaigns.set('0','changed-after-authorization');
 try{await expire();expect(await delivery().dispatcher.runBatch(signal())).toBe(8);
  expect((await f.admin.query('SELECT count(*)::int n FROM raw.olos_calls WHERE nome_campanha_olos IS NOT NULL')).rows[0].n).toBe(0);
  expect((await f.admin.query('SELECT delivery_id,request_digest FROM olos_export.projection_outbox ORDER BY delivery_id')).rows).toEqual(digests);
 }finally{f.catalogs.campaigns.clear();}
});
it('invalid limits and cancellation before claim never authorize or deliver',async()=>{
 const d=delivery(),target=vi.spyOn(d.target,'deliverBatch'),abort=new AbortController();abort.abort();
 await expect(d.dispatcher.runBatch(abort.signal)).rejects.toThrow('STOPPED');
 for(const limit of [0,65,1.5])await expect(d.dispatcher.runBatch(signal(),limit)).rejects.toThrow('VALIDATION_FAILED');
 expect(target).not.toHaveBeenCalled();expect(await counts()).toEqual({business:0,receipts:0});
 expect((await f.admin.query('SELECT count(*)::int n FROM olos_export.projection_events')).rows[0].n).toBe(0);
});
it('group-created states also resume through the unchanged single-delivery protocol',async()=>{
 const d=delivery(),claims=await d.outbox.claimBatch(randomUUID(),4);
 await d.outbox.authorizeBatch(claims.map(claim=>({claim,row:row(claim)})));
 await d.target.deliverBatch!(requests(claims));await expire();
 for(const c of claims)expect(await delivery().dispatcher.reconcile(c.id)).toBe(true);
 for(let i=0;i<4;i++)expect(await delivery().dispatcher.runOnce()).toBe('confirmed');
 expect(await delivery().dispatcher.runOnce()).toBe('empty');expect(await counts()).toEqual({business:8,receipts:8});
});
it('targets without grouped writes retain the original single-delivery protocol',async()=>{
 const d=delivery();Object.defineProperty(d.target,'deliverBatch',{value:undefined});
 const claim=vi.spyOn(d.outbox,'claim'),group=vi.spyOn(d.outbox,'claimBatch');
 expect(await d.dispatcher.runBatch(signal())).toBe(1);expect(claim).toHaveBeenCalledTimes(1);expect(group).not.toHaveBeenCalled();
 expect(await counts()).toEqual({business:1,receipts:1});
});
it('a rejected projection prevents group authorization while preserving the remaining claims',async()=>{
 await f.admin.query("UPDATE olos_export.projection_outbox SET payload=payload||jsonb_build_object('AttemptsRawDataID',NULL)");
 const d=delivery(),target=vi.spyOn(d.target,'deliverBatch');
 await expect(d.dispatcher.runBatch(signal())).rejects.toThrow('DELIVERY_REQUIRES_RECONCILIATION');
 expect(target).not.toHaveBeenCalled();expect(await counts()).toEqual({business:0,receipts:0});
 expect(await states()).toEqual([{state:'pending',n:7},{state:'rejected',n:1}]);
});

it('a confirmed receipt for a later observation does not imply that preserve-existing updated the business row',async()=>{
 const d=delivery(),input=requests(await d.outbox.claimBatch(randomUUID(),1))[0]!;
 const later={...input,deliveryId:randomUUID(),row:{...input.row,olos_call_id:'synthetic-recent'}};
 later.requestDigest=requestDigest(later.row,later.destinationId,later.contractVersion,later.mappingVersion);
 expect(await d.target.deliver(input)).toBe('confirmed');
 expect(await d.target.deliver(later)).toBe('confirmed');
 expect((await f.admin.query('SELECT olos_call_id FROM raw.olos_calls')).rows[0].olos_call_id).toBe(input.row.olos_call_id);
 expect(await counts()).toEqual({business:1,receipts:2});
});

it('the existing mutable writer has no ordering fence: delayed historical delivery can overwrite the recent value',async()=>{
 // UPDATE is deliberately granted only in the disposable fixture, never in production.
 await f.admin.query('GRANT UPDATE ON raw.olos_calls TO olos_export_receiver');
 try{
  const target=new PostgresDeliveryTarget(f.receiver,f.destinationId,new IsolatedTdsCallsWriter(f.profile,false));
  const old=requests(await delivery().outbox.claimBatch(randomUUID(),1))[0]!;
  const recent={...old,deliveryId:randomUUID(),row:{...old.row,olos_call_id:'synthetic-recent'}};
  recent.requestDigest=requestDigest(recent.row,recent.destinationId,recent.contractVersion,recent.mappingVersion);
  expect(await target.deliver(recent)).toBe('confirmed');
  expect(await target.deliver(old)).toBe('confirmed');
  expect((await f.admin.query('SELECT olos_call_id FROM raw.olos_calls')).rows[0].olos_call_id).toBe(old.row.olos_call_id);
  expect(await counts()).toEqual({business:1,receipts:2});
 }finally{await f.admin.query('REVOKE UPDATE ON raw.olos_calls FROM olos_export_receiver');}
});
