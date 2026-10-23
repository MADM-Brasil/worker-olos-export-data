import {it,expect} from 'vitest';
import {writeFile} from 'node:fs/promises';
import {projectionLoadFixture} from './support/projection-load-fixture.js';
import {FairCaptureDelivery} from '../adapters/service-capture-queue.js';

it.skipIf(process.env.OLOS_PROJECTION_LOAD!=='1')('inserts 200000 synthetic deliveries with recent priority and historical progress, resuming receipts',async()=>{
 const f=await projectionLoadFixture(),signal=new AbortController().signal,rows=200000;let progress:ReturnType<typeof setInterval>|undefined;
 try{
  const history=await f.seed(rows-256),d=f.delivery();let historical=0,recent=0,total=0;
  const start=performance.now();
  // A durable new capture arrives while historical work is still pending.
  for(let n=0;n<8;n++){const count=await d.dispatcher.runBatch(signal,64,history);historical+=count;total+=count;}
  const latest=await f.seed(256,true),entries=[{ordinal:1,id:history},{ordinal:2,id:latest}],fair=new FairCaptureDelivery();
  const arrival=performance.now();let recentMs=0,groups=0;
  progress=setInterval(()=>process.stdout.write(JSON.stringify({event:'isolated_queue_load',historical,recent,total})+'\n'),30000);
  while(entries.length){
   const selected=fair.select(entries)!;
   // Recreate the dispatcher to exercise state independent of its process-local owner.
   const dispatcher=groups++===4?f.delivery().dispatcher:d.dispatcher;
   const n=await dispatcher.runBatch(signal,64,selected.id);total+=n;
   if(selected.id===history)historical+=n;else recent+=n;
   if(n<64 || selected.id===latest && recent===256 || selected.id===history && historical===rows-256)entries.splice(entries.indexOf(selected),1);
   if(recent===256 && !recentMs){recentMs=performance.now()-arrival;expect(historical).toBe(576);expect(total).toBeLessThan(rows);}
  }
  const verified=(await f.admin.query(`SELECT
   (SELECT count(*)::int FROM raw.olos_calls) AS business,
   (SELECT count(*)::int FROM olos_export.delivery_receipts WHERE state='confirmed') AS receipts,
   (SELECT count(*)::int FROM olos_export.projection_outbox WHERE state='confirmed') AS confirmed,
   (SELECT count(*)::int FROM olos_export.projection_outbox WHERE state<>'confirmed') AS pending,
   (SELECT count(*)::int FROM olos_export.projection_outbox o JOIN olos_export.delivery_receipts r USING(destination_id,delivery_id) WHERE o.request_digest<>r.request_digest) AS mismatches`)).rows[0];
  expect(total).toBe(rows);expect(verified).toEqual({business:rows,receipts:rows,confirmed:rows,pending:0,mismatches:0});
  const report={node:process.versions.node,postgres:'16',rows,recentRows:recent,historicalRows:historical,
   recentCompletionMs:Math.round(recentMs),totalMs:Math.round(performance.now()-start),verified,productionAccessed:false};
  await writeFile('/tmp/olos-independent-delivery-load.json',JSON.stringify(report,null,2),{mode:0o600});process.stdout.write(JSON.stringify(report)+'\n');
 }finally{clearInterval(progress);await f.stop();}
},900000);
