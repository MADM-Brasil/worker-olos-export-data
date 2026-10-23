import {writeFile} from 'node:fs/promises';
import {it,expect} from 'vitest';
import {projectionLoadFixture} from './support/projection-load-fixture.js';

// Explicit load test: both start with 200k pending rows. The old path is measured
// over the same initial 1024 rows, avoiding an hours-long quadratic baseline.
// The optimized path must additionally finish and verify all 200k deliveries.
it.skipIf(process.env.OLOS_PROJECTION_LOAD!=='1')('compares old/new delivery on a 200,000-row queue and confirms the entire optimized batch',async()=>{
 const f=await projectionLoadFixture();
 const rows=200000,window=1024;
 let sample:ReturnType<typeof setInterval>|undefined,progress:ReturnType<typeof setInterval>|undefined;
 try{
  const metrics:{mode:string;queueRows:number;delivered:number;milliseconds:number;rowsPerSecond:number;peakHeap:number;peakRss:number}[]=[];
  for(const mode of ['runOnce','runBatch']){
   await f.seed(rows);const d=f.delivery();let delivered=0;
   let peakHeap=0,peakRss=0;
   const memory=()=>{const m=process.memoryUsage();peakHeap=Math.max(peakHeap,m.heapUsed);peakRss=Math.max(peakRss,m.rss);};
   sample=setInterval(memory,100);progress=setInterval(()=>process.stdout.write(JSON.stringify({mode,delivered,queueRows:rows})+'\n'),30000);
   const start=performance.now();let windowMs=0;
   if(mode==='runOnce'){
    while(delivered<window){expect(await d.dispatcher.runOnce()).toBe('confirmed');delivered++;}
    windowMs=performance.now()-start;
   }else{
    while(delivered<rows){const count=await d.dispatcher.runBatch(new AbortController().signal);expect(count).toBeGreaterThan(0);delivered+=count;
     if(delivered===window)windowMs=performance.now()-start;}
    expect(await d.dispatcher.runBatch(new AbortController().signal)).toBe(0);
   }
   memory();const elapsed=performance.now()-start;
   clearInterval(sample);clearInterval(progress);sample=undefined;progress=undefined;
   metrics.push({mode,queueRows:rows,delivered:window,milliseconds:Math.round(windowMs),rowsPerSecond:Math.round(window*1000/windowMs),peakHeap,peakRss});
   if(mode==='runBatch')metrics.push({mode:'runBatch-complete',queueRows:rows,delivered,milliseconds:Math.round(elapsed),rowsPerSecond:Math.round(delivered*1000/elapsed),peakHeap,peakRss});
   const verified=(await f.admin.query(`SELECT
    (SELECT count(*)::int FROM raw.olos_calls) AS business,
    (SELECT count(*)::int FROM olos_export.delivery_receipts WHERE state='confirmed') AS receipts,
    (SELECT count(*)::int FROM olos_export.projection_outbox WHERE state='confirmed') AS confirmed,
    (SELECT count(*)::int FROM olos_export.projection_events) AS events,
    (SELECT count(*)::int FROM olos_export.projection_outbox o
      JOIN olos_export.delivery_receipts r USING(destination_id,delivery_id)
      WHERE o.request_digest<>r.request_digest) AS mismatches`)).rows[0];
   expect(verified).toEqual({business:delivered,receipts:delivered,confirmed:delivered,events:delivered*3,mismatches:0});
  }
  const speedup=metrics[0]!.milliseconds/metrics[1]!.milliseconds;
  const report={node:process.versions.node,postgres:'16',poolConnections:{project:2,receiver:2},claimBatchSize:64,
   baselineScope:'first 1024 deliveries from 200000 pending; not a complete baseline run',metrics,speedup:Math.round(speedup*100)/100,
   verifiedOptimizedRows:rows,productionAccessed:false};
  await writeFile('/tmp/olos-projection-throughput.json',JSON.stringify(report,null,2),{mode:0o600});
  process.stdout.write(JSON.stringify(report)+'\n');
  expect(speedup).toBeGreaterThan(2);
 }finally{clearInterval(sample);clearInterval(progress);await f.stop();}
},900000);
