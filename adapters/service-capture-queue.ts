import {lstat,readdir,open,rename,statfs} from 'node:fs/promises';
import {constants} from 'node:fs';
import {join} from 'node:path';
import {z} from 'zod';
import {privateDirectory,privateJson,durableExclusive} from './disposition-integration.js';
import {digest} from '../src/persistence/validation.js';

const sha=z.string().regex(/^[a-f0-9]{64}$/);
export const queuedCaptureSchema=z.object({kind:z.literal('olos-capture-queue/v1'),captureId:z.string().uuid(),
 ordinal:z.number().int().positive().safe(),captureSha256:sha,intentDigest:sha,reviewDigest:sha,configurationDigest:sha,
 pendingDigest:sha}).strict();
export type QueuedCapture=z.infer<typeof queuedCaptureSchema>;
export const queueLimitsSchema=z.object({maxCaptures:z.number().int().min(1).max(128).default(8),
 maxBytes:z.number().int().min(2*1024**3).max(1024**4).default(8*1024**3)}).strict();
export type CaptureQueueLimits=z.infer<typeof queueLimitsSchema>;
export async function exists(path:string){try{await lstat(path);return true;}catch(e){if((e as NodeJS.ErrnoException).code==='ENOENT')return false;throw e;}}
async function sync(path:string){const fd=await open(path,constants.O_RDONLY|constants.O_DIRECTORY|constants.O_NOFOLLOW);try{await fd.sync();}finally{await fd.close();}}

/** Caller owns the instance advisory lock. Never manufactures a delivery acknowledgment. */
export async function handoffCapture(base:string,root:string,entry:QueuedCapture){
 queuedCaptureSchema.parse(entry);
 const dir=join(root,'cases',entry.captureId),record=join(dir,'queued.json'),pending=join(base,'remote-pending.json'),archive=join(dir,'remote-pending.captured.json');
 await privateDirectory(dir);
 const source=await exists(pending)?pending:archive;
 const marker=await privateJson(source) as {attemptId?:string;workDir?:string;configurationDigest?:string};
 if(marker.attemptId!==entry.captureId || marker.workDir!==root || marker.configurationDigest!==entry.configurationDigest || digest(marker)!==entry.pendingDigest)throw Error('CAPTURE_QUEUE_REQUIRES_REVIEW');
 if(await exists(record)){if(digest(queuedCaptureSchema.parse(await privateJson(record)))!==digest(entry))throw Error('CAPTURE_QUEUE_REQUIRES_REVIEW');}
 else {if(source!==pending)throw Error('CAPTURE_QUEUE_REQUIRES_REVIEW');await durableExclusive(record,entry);}
 if(source===pending){
  if(await exists(archive) || digest(await privateJson(pending))!==entry.pendingDigest)throw Error('CAPTURE_QUEUE_REQUIRES_REVIEW');
  await rename(pending,archive);
 }
 // fsync both directories even on recovery after rename but before fsync.
 await sync(base);await sync(dir);
}

async function retainedBytes(path:string):Promise<number>{
 // The local consumer may durably rename/remove its temporary/active marker during admission.
 let st;try{st=await lstat(path);}catch(error){if((error as NodeJS.ErrnoException).code==='ENOENT')return 0;throw error;}
 if(st.isSymbolicLink() || st.uid!==process.getuid?.() || st.mode&0o077)throw Error('CAPTURE_QUEUE_REQUIRES_REVIEW');
 if(st.isFile())return st.size;
 if(!st.isDirectory())throw Error('CAPTURE_QUEUE_REQUIRES_REVIEW');
 let total=0;for(const name of await readdir(path))total+=await retainedBytes(join(path,name));return total;
}
/** Reserve worst-case raw + replay storage before the consumptive RPC. Retention is never deleted here. */
export async function captureAdmission(root:string,pending:number,unmaterialized:number,limits:CaptureQueueLimits,spoolMaxBytes:number,minFreeBytes:number){
 const quota=queueLimitsSchema.parse(limits),used=await retainedBytes(root),disk=await statfs(root);
 const reserve=(unmaterialized+2)*spoolMaxBytes;
 const free=Number(disk.bavail)*Number(disk.bsize);
 const reason=pending>=quota.maxCaptures?'CAPTURE_QUEUE_COUNT_LIMIT':used+reserve>quota.maxBytes?'CAPTURE_QUEUE_BYTES_LIMIT':free<reserve+minFreeBytes?'CAPTURE_QUEUE_DISK_LIMIT':null;
 return {allowed:reason===null,reason,pending,retainedBytes:used,reservedBytes:reserve,maxCaptures:quota.maxCaptures,maxBytes:quota.maxBytes};
}

/** Three recent groups, then one oldest group. A single caller owns all delivery work. */
export class FairCaptureDelivery {
 private turn=0;
 select<T extends {ordinal:number}>(entries:readonly T[]):T|undefined{
  if(!entries.length)return undefined;
  const sorted=[...entries].sort((a,b)=>a.ordinal-b.ordinal);
  return (++this.turn%4===0?sorted[0]:sorted.at(-1));
 }
}
