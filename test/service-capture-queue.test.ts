import {afterEach,beforeEach,it,expect} from 'vitest';
import {mkdtemp,mkdir,writeFile,readFile,rm,rename,symlink} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomUUID} from 'node:crypto';
import {handoffCapture,captureAdmission,FairCaptureDelivery,queueLimitsSchema,type QueuedCapture} from '../adapters/service-capture-queue.js';
import {digest} from '../src/persistence/validation.js';
let base:string,root:string,dir:string,entry:QueuedCapture;
beforeEach(async()=>{
 base=await mkdtemp(join(tmpdir(),'olos-queue-'));root=join(base,'service-run');const id=randomUUID();dir=join(root,'cases',id);await mkdir(dir,{recursive:true,mode:0o700});
 const pending={attemptId:id,workDir:root,configurationDigest:'a'.repeat(64)};
 await writeFile(join(base,'remote-pending.json'),JSON.stringify(pending),{mode:0o600});
 entry={kind:'olos-capture-queue/v1',captureId:id,ordinal:1,captureSha256:'b'.repeat(64),intentDigest:'c'.repeat(64),reviewDigest:'d'.repeat(64),configurationDigest:pending.configurationDigest,pendingDigest:digest(pending)};
});
afterEach(async()=>{await rm(base,{recursive:true,force:true});});
it.each(['before-record','after-record','after-rename'])('recovers durable handoff at %s and repeats without erasing evidence',async point=>{
 if(point!=='before-record')await writeFile(join(dir,'queued.json'),JSON.stringify(entry),{mode:0o600});
 if(point==='after-rename')await rename(join(base,'remote-pending.json'),join(dir,'remote-pending.captured.json'));
 await handoffCapture(base,root,entry);await handoffCapture(base,root,entry);
 expect(JSON.parse(await readFile(join(dir,'queued.json'),'utf8'))).toEqual(entry);
 expect(digest(JSON.parse(await readFile(join(dir,'remote-pending.captured.json'),'utf8')))).toBe(entry.pendingDigest);
 await expect(readFile(join(base,'remote-pending.json'))).rejects.toMatchObject({code:'ENOENT'});
 await expect(readFile(join(dir,'delivered.json'))).rejects.toMatchObject({code:'ENOENT'});
});
it('refuses changed evidence and never archives a newer pending on completion of an older capture',async()=>{
 await writeFile(join(dir,'queued.json'),JSON.stringify({...entry,captureSha256:'e'.repeat(64)}),{mode:0o600});
 await expect(handoffCapture(base,root,entry)).rejects.toThrow('CAPTURE_QUEUE_REQUIRES_REVIEW');
 const next={attemptId:randomUUID(),workDir:root,configurationDigest:entry.configurationDigest};
 await writeFile(join(base,'remote-pending.json'),JSON.stringify(next),{mode:0o600});
 await expect(handoffCapture(base,root,entry)).rejects.toThrow('CAPTURE_QUEUE_REQUIRES_REVIEW');
 expect(JSON.parse(await readFile(join(base,'remote-pending.json'),'utf8'))).toEqual(next);
});
it('reserves raw plus local replay space, includes retained files, and stops admission at count/byte limits',async()=>{
 const limits=queueLimitsSchema.parse({});
 expect((await captureAdmission(root,0,0,limits,512*1024**2,0)).allowed).toBe(true);
 expect((await captureAdmission(root,8,0,limits,512*1024**2,0)).reason).toBe('CAPTURE_QUEUE_COUNT_LIMIT');
 expect((await captureAdmission(root,1,1,{...limits,maxBytes:2*1024**3},1024**3,0)).reason).toBe('CAPTURE_QUEUE_BYTES_LIMIT');
 expect((await captureAdmission(root,0,0,limits,1,Number.MAX_SAFE_INTEGER)).reason).toBe('CAPTURE_QUEUE_DISK_LIMIT');
 await symlink('/tmp',join(root,'foreign'));await expect(captureAdmission(root,0,0,limits,1,0)).rejects.toThrow('CAPTURE_QUEUE_REQUIRES_REVIEW');
});
it('a constant flow of newer captures cannot starve the oldest batch',()=>{
 const fair=new FairCaptureDelivery(),history={ordinal:1};let historicalGroups=0;
 for(let n=2;n<=1001;n++){const picked=fair.select([history,{ordinal:n}]);if(picked===history)historicalGroups++;else expect(picked!.ordinal).toBe(n);}
 expect(historicalGroups).toBe(250);
});
