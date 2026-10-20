import {afterEach,beforeEach,expect,it,vi} from 'vitest';
import {mkdtemp,rm,readFile,writeFile,stat} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomUUID,createHash} from 'node:crypto';
import {Socket} from 'node:net';
import {Readable} from 'node:stream';
import {createRequire} from 'node:module';
import {Collation} from 'tedious/lib/collation.js';
import {ExactTokenParser,observeMetadata,type TdsField} from '../src/olos/tds/raw-parser.js';
import {ExactConnection} from '../src/olos/tds/connection.js';
import {parserLimits} from '../src/olos/tds/raw-parser.js';
import {replayTdsResponse} from '../src/olos/tds/source.js';
import {LocalSpool} from '../adapters/local-spool.js';
import {spoolLimits} from '../src/large-batch/config.js';
import {largePlan,spoolIdentity} from './support/large-batch-fixture.js';
import {corporateObservationSchema,createCorporateResponseReplayBinding} from '../src/integration/attempts/corporate.js';
import {exampleProfile} from '../src/offline/model.js';

const Debug=createRequire(import.meta.url)('tedious/lib/debug.js') as typeof import('tedious/lib/debug.js').default;
const options={useUTC:true,lowerCaseGuids:false,tdsVersion:'7_4' as const,useColumnNames:false,columnNameReplacer:undefined,camelCaseColumns:false};
const collation=new Collation(1033,0,0,0);
const u16=(v:number)=>{const b=Buffer.alloc(2);b.writeUInt16LE(v);return b;};
function meta(name:string,type:Buffer,nullable:boolean){return Buffer.concat([Buffer.alloc(4),u16(nullable?1:0),type,Buffer.from([name.length]),Buffer.from(name,'utf16le')]);}
const id=Buffer.alloc(8);id.writeBigInt64LE(9223372036854775807n);
const date=Buffer.alloc(8);date.writeUInt32LE(1,4);
const columns=Buffer.concat([Buffer.from([0x81]),u16(3),meta('AttemptsRawDataID',Buffer.from([0x7f]),false),
 meta('StartDate',Buffer.from([0x6f,8]),true),meta('CampaignData',Buffer.concat([Buffer.from([0xe7]),u16(400),collation.toBuffer()]),true)]);
const row=Buffer.concat([Buffer.from([0xd1]),id,Buffer.from([8]),date,u16(65535)]);
function output(name:string){return Buffer.concat([Buffer.from([0xac]),u16(0),Buffer.from([name.length]),Buffer.from(name,'utf16le'),Buffer.from([1]),
 Buffer.alloc(4),u16(1),Buffer.from([0xa7]),u16(4000),collation.toBuffer(),u16(65535)]);}
const ending=Buffer.concat([output('Message'),output('ErrorMessage'),Buffer.from([0x79,0,0,0,0,0xfe]),Buffer.alloc(12)]);
const response=Buffer.concat([columns,row,ending]);
async function collect<T>(iter:AsyncIterable<T>){const a:T[]=[];for await(const value of iter)a.push(value);return a;}
async function observed(){
 const parser=new ExactTokenParser([columns],new Debug({data:false,payload:false,packet:false,token:false}),options,undefined);
 const tokens=await collect(parser.tokens());
 return observeMetadata((tokens[0] as unknown as {columns:Parameters<typeof observeMetadata>[0]}).columns);
}
let root:string;
beforeEach(async()=>{root=await mkdtemp(join(tmpdir(),'olos-observation-'));vi.spyOn(Socket.prototype,'connect').mockImplementation(()=>{throw Error('NO_NETWORK');});});
afterEach(async()=>{expect(Socket.prototype.connect).not.toHaveBeenCalled();vi.restoreAllMocks();await rm(root,{recursive:true,force:true});});
const limits=()=>spoolLimits({minFreeBytes:0,bufferBytes:1024});

it('observes actual ordinal/type/nullability/length/collation while preserving BIGINT, DATETIME ticks and null',async()=>{
 const fields=await observed();
 expect(fields.map(f=>[f.name,f.type,f.nullable])).toEqual([['AttemptsRawDataID','bigint',false],['StartDate','datetime',true],['CampaignData','nvarchar',true]]);
 expect(fields[2]).toMatchObject({sqlLength:200,collationHex:collation.toBuffer().toString('hex')});
 const plan={...largePlan(),fields:fields.map(({sqlLength:_,collationHex:_c,...f})=>f)};
 const source=replayTdsResponse(fields,async function*(){for(let i=0;i<response.length;i++)yield response.subarray(i,i+1);},1);
 const events=await collect(source.read(plan as never,new AbortController().signal));
 expect(events).toEqual([{kind:'row',values:[id,date,null]},{kind:'complete',returnValue:0,errorMessage:null,datasetCount:1}]);
});
it('the real per-connection parser hook awaits the durable sink before emitting any row',async()=>{
 const connection=new ExactConnection({server:'127.0.0.1',options:{database:'phase3d_fixture',serverName:'localhost',encrypt:true,trustServerCertificate:false,cryptoCredentialsDetails:{ca:'synthetic-unused-offline'}}});
 let release!:()=>void,entered!:()=>void;
 const gate=new Promise<void>(resolve=>{release=resolve;}),started=new Promise<void>(resolve=>{entered=resolve;});
 const rows:unknown[]=[];let captured:Buffer|undefined;
 connection.responseCapture=async chunk=>{entered();await gate;captured=Buffer.from(chunk);};
 connection.acquisition={fields:undefined,limits:parserLimits,token(){},fail(){throw Error('PARSER_FAILED');}};
 class RequestTokenHandler {onColMetadata(){} onRow(t:unknown){rows.push(t);} onReturnValue(){} onReturnStatus(){} onDoneProc(){}}
 const parser=connection.createTokenStreamParser(Readable.from([response]) as never,new RequestTokenHandler() as never);
 const ended=new Promise<void>(resolve=>parser.on('end',resolve));
 await started;expect(rows).toHaveLength(0);expect(captured).toBeUndefined();release();await ended;
 expect(captured).toEqual(response);expect(rows).toHaveLength(1);connection.close();
});
it('quarantines exact response privately, retaining claim across restart and replay without approving a contract',async()=>{
 const fields=await observed(),repo=await LocalSpool.open(root,limits()),attemptId=randomUUID(),capture=await repo.createResponseCapture(attemptId);
 for(let i=0;i<response.length;i+=19)await capture.append(response.subarray(i,i+19));
 await capture.seal({fields,received:1,contractApproved:true});
 const m=await repo.responseCapture(attemptId);expect(m.sha256).toBe(createHash('sha256').update(response).digest('hex'));
 const path=join(root,'batches',attemptId,'response.ready');expect((await stat(path)).mode&0o777).toBe(0o600);
 expect(JSON.parse(await readFile(join(root,'batches',attemptId,'response.json'),'utf8'))).toMatchObject({state:'sealed',contractApproved:false,businessDelivered:false,checkpointAdvanced:false});
 const restarted=await LocalSpool.open(root,limits());expect(Buffer.concat(await collect(restarted.responseChunks(attemptId)))).toEqual(response);
 await expect(restarted.create(spoolIdentity())).rejects.toMatchObject({code:'BUSY'});
 await expect(restarted.createResponseCapture(randomUUID())).rejects.toMatchObject({code:'INCOMPLETE'});
});
it('rejects a partial response, mismatching layout, missing final DONE/OUTPUT and corruption before completion',async()=>{
 const fields=await observed(),plan={...largePlan(),fields:fields.map(({sqlLength:_,collationHex:_c,...f})=>f)};
 for(const bytes of [Buffer.concat([columns,row]),response.subarray(0,response.length-1)]){
   await expect(collect(replayTdsResponse(fields,async function*(){yield bytes;},1).read(plan as never,new AbortController().signal))).rejects.toThrow();
 }
 await expect(collect(replayTdsResponse([{...fields[0]!,nullable:true},...fields.slice(1)],async function*(){yield response;},1).read(plan as never,new AbortController().signal))).rejects.toThrow();
 const repo=await LocalSpool.open(root,limits()),attemptId=randomUUID(),capture=await repo.createResponseCapture(attemptId);
 await capture.append(response);await capture.seal({fields,received:1});
 const corrupt=Buffer.from(response);corrupt[corrupt.length-1]=1;
 await writeFile(join(root,'batches',attemptId,'response.ready'),corrupt);
 await expect(collect(repo.responseChunks(attemptId))).rejects.toMatchObject({code:'CORRUPT'});
});
it('disk failure leaves an incomplete case, never a ready response',async()=>{
 let armed=false;
 const repo=await LocalSpool.open(root,limits(),{async before(op){if(armed && op==='write')throw Object.assign(Error('private path'),{code:'ENOSPC'});}});
 const attemptId=randomUUID(),capture=await repo.createResponseCapture(attemptId);armed=true;
 await expect(capture.append(response)).rejects.toMatchObject({code:'DISK_FULL'});await capture.interrupt();
 await expect(repo.responseCapture(attemptId)).rejects.toThrow();
 await expect(readFile(join(root,'batches',attemptId,'response.ready'))).rejects.toMatchObject({code:'ENOENT'});
});
it('observation configuration has no fictitious CurrentDay/PreviousDays contract, and observed fields alone cannot approve replay',()=>{
 const cfg={version:'olos-corporate-local/v1',contractMode:'observe',endpoint:{},pinApprovalFile:'/private/pin.json',profile:exampleProfile,
 campaignFile:'/private/c.json',dispositionFile:'/private/d.json',authorization:{operator:'test',reference:'test',issuedAt:new Date().toISOString(),expiresAt:new Date(Date.now()+60000).toISOString(),sourceTimeZone:'UTC',sharedConsumptionEvidence:'real review required',recoveryEvidence:'real review required'}};
 expect(corporateObservationSchema.parse(cfg)).not.toHaveProperty('current');
 expect(corporateObservationSchema.safeParse({...cfg,current:{plan:largePlan(),fields:[]}}).success).toBe(false);
 expect(()=>createCorporateResponseReplayBinding({current:{plan:largePlan(),fields:[]}} as never,{campaigns:new Map(),dispositions:new Map(),agents:new Map()},{} as never,[],'2020-06-03')).toThrow();
});
