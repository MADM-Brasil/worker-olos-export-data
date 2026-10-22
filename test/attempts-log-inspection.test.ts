import {beforeAll,afterAll,beforeEach,afterEach,it,expect,vi} from 'vitest';
import {mkdtemp,rm,readFile,writeFile,chmod,stat,readdir} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomUUID,X509Certificate,createHash} from 'node:crypto';
import * as access from '../src/olos/tds/attempts-access.js';
import {InspectionDiagnostics,inspectionErrorCode,reportInspectionFailure} from '../src/olos/tds/inspection-diagnostics.js';
import {OlosError} from '../src/olos/types.js';
import {Socket} from 'node:net';
import {Request} from 'tedious';
import {Pool} from 'pg';
import {parseCorporateArguments,corporateCommand} from '../adapters/attempts-corporate.js';
import {prepareLogInspection,executeLogInspection,logInspectionConfigSchema,type LogInspectionConfig} from '../adapters/attempts-log-inspection.js';
import {parseOlosPinApproval,type OlosPinApproval} from '../src/security/olos-certificate-pin.js';
import {syntheticCertificates} from './support/tls-diagnostic-fixture.js';
import {reportedSignature} from '../src/olos/procedure-signatures.js';
import {prepareCorporateAttemptsEndpoint,authorizeCorporateLogInspection,authorizeCorporateAttemptsOnce,assertPreparedAttemptsCall,claimAttemptsConnection,claimAttemptsSocket,claimAttemptsLogin,assertPreparedAttemptsRpc,assertCorporateAttemptsExecution} from '../src/olos/tds/attempts-access.js';
import {IsolatedTdsSource,prepareTdsRequest} from '../src/olos/tds/source.js';
import {ExactConnection} from '../src/olos/tds/connection.js';
import {prepareCall} from '../src/olos/client.js';
let certs:Awaited<ReturnType<typeof syntheticCertificates>>,root:string,pin:OlosPinApproval,cfg:LogInspectionConfig;
const signature=reportedSignature('LogReadData',{evidence:'synthetic server signature',nullable:{tablename:false,DateExport:false,Message:true,ErrorMessage:true}});
const values={tablename:'AttemptsRawData',DateExport:'2026-10-22'};
const endpointConfig={server:'10.196.234.11',port:1433,database:'ExportDataV3',user:'OEC',password:'SYNTHETIC_ONLY'};
beforeAll(async()=>{certs=await syntheticCertificates();},30000);
afterAll(async()=>{await certs.stop();});
beforeEach(async()=>{
 vi.spyOn(Pool.prototype,'connect').mockImplementation(()=>{throw Error('POSTGRESQL_FORBIDDEN');});
 root=await mkdtemp(join(tmpdir(),'olos-log-'));
 vi.spyOn(Socket.prototype,'connect').mockImplementation(()=>{throw Error('NETWORK_FORBIDDEN');});
 pin=parseOlosPinApproval({version:1,purpose:'OLOS_ATTEMPTS_TDS_ACQUISITION',host:endpointConfig.server,port:1433,database:'ExportDataV3',approvalId:'synthetic-pin',approvedBy:'test',
 approvedAt:new Date(Date.now()-1000).toISOString(),expiresAt:new Date(Date.now()+60000).toISOString(),fingerprintSha256:new X509Certificate(certs.selfSigned).fingerprint256,certificatePem:certs.selfSigned});
 cfg={version:'olos-log-inspection-config/v1',endpoint:endpointConfig,pinApprovalFile:'/private/pin.json',authorization:{scope:'OLOS_LOGREADDATA_INSPECTION_ONLY',operationId:randomUUID(),operator:'test-operator',reference:'test-ticket-123',procedure:'LogReadData',tableName:'AttemptsRawData',dateExport:values.DateExport,
 issuedAt:new Date(Date.now()-1000).toISOString(),expiresAt:new Date(Date.now()+60000).toISOString(),pinApprovalId:pin.approvalId,fingerprintSha256:pin.fingerprintSha256}};
});
afterEach(async()=>{expect(Pool.prototype.connect).not.toHaveBeenCalled();expect(Socket.prototype.connect).not.toHaveBeenCalled();vi.restoreAllMocks();await rm(root,{recursive:true,force:true});});
const cli=['inspect-log','--confirm','AUTHORIZE_LOG_INSPECTION','--date-export','2026-10-22'];
it('CLI recognizes isolated action and requires confirmation/date, rejects unknown/duplicate/incompatible flags',()=>{
 expect(parseCorporateArguments(cli,{})).toMatchObject({action:'inspect-log',dateExport:'2026-10-22'});
 for(const args of [['inspect-log'],['inspect-log','--confirm','AUTHORIZE_LOG_INSPECTION'],['inspect-log','--date-export','2026-10-22'],
 [...cli,'--anything','x'],[...cli,'--date-export','2026-10-22'],[...cli,'--interval-ms','60000'],[...cli,'--work-dir',root],
 ['once','--date-export','2026-10-22'],['recover','--confirm','AUTHORIZE_LOG_INSPECTION'],['status','--confirm','AUTHORIZE_LOG_INSPECTION']])expect(()=>parseCorporateArguments(args,{})).toThrow();
 for(const date of ['2026-02-29','2026-13-01','0000-01-01','2026-1-01','2026-10-22T00:00:00Z','NULL'])expect(()=>parseCorporateArguments([...cli.slice(0,-1),date],{})).toThrow();
});
it('CLI without private inspection authorization never opens a source',async()=>{
 await expect(corporateCommand([...cli,'--config',join(root,'absent.json')],{})).rejects.toThrow('LOG_INSPECTION_PRIVATE_AUTHORIZATION_REQUIRED');
});
it.each(['operator','reference'])('missing or blank %s fails private schema',key=>{
 expect(logInspectionConfigSchema.safeParse({...cfg,authorization:{...cfg.authorization,[key]:'  '}}).success).toBe(false);
});
it.each(['fingerprint','date','expiry'])('rejects divergent %s before preparing state',async field=>{
 const changed=structuredClone(cfg);
 if(field==='fingerprint')changed.authorization.fingerprintSha256='wrong';
 if(field==='date')changed.authorization.dateExport='2026-10-21';
 if(field==='expiry')changed.authorization.expiresAt=new Date(Date.now()-100).toISOString();
 await expect(executeLogInspection(changed,pin,root,values.DateExport)).rejects.toThrow();expect(await readdir(root)).toEqual([]);
});
async function prepared(){
 const ep=prepareCorporateAttemptsEndpoint(endpointConfig,pin,signature,values),p=await prepareLogInspection(root,cfg,pin,values.DateExport);
 return {ep,p,authorize:()=>authorizeCorporateLogInspection(ep,()=>{},async()=>({intentPath:p.intentPath,pendingPath:p.pendingPath}),p.intent)};
}
it('durable exclusive intent binds exact operation/config/pin and capture exists before permit',async()=>{
 const {ep,p,authorize}=await prepared();
 expect(JSON.parse(await readFile(p.intentPath,'utf8'))).toMatchObject({state:'PREPARED',scope:cfg.authorization.scope,dateExport:values.DateExport,fingerprintSha256:pin.fingerprintSha256});
 expect((await stat(p.workDir)).mode&0o777).toBe(0o700);expect((await stat(p.intentPath)).mode&0o777).toBe(0o600);
 await stat(join(p.workDir,'spool','batches',p.attemptId,'response.partial'));
 await authorize();expect(()=>assertCorporateAttemptsExecution(ep)).not.toThrow();
 await expect(authorize()).rejects.toThrow();
 const another=prepareCorporateAttemptsEndpoint(endpointConfig,pin,signature,values);
 await expect(authorizeCorporateLogInspection(another,()=>{},async()=>({intentPath:p.intentPath,pendingPath:p.pendingPath}),p.intent)).rejects.toThrow();
 await p.capture.interrupt();
 await expect(prepareLogInspection(root,cfg,pin,values.DateExport)).rejects.toMatchObject({code:'EEXIST'});
});
it.each(['missing','tamper','permissions','pending','procedure','fingerprint','date'])('rejects %s intent/pending before issuing permission',async kind=>{
 const {p,authorize}=await prepared();
 if(kind==='missing')await rm(p.intentPath);
 else if(kind==='permissions')await chmod(p.intentPath,0o644);
 else if(kind==='pending')await writeFile(p.pendingPath,'{}');
 else{const content={...p.intent,...(kind==='procedure'?{procedure:'ReReadData'}:kind==='fingerprint'?{fingerprintSha256:'wrong'}:kind==='date'?{dateExport:'2026-10-21'}:{operator:'attacker'})};await writeFile(p.intentPath,JSON.stringify(content));}
 await expect(authorize()).rejects.toThrow();await p.capture.interrupt();
});
it('rechecks durable intent at subsequent connection/RPC boundaries',async()=>{
 const {ep,p,authorize}=await prepared();await authorize();await writeFile(p.intentPath,'{}');
 expect(()=>assertCorporateAttemptsExecution(ep)).toThrow();await p.capture.interrupt();
});
it.each(['ReadDataCurrentDay','ReReadData','ResetReadData','DataAvailable'])('inspection permission rejects %s RPC',async proc=>{
 const {ep,p,authorize}=await prepared();await authorize();assertPreparedAttemptsCall(ep,prepareCall(signature,values,300000));claimAttemptsConnection(ep);claimAttemptsSocket(ep);claimAttemptsLogin(ep);
 const request=new Request(`dbo.usp_ExportData_${proc}`,()=>{});prepareTdsRequest(request,signature,values,300000);
 expect(()=>assertPreparedAttemptsRpc(request,ep)).toThrow();await p.capture.interrupt();
});
it.each(['date','order','type','length','extra','output-value'])('rejects changed Tedious %s parameter',async kind=>{
 const {ep,p,authorize}=await prepared();await authorize();assertPreparedAttemptsCall(ep,prepareCall(signature,values,300000));claimAttemptsConnection(ep);claimAttemptsSocket(ep);claimAttemptsLogin(ep);
 const request=new Request('dbo.usp_ExportData_LogReadData',()=>{});prepareTdsRequest(request,signature,values,300000);
 if(kind==='date')request.parameters[1]!.value=new Date('2026-10-21T00:00:00Z');
 if(kind==='order')request.parameters.reverse();
 if(kind==='length')request.parameters[0]!.length=127;
 if(kind==='extra')request.parameters.push(request.parameters[0]!);
 if(kind==='type')request.parameters[1]!.type=request.parameters[0]!.type;
 if(kind==='output-value')request.parameters[2]!.value='unauthorized';
 expect(()=>assertPreparedAttemptsRpc(request,ep)).toThrow();await p.capture.interrupt();
});
it('exact Tedious request succeeds once; second RPC and acquisition permission are rejected',async()=>{
 const {ep,p,authorize}=await prepared();await expect(authorizeCorporateAttemptsOnce(ep,()=>{},async()=>({intentPath:p.intentPath,pendingPath:p.pendingPath}))).rejects.toThrow();
 await authorize();assertPreparedAttemptsCall(ep,prepareCall(signature,values,300000));claimAttemptsConnection(ep);claimAttemptsSocket(ep);claimAttemptsLogin(ep);
 const request=new Request('dbo.usp_ExportData_LogReadData',()=>{});prepareTdsRequest(request,signature,values,300000);
 expect(()=>assertPreparedAttemptsRpc(request,ep)).not.toThrow();expect(()=>assertPreparedAttemptsRpc(request,ep)).toThrow();await p.capture.interrupt();
});
it('existing acquisition pending blocks inspection, preserving its evidence',async()=>{
 const pending=join(root,'remote-pending.json');await writeFile(pending,'original',{mode:0o600});
 await expect(prepareLogInspection(root,cfg,pin,values.DateExport)).rejects.toThrow('REMOTE_OPERATION_REQUIRES_REVIEW');expect(await readFile(pending,'utf8')).toBe('original');
});
it.each(['complete','partial','parse-error'])('preserves %s response and digest independently, no retries or PostgreSQL',async mode=>{
 const bytes=Buffer.from('SYNTHETIC private response bytes'),called=vi.fn();
 const result=await executeLogInspection(cfg,pin,root,values.DateExport,(...args)=>{
  const s=new IsolatedTdsSource(...args);
  vi.spyOn(s,'call').mockImplementation(async function*(){
   called();const work=join(root,'log-inspections',cfg.authorization.operationId);
   await stat(join(work,'intent.json'));await stat(join(work,'remote-outcome-unknown.json'));
   await stat(join(work,'spool','batches',cfg.authorization.operationId,'response.partial'));
   await args[3]!(bytes);
   if(mode==='partial')throw Error('PRIVATE connection failure');
   for await(const _ of args[4]!()){};
   if(mode==='parse-error')throw Error('unsupported private layout');
   s.responseObservation.returnValue=0;s.responseObservation.datasetCount=1;
   yield {kind:'complete',datasetCount:1,returnValue:0,errorMessage:null,message:null,output:{Message:null,ErrorMessage:null}};
  });return s;
 });
 expect(called).toHaveBeenCalledTimes(1);expect(result).toMatchObject({retryRemote:false,persisted:0,checkpointAdvanced:false,reviewRequired:true,rangeAttribution:'REMOTE_RANGE_ATTRIBUTION_NOT_ESTABLISHED'});
 expect(JSON.stringify(result)).not.toContain('private');
 const work=join(root,'log-inspections',cfg.authorization.operationId),dir=join(work,'spool','batches',cfg.authorization.operationId);
 expect(await readFile(join(dir,mode==='partial'?'response.partial':'response.ready'))).toEqual(bytes);
 const descriptor=JSON.parse(await readFile(join(dir,'response.json'),'utf8'));
 expect(descriptor.sha256).toBe(createHash('sha256').update(bytes).digest('hex'));
 expect(descriptor.state).toBe(mode==='partial'?'incomplete':'sealed');
 expect(result.state).toBe(mode==='partial'?'REMOTE_OUTCOME_UNKNOWN':mode==='parse-error'?'REVIEW_REQUIRED':'RESPONSE_SEALED');
 await expect(executeLogInspection(cfg,pin,root,values.DateExport)).rejects.toThrow();
 expect(await readdir(root)).toEqual(expect.arrayContaining(['log-inspections','log-inspection-active.json']));
 await expect(stat(join(root,'first-run'))).rejects.toMatchObject({code:'ENOENT'});
});
it('real source connection failure attempts one connect and zero RPCs',async()=>{
 const connect=vi.spyOn(ExactConnection.prototype,'connect').mockImplementation(function(this:ExactConnection,cb){cb?.(Error('private login failure'));});
 const rpc=vi.spyOn(ExactConnection.prototype,'callProcedure');
 const result=await executeLogInspection(cfg,pin,root,values.DateExport);
 expect(result.state).toBe('REMOTE_OUTCOME_UNKNOWN');expect(connect).toHaveBeenCalledTimes(1);expect(rpc).not.toHaveBeenCalled();
});
it('failure after RPC dispatch closes without another connect or RPC',async()=>{
 const connect=vi.spyOn(ExactConnection.prototype,'connect').mockImplementation(function(this:ExactConnection,cb){
  this.emit('secure',{authorized:true,getProtocol:()=> 'TLSv1.2'} as import('node:tls').TLSSocket);cb?.();
 });
 const rpc=vi.spyOn(ExactConnection.prototype,'callProcedure').mockImplementation(request=>{request.callback(Error('PRIVATE remote outcome unknown'));});
 const result=await executeLogInspection(cfg,pin,root,values.DateExport);
 expect(result.state).toBe('REMOTE_OUTCOME_UNKNOWN');expect(connect).toHaveBeenCalledTimes(1);expect(rpc).toHaveBeenCalledTimes(1);
});
it('inspection cannot authorize an acquisition endpoint',async()=>{
 const current=reportedSignature('ReadDataCurrentDay',{evidence:'synthetic',nullable:{TableName:false,Message:true,ErrorMessage:true}});
 const ep=prepareCorporateAttemptsEndpoint(endpointConfig,pin,current,{TableName:'AttemptsRawData'});
 const callback=vi.fn();await expect(authorizeCorporateLogInspection(ep,()=>{},callback,{})).rejects.toThrow();expect(callback).not.toHaveBeenCalled();
});
it.each(['success','multiple','output-error','return-error','truncated'])('real source/parser with synthetic %s payload retains exact bytes',async mode=>{
 const {Readable}=await import('node:stream');const {Collation}=await import('tedious/lib/collation.js');
 const u16=(v:number)=>{const b=Buffer.alloc(2);b.writeUInt16LE(v);return b;};
 const output=(name:string,value:string|null)=>Buffer.concat([Buffer.from([0xac]),u16(0),Buffer.from([name.length]),Buffer.from(name,'utf16le'),Buffer.from([1]),Buffer.alloc(4),u16(1),Buffer.from([0xa7]),u16(4000),new Collation(1033,0,0,0).toBuffer(),u16(value===null?65535:value.length),...(value===null?[]:[Buffer.from(value)])]);
 const columns=Buffer.concat([Buffer.from([0x81]),u16(1),Buffer.alloc(4),u16(0),Buffer.from([0x38,2]),Buffer.from('ID','utf16le')]);
 const row=Buffer.from([0xd1,7,0,0,0]);
 const bytes=Buffer.concat([columns,row,...(mode==='multiple'?[Buffer.from([0xfd,1,0]),Buffer.alloc(10),columns]:[]),output('Message',null),output('ErrorMessage',mode==='output-error'?'PRIVATE_SQL_MESSAGE':null),Buffer.from([0x79,mode==='return-error'?5:0,0,0,0]),...(mode==='truncated'?[]:[Buffer.from([0xfe]),Buffer.alloc(12)])]);
 const connect=vi.spyOn(ExactConnection.prototype,'connect').mockImplementation(function(this:ExactConnection,cb){this.emit('secure',{authorized:true,getProtocol:()=> 'TLSv1.2'} as import('node:tls').TLSSocket);cb?.();});
 const rpc=vi.spyOn(ExactConnection.prototype,'callProcedure').mockImplementation(function(this:ExactConnection,request){
  class RequestTokenHandler {
   onColMetadata(token:{columns:import('tedious/lib/token/colmetadata-token-parser.js').ColumnMetadata[]}){request.emit('columnMetadata',token.columns);}
   onRow(token:{columns:unknown[]}){request.emit('row',token.columns);}
   onReturnValue(token:{paramName:string;value:unknown;metadata:import('tedious/lib/metadata-parser.js').Metadata}){request.emit('returnValue',token.paramName,token.value,token.metadata);}
   onReturnStatus(){} onDone(){} onDoneProc(){request.callback(null);}
  }
  const parser=this.createTokenStreamParser(Readable.from([bytes.subarray(0,20),bytes.subarray(20)]) as never,new RequestTokenHandler() as never);
  request.on('pause',()=>parser.pause());request.on('resume',()=>parser.resume());
  if(mode==='truncated')parser.on('end',()=>request.callback(Error('INCOMPLETE')));
 });
 const result=await executeLogInspection(cfg,pin,root,values.DateExport);
 expect(connect).toHaveBeenCalledTimes(1);expect(rpc).toHaveBeenCalledTimes(1);
 expect(result.sha256).toBe(createHash('sha256').update(bytes).digest('hex'));
 expect(JSON.stringify(result)).not.toContain('PRIVATE_SQL_MESSAGE');
 if(mode==='success' || mode==='multiple')expect(result).toMatchObject({received:1,responseValidated:true,datasetCount:mode==='multiple'?2:1});
 if(mode==='output-error' || mode==='return-error' || mode==='truncated')expect(result.state).toBe('REVIEW_REQUIRED');
});
it('inspection claim blocks acquisition permission before source connection',async()=>{
 const {mkdir}=await import('node:fs/promises');const {durableExclusive}=await import('../adapters/disposition-integration.js');
 const dir=join(root,'first-run');await mkdir(dir,{mode:0o700});
 await durableExclusive(join(root,'log-inspection-active.json'),{attemptId:randomUUID()});
 const current=reportedSignature('ReadDataCurrentDay',{evidence:'synthetic',nullable:{TableName:false,Message:true,ErrorMessage:true}});
 const ep=prepareCorporateAttemptsEndpoint(endpointConfig,pin,current,{TableName:'AttemptsRawData'}),attemptId=randomUUID(),configurationDigest='a'.repeat(64);
 await expect(authorizeCorporateAttemptsOnce(ep,()=>{},async()=>{
  const intentPath=join(dir,`attempt-${attemptId}.json`),pendingPath=join(root,'remote-pending.json');
  await durableExclusive(intentPath,{attemptId,configurationDigest,procedure:'ReadDataCurrentDay',operator:'test',approvalReference:'test-ticket',pinApprovalId:pin.approvalId});
  await durableExclusive(pendingPath,{attemptId,configurationDigest,workDir:dir});return {intentPath,pendingPath};
 })).rejects.toThrow();
 expect(()=>assertCorporateAttemptsExecution(ep)).toThrow();
});
it('storage reservation failure grants no execution permission and preserves prepared intent',async()=>{
 const {LocalSpool}=await import('../adapters/local-spool.js');
 vi.spyOn(LocalSpool.prototype,'createResponseCapture').mockRejectedValue(Object.assign(Error('DISK_FULL'),{code:'DISK_FULL'}));
 const connect=vi.spyOn(ExactConnection.prototype,'connect');
 await expect(executeLogInspection(cfg,pin,root,values.DateExport)).rejects.toThrow('DISK_FULL');
 expect(connect).not.toHaveBeenCalled();await stat(join(root,'log-inspections',cfg.authorization.operationId,'intent.json'));
});

it.each(['authorization','connection','tls','login','rpc-guard','rpc-handoff','rpc-timeout','capture','validation','seal'] as const)('durable sanitized diagnostic for %s failure without remote retry',async mode=>{
 const secret='PRIVATE password SQL text token payload person';
 const native=(code:string)=>Object.assign(Error(secret),{code});
 const rpc=vi.spyOn(ExactConnection.prototype,'callProcedure');
 if(mode==='capture' || mode==='seal'){
  const {LocalSpool}=await import('../adapters/local-spool.js'),create=LocalSpool.prototype.createResponseCapture;
  vi.spyOn(LocalSpool.prototype,'createResponseCapture').mockImplementation(async function(this:import('../adapters/local-spool.js').LocalSpool,id){
   const capture=await create.call(this,id);
   return {...capture,...(mode==='capture'?{append:async(chunk:Buffer)=>{await capture.append(chunk);throw native('ENOSPC');}}:{seal:async()=>{throw native('ENOSPC');}})};
  });
 }
 if(mode==='authorization')vi.spyOn(access,'authorizeCorporateLogInspection').mockRejectedValueOnce(new OlosError('REAL_EXECUTION_BLOCKED'));
 const connect=vi.spyOn(ExactConnection.prototype,'connect').mockImplementation(function(this:ExactConnection,cb){
  if(mode==='connection'){cb?.(native('ECONNREFUSED'));return;}
  this.inspectionDiagnostics?.({kind:'stage',stage:'TLS_HANDSHAKE_STARTED'});
  if(mode==='tls'){
   reportInspectionFailure(this.inspectionDiagnostics,native('PIN_MISMATCH'),'CONNECTION_FAILED');
   cb?.(native('ESOCKET'));return;
  }
  this.inspectionDiagnostics?.({kind:'stage',stage:'TLS_VALIDATED'});
  this.emit('secure',{authorized:true,getProtocol:()=> 'TLSv1.2'} as import('node:tls').TLSSocket);
  this.inspectionDiagnostics?.({kind:'stage',stage:'SQL_LOGIN_STARTED'});
  if(mode==='login'){cb?.(native('ELOGIN'));return;}cb?.();
 });
 rpc.mockImplementation(function(this:ExactConnection,request){
  if(mode==='rpc-guard')throw new OlosError('PROCEDURE_BLOCKED');
  this.inspectionDiagnostics?.({kind:'stage',stage:'RPC_HANDOFF_STARTED'});
  if(mode==='rpc-handoff'){this.emit('error',native('ECONNRESET'));return;}
  // Keep this branch unused: capture/validation below exercise the source callback seam.
  request.callback(native('TIMEOUT'));
 });
 const result=await executeLogInspection(cfg,pin,root,values.DateExport,(...args)=>{
  const source=new IsolatedTdsSource(...args);
  if(mode==='capture' || mode==='validation' || mode==='seal')vi.spyOn(source,'call').mockImplementation(async function*(){
   args[5]?.({kind:'stage',stage:'RPC_HANDOFF_STARTED'});
   await args[3]!(Buffer.from('SYNTHETIC-PARTIAL-RESPONSE'));
   for await(const _ of args[4]!()){}
   if(mode==='validation')throw new OlosError('INVALID_RESPONSE','unknown');
   yield {kind:'complete',datasetCount:0,returnValue:0,errorMessage:null,message:null,output:{Message:null,ErrorMessage:null}};
  });return source;
 });
 const expected={
  authorization:['AUTHORIZATION','AUTHORIZATION_STARTED','REAL_EXECUTION_BLOCKED','NOT_OBSERVED','NOT_OBSERVED'],
  connection:['CONNECTION_INIT','CONNECTION_INIT_STARTED','ECONNREFUSED','NOT_OBSERVED','NOT_OBSERVED'],
  tls:['TLS_HANDSHAKE','TLS_HANDSHAKE_STARTED','PIN_MISMATCH','NOT_OBSERVED','NOT_OBSERVED'],
  login:['SQL_LOGIN','SQL_LOGIN_STARTED','ELOGIN','NOT_OBSERVED','NOT_OBSERVED'],
  'rpc-guard':['RPC_DISPATCH','RPC_DISPATCH_STARTED','PROCEDURE_BLOCKED','NOT_OBSERVED','NOT_OBSERVED'],
  'rpc-handoff':['RPC_DISPATCH','RPC_HANDOFF_STARTED','ECONNRESET','DRIVER_HANDOFF_STARTED','UNKNOWN'],
  'rpc-timeout':['RPC_DISPATCH','RPC_HANDOFF_STARTED','TIMEOUT','DRIVER_HANDOFF_STARTED','UNKNOWN'],
  seal:['RESPONSE_CAPTURE','CAPTURE_FINALIZATION_STARTED','ENOSPC','DRIVER_HANDOFF_STARTED','RESPONSE_VALIDATED'],
  capture:['RESPONSE_CAPTURE','RESPONSE_CAPTURE_STARTED','ENOSPC','DRIVER_HANDOFF_STARTED','UNKNOWN'],
  validation:['RESPONSE_VALIDATION','RESPONSE_VALIDATION_STARTED','INVALID_RESPONSE','DRIVER_HANDOFF_STARTED','UNKNOWN'],
 }[mode]!;
 expect(result.diagnostic).toMatchObject({phase:expected[0],lastObservedStage:expected[1],errorCode:expected[2],rpcDispatchState:expected[3],remoteExecutionOutcome:expected[4]});
 const work=join(root,'log-inspections',cfg.authorization.operationId),path=join(work,'diagnostic.json');
 const serialized=await readFile(path,'utf8');expect(JSON.parse(serialized)).toEqual(result.diagnostic);
 expect(serialized).not.toContain(secret);expect(serialized).not.toMatch(/password|stack|cause|payload|person/);
 expect((await stat(path)).mode&0o777).toBe(0o600);
 expect((await stat(join(work,'diagnostic-start.json'))).mode&0o777).toBe(0o600);
 expect(connect.mock.calls.length).toBeLessThanOrEqual(1);expect(rpc.mock.calls.length).toBeLessThanOrEqual(1);
 await stat(join(root,'log-inspection-active.json'));
 await expect(executeLogInspection(cfg,pin,root,values.DateExport)).rejects.toThrow();
});
it('error allowlist never emits raw messages, nested metadata or an unrecognized code',()=>{
 for(const error of [Error('private SQL password'),{code:'PRIVATE_TOKEN',message:'PIN_MISMATCH including sensitive text',stack:'private'},
  {message:'private',cause:{code:'untrusted private code',payload:'secret'}}])expect(inspectionErrorCode(error)).toBe('EXECUTION_FAILED');
 expect(inspectionErrorCode({code:'ESOCKET',message:'private',cause:{code:'PIN_MISMATCH'}})).toBe('ESOCKET');
 expect(inspectionErrorCode({cause:{code:'PIN_MISMATCH',message:'private'}})).toBe('PIN_MISMATCH');
 const tracker=new InspectionDiagnostics();tracker.observe({kind:'stage',stage:'TLS_HANDSHAKE_STARTED'});
 reportInspectionFailure(tracker.observe,{code:'PIN_MISMATCH',message:'private'});
 tracker.observe({kind:'stage',stage:'SQL_LOGIN_ACCEPTED'});reportInspectionFailure(tracker.observe,new OlosError('CONNECTION_FAILED'));
 expect(tracker.snapshot()).toMatchObject({phase:'TLS_HANDSHAKE',lastObservedStage:'TLS_HANDSHAKE_STARTED',errorCode:'PIN_MISMATCH',rpcDispatchState:'NOT_OBSERVED'});
});
it('does not backfill diagnostics or change any existing operation evidence',async()=>{
 const {mkdir}=await import('node:fs/promises');
 cfg.authorization.operationId='193c7869-6ba0-480f-a7c8-3fcd6e178394';
 const work=join(root,'log-inspections',cfg.authorization.operationId);await mkdir(work,{recursive:true,mode:0o700});
 const files=['intent.json','pending.json','result.json','response.partial'];
 for(const file of files)await writeFile(join(work,file),`synthetic existing evidence: ${file}`,{mode:0o600});
 await expect(executeLogInspection(cfg,pin,root,values.DateExport)).rejects.toMatchObject({code:'EEXIST'});
 for(const file of files)expect(await readFile(join(work,file),'utf8')).toBe(`synthetic existing evidence: ${file}`);
 expect((await readdir(work)).sort()).toEqual(files.sort());
});

it.each([false,true])('ExactConnection records RPC handoff only after the actual guard; guard passes=%s',async accepted=>{
 const {ep,p,authorize}=await prepared();await authorize();
 assertPreparedAttemptsCall(ep,prepareCall(signature,values,300000));claimAttemptsConnection(ep);claimAttemptsSocket(ep);claimAttemptsLogin(ep);
 const connection=new ExactConnection({server:endpointConfig.server,authentication:{type:'default',options:{userName:'OEC',password:endpointConfig.password}},
  options:{database:'ExportDataV3',port:1433,serverName:endpointConfig.server,encrypt:true,trustServerCertificate:false,tdsVersion:'7_4',useUTC:true,
   maxRetriesOnTransientErrors:0,cryptoCredentialsDetails:{minVersion:'TLSv1.2',maxVersion:'TLSv1.2'}}},ep);
 const diagnostics=new InspectionDiagnostics();diagnostics.observe({kind:'stage',stage:'RPC_DISPATCH_STARTED'});connection.inspectionDiagnostics=diagnostics.observe;
 const {Connection}=await import('tedious');
 const handoff=vi.spyOn(Connection.prototype,'callProcedure').mockImplementation(()=>{throw new OlosError('TIMEOUT','unknown');});
 const request=new Request(`dbo.usp_ExportData_${accepted?'LogReadData':'ReReadData'}`,()=>{});prepareTdsRequest(request,signature,values,300000);
 try{
  expect(()=>connection.callProcedure(request)).toThrow(accepted?'TIMEOUT':'PROCEDURE_BLOCKED');
  expect(handoff).toHaveBeenCalledTimes(accepted?1:0);
  expect(diagnostics.snapshot()).toMatchObject({lastObservedStage:accepted?'RPC_HANDOFF_STARTED':'RPC_DISPATCH_STARTED',
   rpcDispatchState:accepted?'DRIVER_HANDOFF_STARTED':'NOT_OBSERVED',remoteExecutionOutcome:accepted?'UNKNOWN':'NOT_OBSERVED'});
  if(accepted){expect(()=>connection.callProcedure(request)).toThrow();expect(handoff).toHaveBeenCalledTimes(1);}
 }finally{await p.capture.interrupt();connection.close();}
});
