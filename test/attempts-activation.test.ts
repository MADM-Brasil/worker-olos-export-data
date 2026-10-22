import {beforeAll,afterAll,beforeEach,afterEach,it,expect,vi} from 'vitest';
import {mkdtemp,mkdir,writeFile,readFile,rm,readdir} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomUUID,X509Certificate} from 'node:crypto';
import {Socket} from 'node:net';
import {Collation} from 'tedious/lib/collation.js';
import {Pool} from 'pg';
import {syntheticCertificates} from './support/tls-diagnostic-fixture.js';
import {serviceConfigSchema,validateServiceApproval,validateActivation,validateReviewedContract,planForCapture,type ContractReview} from '../src/integration/attempts/activation.js';
import {prepareServiceContract,loadServiceConfiguration,serviceGuard,observeServiceCapture,reviewServiceContract,promoteService,pendingServiceCase,reconcileServiceInspection,operationalServiceStatus} from '../adapters/attempts-service.js';
import {attemptsFixtureFields,attemptsFixturePlan,signature} from '../src/integration/attempts/contracts.js';
import {exampleProfile} from '../src/offline/model.js';
import {APPROVED_COLUMNS} from '../src/offline/tds-writer.js';
import {parseOlosPinApproval} from '../src/security/olos-certificate-pin.js';
import {digest,canonical} from '../src/persistence/validation.js';
import {blockingCorporateInspection} from '../src/olos/tds/attempts-access.js';
import {IsolatedTdsSource} from '../src/olos/tds/source.js';
import {OlosError} from '../src/olos/types.js';
import {parseCorporateArguments} from '../adapters/attempts-corporate.js';
import {AttemptsCycleRunner} from '../src/integration/attempts/cycle-runner.js';
import {parserLimits} from '../src/olos/tds/raw-parser.js';
let certificates:Awaited<ReturnType<typeof syntheticCertificates>>,base:string,root:string,configFile:string,loaded:Awaited<ReturnType<typeof loadServiceConfiguration>>;
import {fields,response,collation} from './support/activation-capture.js';
const save=async(p:string,o:unknown)=>writeFile(p,JSON.stringify(o),{mode:0o600});
beforeAll(async()=>{certificates=await syntheticCertificates();},30000);
afterAll(async()=>{await certificates.stop();});
beforeEach(async()=>{
 base=await mkdtemp(join(tmpdir(),'olos-activation-'));root=join(base,'service');await mkdir(root,{mode:0o700});configFile=join(base,'config.json');
 vi.spyOn(Socket.prototype,'connect').mockImplementation(()=>{throw Error('CORPORATE_NETWORK_FORBIDDEN');});
 vi.spyOn(Pool.prototype,'connect').mockImplementation(()=>{throw Error('DATABASE_NOT_EXPECTED');});
 const config=serviceConfigSchema.parse({version:'olos-service/v2',workerId:'synthetic-worker',endpoint:{server:'10.196.234.11',port:1433,database:'ExportDataV3',user:'OEC',password:'SYNTHETIC'},
  postgres:{host:'127.0.0.1',port:5434,database:'olos_worker'},pinApprovalFile:join(base,'pin.json'),approvalFile:join(base,'approval.json'),
  profile:{...exampleProfile,phoneMatchKey:'normalized',columns:[...APPROVED_COLUMNS]},campaignFile:join(base,'campaign.json'),dispositionFile:join(base,'disposition.json')});
 await save(configFile,config);await save(config.campaignFile,[{CampaignId:'1',Description:'synthetic'}]);await save(config.dispositionFile,[{DispositionId:'1',Description:'synthetic'}]);
 await save(config.pinApprovalFile,{version:1,purpose:'OLOS_ATTEMPTS_TDS_ACQUISITION',host:'10.196.234.11',port:1433,database:'ExportDataV3',approvalId:'synthetic',approvedBy:'test',approvedAt:new Date(Date.now()-1000).toISOString(),expiresAt:new Date(Date.now()+3600000).toISOString(),certificatePem:certificates.selfSigned,fingerprintSha256:new X509Certificate(certificates.selfSigned).fingerprint256});
 loaded=await loadServiceConfiguration(configFile);await save(config.approvalFile,approval());
});
afterEach(async()=>{expect(Socket.prototype.connect).not.toHaveBeenCalled();expect(Pool.prototype.connect).not.toHaveBeenCalled();vi.restoreAllMocks();await rm(base,{recursive:true,force:true});});
function approval(){return {scope:'OLOS_ATTEMPTS_SERVICE' as const,approvalId:'op-approval',workerId:loaded.config.workerId,configurationDigest:loaded.configurationDigest,
 procedures:['ReadDataCurrentDay','ReadDataPreviousDays'] as ['ReadDataCurrentDay','ReadDataPreviousDays'],table:'AttemptsRawData' as const,destination:'olos_worker.raw.olos_calls' as const,operator:'responsible',reference:'real-reference-required-in-production',issuedAt:'2020-01-01T00:00:00.000Z',
 reviewPolicy:'REVOKE_BY_PRIVATE_FILE' as const,revocationFile:join(base,'revoked.json'),bootstrapRecoveryEvidence:'external first-read reconciliation approval required'};}
const guard=()=>serviceGuard(configFile,loaded,approval());
function sourceFactory(mode:'complete'|'before-rpc'|'after-rpc'|'partial'|'tls'|'login'|'vpn'|'unsupported'='complete'){
 return (...args:ConstructorParameters<typeof IsolatedTdsSource>)=>{
  const source=new IsolatedTdsSource(...args);
  vi.spyOn(source,'call').mockImplementation(async function*(){
   args[5]?.({kind:'stage',stage:'CONNECTION_INIT_STARTED'});
   if(mode==='before-rpc')throw new OlosError('REAL_EXECUTION_BLOCKED');
   if(mode==='tls'){args[5]?.({kind:'stage',stage:'TLS_HANDSHAKE_STARTED'});throw Object.assign(Error('sensitive certificate details'),{code:'PIN_MISMATCH'});}
   if(mode==='login'){args[5]?.({kind:'stage',stage:'SQL_LOGIN_STARTED'});throw Object.assign(Error('sensitive SQL text'),{code:'ELOGIN'});}
   if(mode==='vpn')throw Object.assign(Error('sensitive connection text'),{code:'ENETUNREACH'});
   args[5]?.({kind:'stage',stage:'RPC_HANDOFF_STARTED'});
   if(mode==='after-rpc')throw new OlosError('TIMEOUT','unknown');
   await args[3]!(mode==='partial'?response.subarray(0,33):response);
   if(mode==='partial')throw new OlosError('CONNECTION_FAILED','unknown');
   if(args[6])for await(const _ of args[6]()){}
   if(mode==='unsupported')throw new OlosError('INVALID_RESPONSE');
   source.transportSecurity.push({encrypted:true,identityValidated:true,protocol:'TLSv1.2',tlsMode:'certificate-pinned'});
   source.observedFields.push(...fields);
   yield {kind:'row',values:[]};
   yield {kind:'complete',returnValue:0,datasetCount:1,errorMessage:null,message:null,output:{Message:null,ErrorMessage:null}};
  });return source;
 };
}
async function capture(mode:Parameters<typeof sourceFactory>[0]='complete'){
 return observeServiceCapture(base,root,loaded,approval(),guard(),'ReadDataCurrentDay',new AbortController().signal,undefined,sourceFactory(mode));
}
async function reviewFor(id:string):Promise<ContractReview>{
 const data=JSON.parse(await readFile(join(root,'cases',id,'capture','batches',id,'response.json'),'utf8'));
 const fixture=attemptsFixturePlan('2026-10-22',collation.toBuffer().toString('hex'));
 const plan={...fixture,fields:fields.map(({sqlLength:_s,collationHex:_c,unicodeMetadataPolicy:_u,...f})=>f) as typeof fixture.fields,
  contract:{...fixture.contract,version:'reviewed-corporate/v1'},run:{...fixture.run,contractVersion:'reviewed-corporate/v1'}};
 return {version:'olos-contract-review/v1',procedure:'ReadDataCurrentDay',captureId:id,captureSha256:data.sha256,configurationDigest:loaded.configurationDigest,operator:'reviewer',reference:'test-review',reviewedAt:new Date().toISOString(),sourceTimeZone:'UTC',controlDayOffset:0,partitionSemanticsEvidence:'control bucket; server partition separately attested',plan,fields};
}
it('BOOTSTRAP has no first-confirmed dependency and captures privately without PostgreSQL or contract approval',async()=>{
 const intent=await capture();expect(intent.plan).toBeNull();expect(await readdir(root)).not.toContain('first-confirmed.json');
 expect((await pendingServiceCase(base,root))?.attemptId).toBe(intent.attemptId);
 const proposal=await prepareServiceContract(root,loaded,intent.attemptId);expect(proposal.contractApproved).toBe(false);
 const candidate=JSON.parse(await readFile(proposal.proposalFile,'utf8'));expect(candidate.plan.contract).toMatchObject({approved:false,key_fields:[]});
 await expect(reviewServiceContract(root,loaded,candidate)).rejects.toThrow();
 expect(await readFile(join(root,'cases',intent.attemptId,'capture','batches',intent.attemptId,'response.ready'))).toEqual(response);
 await expect(capture()).rejects.toThrow('RECOVERY_REQUIRED');
});
it('service approval persists beyond 12 hours; revocation and configuration changes fail before source',async()=>{
 const a=approval();expect(validateServiceApproval(a,loaded.config,loaded.configurationDigest,false)).toEqual(a);guard()();
 await save(a.revocationFile,{reference:'revoked'});expect(()=>guard()()).toThrow('SERVICE_AUTHORIZATION_REJECTED');
 await rm(a.revocationFile);await save(loaded.config.campaignFile,[{CampaignId:'1',Description:'changed'}]);expect(()=>guard()()).toThrow('SERVICE_CONFIGURATION_CHANGED');
});
it('observed metadata must be reviewed explicitly, and premature ACTIVE cannot be manufactured by a flag',async()=>{
 const i=await capture(),r=await reviewFor(i.attemptId);
 await expect(promoteService(root,loaded,approval(),{state:'ACTIVE'})).rejects.toThrow();
 await expect(reviewServiceContract(root,loaded,{...r,captureSha256:'0'.repeat(64)})).rejects.toThrow('OBSERVED_CONTRACT_NOT_APPROVED');
 await expect(reviewServiceContract(root,loaded,r)).resolves.toMatchObject({captureId:i.attemptId});
 const bad={...r,plan:{...r.plan,contract:{...r.plan.contract,approved:false}}};expect(()=>validateReviewedContract(bad,fields,'ReadDataCurrentDay')).toThrow();
 expect(()=>validateActivation({active:true},loaded.config,loaded.configurationDigest,approval(),[r])).toThrow();
});
it('CurrentDay and PreviousDays retain independent contracts, metadata and calendar buckets across midnight',async()=>{
 const i=await capture(),current=await reviewFor(i.attemptId);
 const previous={...structuredClone(current),procedure:'ReadDataPreviousDays' as const,controlDayOffset:-1,plan:{...structuredClone(current.plan),signature:signature('ReadDataPreviousDays'),run:{...current.plan.run,readType:'previous_days' as const}}};
 validateReviewedContract(previous,fields,'ReadDataPreviousDays');
 expect(planForCapture(current,new Date('2026-10-23T00:01:00Z')).run.exportDate).toBe('2026-10-23');
 expect(planForCapture(previous,new Date('2026-10-23T00:01:00Z')).run.exportDate).toBe('2026-10-22');
 const changed={...previous,fields:previous.fields.map(f=>f.name==='CampaignData'?{...f,maxBytes:128}:f)};
 expect(()=>validateReviewedContract(changed,fields,'ReadDataPreviousDays')).toThrow();
});
it.each(['before-rpc','after-rpc','partial','tls','login','vpn'] as const)('capture failure %s preserves pending and never retries remotely',async mode=>{
 await expect(capture(mode)).rejects.toThrow();const p=await pendingServiceCase(base,root);expect(p).not.toBeNull();
 const dir=join(root,'cases',p!.attemptId),diag=JSON.parse(await readFile(join(dir,'diagnostic.json'),'utf8'));
 expect(diag.rpcDispatchState).toBe(['before-rpc','tls','login','vpn'].includes(mode)?'NOT_OBSERVED':'DRIVER_HANDOFF_STARTED');
 const files=await readdir(dir);expect(files.includes('not-dispatched.json')).toBe(['before-rpc','tls','login','vpn'].includes(mode));
 expect(JSON.stringify(diag)).not.toContain('sensitive');
 await expect(capture()).rejects.toThrow('RECOVERY_REQUIRED');
 expect(await readFile(join(dir,'capture','batches',p!.attemptId,'response.partial'))).toEqual(mode==='partial'?response.subarray(0,33):Buffer.alloc(0));
});
it('inspection administrative decisions preserve the old case and cannot clear consumptive exclusion',async()=>{
 const id='193c7869-6ba0-480f-a7c8-3fcd6e178394',workDir=join(base,'log-inspections',id);await mkdir(workDir,{recursive:true,mode:0o700});
 const original={intent:{attemptId:id},pending:{attemptId:id},result:{state:'REMOTE_OUTCOME_UNKNOWN',bytes:0}};
 for(const [file,value] of Object.entries(original))await save(join(workDir,`${file}.json`),value);
 const marker={attemptId:id,workDir};await save(join(base,'log-inspection-active.json'),marker);
 for(const decision of ['PRESERVE_PENDING_EXTERNAL_RECONCILIATION','ADMINISTRATIVE_CLOSURE'] as const){
  const r=await reconcileServiceInspection(base,{version:'olos-inspection-reconciliation/v1',recordId:randomUUID(),caseId:id,operator:'responsible',reference:'external-review',issuedAt:new Date().toISOString(),decision,remoteOutcome:'INDETERMINATE'});
  expect(r.consumptiveRelease).toBe(false);expect(blockingCorporateInspection(base)).toBe(true);
 }
 const audit=join(base,'audit.json');await save(audit,{kind:'olos-remote-audit/v1',caseId:id,markerDigest:digest(marker),operator:'external-auditor',reference:'AUDIT-TEST',conclusion:'NO_UNRESOLVED_INTERFERENCE',rpcAuditEvidence:'external server audit',consumptionImpactEvidence:'external consumption evidence',issuedAt:new Date().toISOString()});
 await reconcileServiceInspection(base,{version:'olos-inspection-reconciliation/v1',recordId:randomUUID(),caseId:id,operator:'responsible',reference:'external-review',issuedAt:new Date().toISOString(),decision:'TECHNICAL_CLEARANCE',remoteOutcome:'REMOTELY_ESTABLISHED',externalAuditFile:audit});
 expect(blockingCorporateInspection(base)).toBe(false);
 for(const [file,value] of Object.entries(original))expect(JSON.parse(await readFile(join(workDir,`${file}.json`),'utf8'))).toEqual(value);
 expect(JSON.parse(await readFile(join(base,'log-inspection-active.json'),'utf8'))).toEqual(marker);
 await save(join(workDir,'result.json'),{state:'tampered'});expect(blockingCorporateInspection(base)).toBe(true);
});
it('certificate lifetime policy never outlives the actual certificate or approves an observed pin implicitly',async()=>{
 const input=JSON.parse(await readFile(loaded.config.pinApprovalFile,'utf8')),end=new X509Certificate(input.certificatePem).validTo;
 expect(parseOlosPinApproval({...input,validityPolicy:'CERTIFICATE_LIFETIME',expiresAt:new Date(end).toISOString()}).validityPolicy).toBe('CERTIFICATE_LIFETIME');
 expect(()=>parseOlosPinApproval({...input,validityPolicy:'CERTIFICATE_LIFETIME',expiresAt:new Date(Date.parse(end)+1000).toISOString()})).toThrow();
});
it('explicit CLI bootstrap/review/activation actions reject missing confirmations and cross-action flags',()=>{
 expect(parseCorporateArguments(['bootstrap','--procedure','ReadDataCurrentDay','--confirm','AUTHORIZE_BOOTSTRAP_CAPTURE'],{}).action).toBe('bootstrap');
 for(const args of [['bootstrap'],['activate','--activation-file','/private/a.json'],['start','--procedure','ReadDataCurrentDay'],['bootstrap','--procedure','ResetReadData','--confirm','AUTHORIZE_BOOTSTRAP_CAPTURE']])expect(()=>parseCorporateArguments(args,{})).toThrow();
});
it('120-second scheduler never overlaps a long cycle and suspends after uncertainty',async()=>{
 vi.useFakeTimers();try{
  let release!:()=>void,calls=0,active=0,max=0;
  const runner=new AttemptsCycleRunner(async()=>{calls++;active++;max=Math.max(max,active);await new Promise<void>(r=>{release=r;});active--;throw Error('REMOTE_OUTCOME_UNKNOWN');},120000);
  runner.start();await vi.advanceTimersByTimeAsync(120000);await vi.advanceTimersByTimeAsync(360000);expect(calls).toBe(1);expect(max).toBe(1);
  release();await vi.advanceTimersByTimeAsync(1);await vi.advanceTimersByTimeAsync(360000);expect(calls).toBe(1);expect(runner.status).toBe('failed');await runner.stop();
 }finally{vi.useRealTimers();}
});
it('service status reports unknown PostgreSQL rather than inventing operational metrics',async()=>{
 const s=await operationalServiceStatus({base,root,configFile,intervalMs:120000});expect(s).toMatchObject({state:'BOOTSTRAP',postgresState:'NOT_PROBED',imported:null,duplicates:null,nextExecutionAt:null,networkAccessed:false});
});

it('a complete response with unsupported layout remains sealed in quarantine and cannot be approved',async()=>{
 await expect(capture('unsupported')).rejects.toThrow('INVALID_RESPONSE');
 const pending=await pendingServiceCase(base,root),dir=join(root,'cases',pending!.attemptId,'capture','batches',pending!.attemptId);
 expect(await readFile(join(dir,'response.ready'))).toEqual(response);
 expect(JSON.parse(await readFile(join(dir,'response.json'),'utf8'))).toMatchObject({responseValidated:false,rawMessageComplete:true});
 await expect(prepareServiceContract(root,loaded,pending!.attemptId)).rejects.toThrow('OBSERVED_CONTRACT_NOT_APPROVED');
 await expect(capture()).rejects.toThrow('RECOVERY_REQUIRED');
});
it('V2 loads an optional sanitized agent snapshot, binding its contents to the configuration digest',async()=>{
 const agentFile=join(base,'agents.json');await save(agentFile,[{AgentId:'1',UserName:'test-reference'}]);
 await save(configFile,{...loaded.config,agentFile});
 const revised=await loadServiceConfiguration(configFile);
 expect(revised.catalogs.agents.get('1')==='test-reference').toBe(true);
 expect(revised.configurationDigest===loaded.configurationDigest).toBe(false);
 const revisedApproval={...approval(),configurationDigest:revised.configurationDigest};await save(revised.config.approvalFile,revisedApproval);
 const check=serviceGuard(configFile,revised,revisedApproval);check();
 await save(agentFile,[{AgentId:'1',UserName:'test-reference-updated'}]);expect(check).toThrow('SERVICE_CONFIGURATION_CHANGED');
});
it('120 seconds are counted from completion, even when a successful cycle takes longer than the interval',async()=>{
 vi.useFakeTimers();try{
  let release!:()=>void,calls=0;const runner=new AttemptsCycleRunner(async()=>{calls++;await new Promise<void>(r=>{release=r;});},120000);
  runner.start();await vi.advanceTimersByTimeAsync(119999);expect(calls).toBe(0);
  await vi.advanceTimersByTimeAsync(1);await vi.advanceTimersByTimeAsync(480000);expect(calls).toBe(1);expect(runner.nextExecutionAt).toBeNull();
  release();await vi.advanceTimersByTimeAsync(0);expect(runner.nextExecutionAt).toBe(new Date(Date.now()+120000).toISOString());
  await vi.advanceTimersByTimeAsync(119999);expect(calls).toBe(1);await vi.advanceTimersByTimeAsync(1);expect(calls).toBe(2);
  release();await vi.advanceTimersByTimeAsync(0);await runner.stop();
 }finally{vi.useRealTimers();}
});
