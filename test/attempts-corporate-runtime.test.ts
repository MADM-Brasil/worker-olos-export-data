import {beforeAll,afterAll,it,expect,vi} from 'vitest';
import {X509Certificate} from 'node:crypto';
import {homedir} from 'node:os';
import {join} from 'node:path';
import {mkdtemp,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {randomUUID} from 'node:crypto';
import {syntheticCertificates} from './support/tls-diagnostic-fixture.js';
import {parseOlosPinApproval} from '../src/security/olos-certificate-pin.js';
import {createCorporateAttemptsBinding,corporateConfigSchema,corporateAcquisitionScope,type CorporateCycleState} from '../src/integration/attempts/corporate.js';
import {attemptsFixturePlan,attemptsFixtureFields,signature} from '../src/integration/attempts/contracts.js';
import {exampleProfile} from '../src/offline/model.js';
import {APPROVED_COLUMNS} from '../src/offline/tds-writer.js';
import {parseCorporateArguments} from '../adapters/attempts-corporate.js';
import {authorizeCorporateAttemptsOnce,prepareCorporateAttemptsEndpoint,assertCorporateAttemptsExecution,assertPreparedAttemptsCall,claimAttemptsConnection,claimAttemptsSocket,claimAttemptsLogin,assertPreparedAttemptsRpc} from '../src/olos/tds/attempts-access.js';
import {prepareCall} from '../src/olos/client.js';
import {prepareTdsRequest} from '../src/olos/tds/source.js';
import {Request} from 'tedious';
import {createAttemptsPipeline} from '../src/integration/attempts/pipeline.js';
import {PostgresLargeBatchPersistence} from '../src/persistence/large-batch.js';
import {ProjectionDispatcher} from '../src/projection/dispatcher.js';
import {spoolLimits} from '../src/large-batch/config.js';
import type {Pool} from 'pg';
import type {SpoolRepository} from '../src/large-batch/types.js';
let certs:Awaited<ReturnType<typeof syntheticCertificates>>,dir:string;
async function intentFiles(intent:Record<string,unknown>){const root=await mkdtemp(join(dir,'case-'));const intentPath=join(root,`attempt-${intent.attemptId}.json`),pendingPath=join(root,'pending.json');
 await writeFile(intentPath,JSON.stringify(intent),{mode:0o600});await writeFile(pendingPath,JSON.stringify({attemptId:intent.attemptId,configurationDigest:intent.configurationDigest,workDir:root}),{mode:0o600});return {intentPath,pendingPath};}

beforeAll(async()=>{certs=await syntheticCertificates();dir=await mkdtemp(join(tmpdir(),'olos-corporate-runtime-'));},30000);afterAll(async()=>{await certs?.stop();if(dir)await rm(dir,{recursive:true,force:true});});
const endpoint={server:'10.196.234.11',port:1433,database:'ExportDataV3',user:'OEC',password:'synthetic-only'};
function pin(){return parseOlosPinApproval({version:1,purpose:'OLOS_ATTEMPTS_TDS_ACQUISITION',host:endpoint.server,port:1433,database:endpoint.database,approvalId:'synthetic',approvedBy:'synthetic',
 approvedAt:new Date(Date.now()-1000).toISOString(),expiresAt:new Date(Date.now()+3600000).toISOString(),certificatePem:certs.selfSigned,fingerprintSha256:new X509Certificate(certs.selfSigned).fingerprint256});}
function config(){
 const date=new Date().toISOString().slice(0,10),original=attemptsFixturePlan(date,'0000000000');
 const p={...original,contract:{...original.contract,version:'corporate-runtime-test/v1',layout_evidence:'synthetic reviewed exact layout'},run:{...original.run,contractVersion:'corporate-runtime-test/v1'}};
 const previous={...structuredClone(p),run:{...p.run,readType:'previous_days' as const},signature:signature('ReadDataPreviousDays')};
 return corporateConfigSchema.parse({version:'olos-corporate-local/v1',endpoint,pinApprovalFile:'/private/pin.json',
  current:{plan:p,fields:attemptsFixtureFields('0000000000')},previous:{plan:previous,fields:attemptsFixtureFields('0000000000')},
  profile:{...exampleProfile,columns:[...APPROVED_COLUMNS],phoneMatchKey:'normalized'},campaignFile:'/private/campaign.json',dispositionFile:'/private/disposition.json',
  authorization:{operator:'synthetic-operator',reference:'synthetic-review',issuedAt:new Date(Date.now()-1000).toISOString(),expiresAt:new Date(Date.now()+600000).toISOString(),sourceTimeZone:'UTC',
   partitionPolicy:'ACQUISITION_DAY_CONTROL_BUCKET',layoutEvidence:'synthetic reviewed exact layout',sharedConsumptionEvidence:'synthetic coordinated source review',recoveryEvidence:'synthetic stop and remote review evidence',failurePolicy:'STOP_PRESERVE_CASE_RECONCILE_LOG_REREAD_NO_RESET'}});
}
const cats={campaigns:new Map([['1','synthetic campaign']]),dispositions:new Map([['1','synthetic disposition']]),agents:new Map<string,string>()};
it('CurrentDay once needs no invented PreviousDays layout; adding it or renewing authorization does not invalidate a completed acquisition scope',()=>{
 const complete=config(),{previous,...input}=structuredClone(complete),single=corporateConfigSchema.parse(input);
 const state:CorporateCycleState={next:'current',pending:null};
 const b=createCorporateAttemptsBinding(single,pin(),cats,state,async()=>{throw Error('NO_RPC');},async()=>{});
 expect(b.plan().signature.procedure).toBe('ReadDataCurrentDay');
 state.next='previous';expect(()=>b.plan()).toThrow('PREVIOUS_CONTRACT_UNCONFIRMED');
 expect(corporateAcquisitionScope(single,cats)).toBe(corporateAcquisitionScope(complete,cats));
 complete.authorization.reference='renewed legitimate authorization';
 expect(corporateAcquisitionScope(single,cats)).toBe(corporateAcquisitionScope(complete,cats));
 complete.current.fields=[{...complete.current.fields[0]!,nullable:!complete.current.fields[0]!.nullable},...complete.current.fields.slice(1)];
 expect(corporateAcquisitionScope(single,cats)).not.toBe(corporateAcquisitionScope(complete,cats));
});
it('durable intent precedes the permit; only one exact call/connection/socket/login/RPC is allowed',async()=>{
 const s=signature('ReadDataCurrentDay'),v={TableName:'AttemptsRawData'},ep=prepareCorporateAttemptsEndpoint(endpoint,pin(),s,v);
 let durable=false;await authorizeCorporateAttemptsOnce(ep,()=>{},async()=>{expect(()=>assertCorporateAttemptsExecution(ep)).toThrow();durable=true;return intentFiles({attemptId:randomUUID(),configurationDigest:'a'.repeat(64),procedure:'ReadDataCurrentDay',pinApprovalId:'synthetic',operator:'synthetic',approvalReference:'synthetic'});});expect(durable).toBe(true);
 assertPreparedAttemptsCall(ep,prepareCall(s,v,300000));expect(()=>assertPreparedAttemptsCall(ep,prepareCall(s,v,300000))).toThrow();
 claimAttemptsConnection(ep);expect(()=>claimAttemptsConnection(ep)).toThrow();claimAttemptsSocket(ep);expect(()=>claimAttemptsSocket(ep)).toThrow();claimAttemptsLogin(ep);
 const wrong=new Request('dbo.usp_ExportData_ResetReadData',()=>{});prepareTdsRequest(wrong,s,v,300000);expect(()=>assertPreparedAttemptsRpc(wrong,ep)).toThrow();
 const rpc=new Request('dbo.usp_ExportData_ReadDataCurrentDay',()=>{});prepareTdsRequest(rpc,s,v,300000);assertPreparedAttemptsRpc(rpc,ep);expect(()=>assertPreparedAttemptsRpc(rpc,ep)).toThrow();
});
it('failed fsync/claim or expired authorization never creates a permit',async()=>{
 const ep=prepareCorporateAttemptsEndpoint(endpoint,pin(),signature('ReadDataCurrentDay'),{TableName:'AttemptsRawData'});
 await expect(authorizeCorporateAttemptsOnce(ep,()=>{},async()=>{throw Error('DISK_FULL');})).rejects.toThrow('DISK_FULL');expect(()=>assertCorporateAttemptsExecution(ep)).toThrow();
});
it('CurrentDay and PreviousDays alternate only after full delivery; pending or failed delivery cannot advance',async()=>{
 const c=config(),state:CorporateCycleState={next:'current',pending:null},events:string[]=[];
 const b=createCorporateAttemptsBinding(c,pin(),cats,state,async i=>{events.push(i.procedure);return intentFiles({...i});},async()=>{events.push('confirmed');});
 expect(b.tlsVerified).toBeUndefined();expect(b.recoveryValidated).toBeUndefined();
 const signal=new AbortController().signal;await b.beforeAcquire!(b.plan(),signal);expect(events).toEqual(['ReadDataCurrentDay']);expect(state.next).toBe('current');
 await expect(b.beforeAcquire!(b.plan(),signal)).rejects.toThrow('RECOVERY_REQUIRED');
 await b.afterAcquire!({received:1,persisted:1,duplicates:0,rejected:0});expect(state.next).toBe('previous');
 await b.beforeAcquire!(b.plan(),signal);expect(events).toEqual(['ReadDataCurrentDay','confirmed','ReadDataPreviousDays']);
 expect(state.next).toBe('previous');expect(state.pending).not.toBeNull();
});
it('configuration expiry and unreviewed contract/date/profile stop before any intent',async()=>{
 for(const mutate of [(c:ReturnType<typeof config>)=>{c.authorization.expiresAt='2020-01-01T00:00:00.000Z';},
  (c:ReturnType<typeof config>)=>{c.previous!.plan={...c.previous!.plan,run:{...c.previous!.plan.run,exportDate:'2000-01-01'}};},(c:ReturnType<typeof config>)=>{c.profile.tmaPolicy='EndCall-minus-StartDate-provisional';}]){
  const c=config();mutate(c);const save=vi.fn();expect(()=>createCorporateAttemptsBinding(c,pin(),cats,{next:'current',pending:null},save,async()=>{})).toThrow();expect(save).not.toHaveBeenCalled();
 }
});
it('CLI requires explicit authorization and private case under the local root; no scheduler enabled by .env alone',()=>{
 const root=join(homedir(),'.local','state','olos-worker','corporate-attempts','synthetic-case');
 const args=['once','--work-dir',root,'--config','/private/source.json','--confirm','AUTHORIZE_ONE_ATTEMPTS_CURRENT_DAY'];
 expect(parseCorporateArguments(args,{SYNC_ENABLED:'false'})).toMatchObject({action:'once',intervalMs:60000});
 expect(()=>parseCorporateArguments(args,{SYNC_ENABLED:'true'})).toThrow();expect(parseCorporateArguments(args.slice(0,-2),{SYNC_ENABLED:'false'})).toMatchObject({action:'once'});
 expect(()=>parseCorporateArguments([...args.slice(0,-2),'--confirm','WRONG'],{SYNC_ENABLED:'false'})).toThrow('CORPORATE_AUTHORIZATION_REQUIRED');
 expect(()=>parseCorporateArguments([...args,'--interval-ms','10'],{SYNC_ENABLED:'false'})).toThrow();
 expect(()=>parseCorporateArguments(args.map(v=>v===root?'/home/madm/olos-offline-homologation/run-001':v),{SYNC_ENABLED:'false'})).toThrow();
});
it('plain CLI actions use the same private configuration/case and never enable generic synchronization',()=>{
 for(const action of ['once','start','recover','status'])expect(parseCorporateArguments([action],{})).toMatchObject({action,
  root:join(homedir(),'.local/state/olos-worker/corporate-attempts/first-run'),configFile:join(homedir(),'.config/olos-worker/attempts-corporate.json'),intervalMs:60000});
 expect(()=>parseCorporateArguments(['once'],{NODE_TLS_REJECT_UNAUTHORIZED:'0'})).toThrow();
});
it('a duplicate layout reference and repeated fixed policies are unnecessary; contract and recovery gates remain mandatory',()=>{
 const original=config(),{layoutEvidence:_l,partitionPolicy:_p,failurePolicy:_f,...authorization}=original.authorization;
 const c=corporateConfigSchema.parse({...original,authorization:{...authorization,sharedConsumptionEvidence:'R1',recoveryEvidence:'R2'}});
 expect(c.authorization.layoutEvidence).toBeUndefined();
 expect(c.authorization.failurePolicy).toBe('STOP_PRESERVE_CASE_RECONCILE_LOG_REREAD_NO_RESET');
 expect(c.authorization.partitionPolicy).toBe('ACQUISITION_DAY_CONTROL_BUCKET');
 const binding=createCorporateAttemptsBinding(c,pin(),cats,{next:'current',pending:null},vi.fn(),async()=>{});
 expect(()=>binding.source()).toThrow('DURABLE_INTENT_REQUIRED');
 expect(()=>corporateConfigSchema.parse({...c,authorization:{...c.authorization,recoveryEvidence:''}})).toThrow();
 expect(()=>createCorporateAttemptsBinding({...c,current:{...c.current,plan:{...c.current.plan,contract:{...c.current.plan.contract,approved:false}}}},pin(),cats,{next:'current',pending:null},vi.fn(),async()=>{})).toThrow();
 expect(()=>createCorporateAttemptsBinding({...c,authorization:{...c.authorization,layoutEvidence:'wrong'}},pin(),cats,{next:'current',pending:null},vi.fn(),async()=>{})).toThrow();
});
it('a failed local preflight cannot create a remote intent, source or spool',async()=>{
 const save=vi.fn(),state:CorporateCycleState={next:'current',pending:null},b=createCorporateAttemptsBinding(config(),pin(),cats,state,save,async()=>{});
 const preflight=vi.spyOn(PostgresLargeBatchPersistence.prototype,'preflight').mockRejectedValue(Error('SOURCE_CONTRACT_NOT_APPROVED'));
 const run=vi.spyOn(ProjectionDispatcher.prototype,'runBatch').mockResolvedValue(0);
 const pool={query:vi.fn(async(sql:string)=>({rows:sql.includes('count(*)')?[{n:0}]:[],rowCount:0}))} as unknown as Pool;
 const spool={active:vi.fn(async()=>null),create:vi.fn()} as unknown as SpoolRepository;
 try{await expect(createAttemptsPipeline(pool,pool,pool,spool,spoolLimits(),b).cycle(new AbortController().signal)).rejects.toThrow('SOURCE_CONTRACT_NOT_APPROVED');
  expect(preflight).toHaveBeenCalledOnce();expect(save).not.toHaveBeenCalled();expect(state.pending).toBeNull();expect(spool.create).not.toHaveBeenCalled();
  expect(()=>b.source()).toThrow('DURABLE_INTENT_REQUIRED');
 }finally{preflight.mockRestore();run.mockRestore();}
});
