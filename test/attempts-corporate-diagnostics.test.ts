import {afterEach,beforeEach,expect,it,vi} from 'vitest';
import {mkdtemp,mkdir,writeFile,rm,readdir} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {Socket} from 'node:net';
import {exampleProfile} from '../src/offline/model.js';

const state=vi.hoisted(()=>({home:'',settings:vi.fn(),pools:vi.fn()}));
vi.mock('node:os',async importOriginal=>({...await importOriginal<typeof import('node:os')>(),homedir:()=>state.home}));
vi.mock('../adapters/local-worker.js',()=>({localSettings:state.settings}));
vi.mock('../src/integration/attempts/pools.js',()=>({createLocalRuntimePools:state.pools}));
import {corporateCommand,corporateErrorCode} from '../adapters/attempts-corporate.js';

let root:string,configFile:string,output:ReturnType<typeof vi.spyOn>;
const env={SYNC_ENABLED:'false'};
beforeEach(async()=>{
 state.home=await mkdtemp(join(tmpdir(),'olos-cli-diagnostic-'));
 root=join(state.home,'.local/state/olos-worker/corporate-attempts/first-run');
 configFile=join(state.home,'.config/olos-worker/attempts-corporate.json');
 state.settings.mockReset().mockImplementation(()=>{throw Error('NO_RUNTIME_ACCESS');});
 state.pools.mockReset().mockImplementation(()=>{throw Error('NO_POOL_ACCESS');});
 vi.spyOn(Socket.prototype,'connect').mockImplementation(()=>{throw Error('NO_NETWORK_ACCESS');});
 output=vi.spyOn(process.stdout,'write').mockImplementation(()=>true);
});
afterEach(async()=>{
 expect(state.settings).not.toHaveBeenCalled();expect(state.pools).not.toHaveBeenCalled();
 expect(Socket.prototype.connect).not.toHaveBeenCalled();
 vi.restoreAllMocks();await rm(state.home,{recursive:true,force:true});
});
async function save(path:string,value:unknown){await mkdir(join(path,'..'),{recursive:true,mode:0o700});await writeFile(path,JSON.stringify(value),{mode:0o600});}
function configuration(){return {
 version:'olos-corporate-local/v1',endpoint:{password:'NEVER_PRINT_SECRET'},pinApprovalFile:join(state.home,'pin.json'),
 current:{plan:{},fields:[]},previous:{plan:{},fields:[]},profile:exampleProfile,
 campaignFile:join(state.home,'campaign.json'),dispositionFile:join(state.home,'disposition.json'),
 authorization:{operator:'test',reference:'test',issuedAt:new Date().toISOString(),expiresAt:new Date(Date.now()+60000).toISOString(),
 sourceTimeZone:'UTC',partitionPolicy:'ACQUISITION_DAY_CONTROL_BUCKET',layoutEvidence:'synthetic evidence',sharedConsumptionEvidence:'synthetic evidence',recoveryEvidence:'synthetic evidence',failurePolicy:'STOP_PRESERVE_CASE_RECONCILE_LOG_REREAD_NO_RESET'}
};}
async function status(args:string[]=[]){await corporateCommand(['status',...args],env);const text=String(output.mock.calls.at(-1)?.[0]);expect(text).not.toContain('NEVER_PRINT_SECRET');return JSON.parse(text);}

it('status works without configuration, certificate or case directory and creates no state',async()=>{
 const result=await status();
 expect(result).toMatchObject({configuration:{exists:false,valid:false},tlsApproval:{exists:null,valid:null},remoteIntentPending:false,
 firstAcquisitionConfirmed:false,acquisitionAuthorized:false,networkAccessed:false});
 expect(result.blockingPreconditions).toContain('CORPORATE_CONFIGURATION_MISSING');
 expect(result.unverifiedPreconditions).toContain('POSTGRES_SCHEMA_ROLES_AND_SOURCE_CONTRACT');
 expect(await readdir(state.home)).toEqual([]);
});
it('status diagnoses invalid configuration without leaking parse errors or credentials',async()=>{
 await save(configFile,{password:'NEVER_PRINT_SECRET'});
 expect(await status()).toMatchObject({configuration:{exists:true,valid:false},blockingPreconditions:expect.arrayContaining(['CORPORATE_CONFIGURATION_INVALID'])});
});
it('status reports missing or invalid TLS approval locally without loading the runtime',async()=>{
 await save(configFile,configuration());
 expect(await status()).toMatchObject({configuration:{exists:true,valid:true},tlsApproval:{exists:false,valid:null},blockingPreconditions:expect.arrayContaining(['TLS_APPROVAL_MISSING'])});
 await save(configuration().pinApprovalFile,{certificatePem:'NEVER_PRINT_SECRET'});
 expect(await status()).toMatchObject({tlsApproval:{exists:true,valid:false},blockingPreconditions:expect.arrayContaining(['PIN_APPROVAL_INVALID'])});
});
it('observation is explicit and still refuses missing TLS without accessing either database or source',async()=>{
 const {current:_c,previous:_p,...base}=configuration();
 await save(configFile,{...base,contractMode:'observe'});
 expect(await status()).toMatchObject({configuration:{valid:true},tlsApproval:{exists:false},acquisitionAuthorized:false});
 await expect(corporateCommand(['once'],env)).rejects.toThrow('TLS_APPROVAL_MISSING');
});
it('status reports global pending intent and verifies first confirmation against local intent and ack',async()=>{
 const attemptId='11111111-1111-4111-8111-111111111111',configurationDigest='a'.repeat(64);
 await save(join(root,'..','remote-pending.json'),{password:'NEVER_PRINT_SECRET'});
 await save(join(root,`attempt-${attemptId}.json`),{attemptId,configurationDigest,procedure:'ReadDataCurrentDay',controlDate:'2026-10-19',password:'NEVER_PRINT_SECRET'});
 await save(join(root,`confirmed-${attemptId}.json`),{configurationDigest});
 await save(join(root,'first-confirmed.json'),{attemptId,configurationDigest});
 expect(await status()).toMatchObject({remoteIntentPending:true,reconciliationRequired:true,firstAcquisitionConfirmed:true});
 await save(join(root,`confirmed-${attemptId}.json`),{configurationDigest:'b'.repeat(64)});
 expect(await status()).toMatchObject({firstAcquisitionConfirmed:null,blockingPreconditions:expect.arrayContaining(['RECOVERY_REQUIRES_REVIEW'])});
});
it('status preserves uncertainty on corrupt attempt files instead of terminating with a generic error',async()=>{
 await save(join(root,'attempt-11111111-1111-4111-8111-111111111111.json'),{password:'NEVER_PRINT_SECRET'});
 expect(await status(['--work-dir',root,'--config',configFile])).toMatchObject({reconciliationRequired:true,blockingPreconditions:expect.arrayContaining(['RECOVERY_REQUIRES_REVIEW'])});
});
it('once has specific sanitized configuration errors with default paths; wrong legacy confirmations remain refused',async()=>{
 const args=['once','--work-dir',root,'--config',configFile,'--confirm','AUTHORIZE_ONE_ATTEMPTS_CURRENT_DAY'];
 await expect(corporateCommand(args,env)).rejects.toThrow('CORPORATE_CONFIGURATION_MISSING');
 await save(configFile,{password:'NEVER_PRINT_SECRET'});
 await expect(corporateCommand(args,env)).rejects.toThrow('CORPORATE_CONFIGURATION_INVALID');
 await expect(corporateCommand(['once'],env)).rejects.toThrow('CORPORATE_CONFIGURATION_INVALID');
 await expect(corporateCommand(['once','--confirm','WRONG'],env)).rejects.toThrow('CORPORATE_AUTHORIZATION_REQUIRED');
 expect(corporateErrorCode(Error('CORPORATE_CONFIGURATION_INVALID'))).toBe('CORPORATE_CONFIGURATION_INVALID');
 expect(corporateErrorCode(Error('NEVER_PRINT_SECRET'))).toBe('CORPORATE_CONFIGURATION_OR_EXECUTION_REJECTED');
});
it('the existing private profile can be referenced without a duplicate embedded approval or content changes',async()=>{
 const profileFile=join(state.home,'profile-16-columns.json');
 const {profile,...base}=configuration();await save(profileFile,profile);await save(configFile,{...base,profileFile});
 expect(await status()).toMatchObject({configuration:{valid:true},tlsApproval:{exists:false}});
 await save(configFile,{...base,profileFile,profile});expect(await status()).toMatchObject({blockingPreconditions:expect.arrayContaining(['CORPORATE_CONFIGURATION_INVALID'])});
 await save(configFile,{...base,profileFile:join(state.home,'missing-profile.json')});expect(await status()).toMatchObject({blockingPreconditions:expect.arrayContaining(['CORPORATE_PROFILE_MISSING'])});
});
it('preparationFile reuses credentials, profile and catalogs without importing any authorization or contract',async()=>{
 const c=configuration(),profileFile=join(state.home,'profile.json'),preparationFile=join(state.home,'preparation.json');await save(profileFile,c.profile);
 const preparation={kind:'olos-attempts-config-preparation/v1',executable:false,available:{version:c.version,endpoint:c.endpoint,profileFile,campaignFile:c.campaignFile,dispositionFile:c.dispositionFile},
   authorization:c.authorization,current:c.current,previous:c.previous,pinApprovalFile:c.pinApprovalFile};
 await save(preparationFile,preparation);
 await save(configFile,{preparationFile,pinApprovalFile:c.pinApprovalFile,current:c.current,previous:c.previous,authorization:c.authorization});
 expect(await status()).toMatchObject({configuration:{valid:true},tlsApproval:{exists:false}});
 // A preparation carrying approval-looking fields cannot authorize a runtime by itself.
 await save(configFile,{preparationFile});
 expect(await status()).toMatchObject({configuration:{valid:false},blockingPreconditions:expect.arrayContaining(['CORPORATE_CONFIGURATION_INVALID'])});
 await save(configFile,{preparationFile:join(state.home,'missing.json')});
 expect(await status()).toMatchObject({blockingPreconditions:expect.arrayContaining(['CORPORATE_PREPARATION_MISSING'])});
 await save(preparationFile,{...preparation,executable:true});await save(configFile,{preparationFile});
 expect(await status()).toMatchObject({blockingPreconditions:expect.arrayContaining(['CORPORATE_PREPARATION_INVALID'])});
});
