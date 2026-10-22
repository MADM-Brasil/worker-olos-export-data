import {mkdir,readdir,unlink,rename,open,lstat} from 'node:fs/promises';
import {join,resolve} from 'node:path';
import {homedir} from 'node:os';
import {pathToFileURL} from 'node:url';
import {constants} from 'node:fs';
import {randomUUID} from 'node:crypto';
import type {PoolClient} from 'pg';
import {localSettings} from './local-worker.js';
import {privateDirectory,privateJson,durableExclusive} from './disposition-integration.js';
import {LocalSpool} from './local-spool.js';
import {loadOlosPinApproval} from '../src/security/olos-certificate-pin.js';
import {corporateConfigSchema,corporateObservationSchema,validateCorporateObservation,createCorporateAttemptsBinding,createCorporateResponseReplayBinding,validateCorporateConfiguration,type CorporateAttemptIntent,type CorporateObservationConfig} from '../src/integration/attempts/corporate.js';
import {reportedSignature} from '../src/olos/procedure-signatures.js';
import {prepareCorporateAttemptsEndpoint,authorizeCorporateAttemptsOnce} from '../src/olos/tds/attempts-access.js';
import {IsolatedTdsSource,replayTdsResponse} from '../src/olos/tds/source.js';
import {parserLimits} from '../src/olos/tds/raw-parser.js';
import {createLocalRuntimePools} from '../src/integration/attempts/pools.js';
import {AttemptsService} from '../src/integration/attempts/service.js';
import {createAttemptsPipeline,createAutomatedAttemptsService} from '../src/integration/attempts/pipeline.js';
import {spoolLimits} from '../src/large-batch/config.js';
import {catalog,parseLossless,recordsFromJson,profileSchema} from '../src/offline/model.js';
import {serveWorker} from '../src/main.js';
import {digest,validDate} from '../src/persistence/validation.js';
import type {AttemptsBinding} from '../src/integration/attempts/pipeline.js';
import {z} from 'zod';
import {runLogInspection} from './attempts-log-inspection.js';

async function exists(path:string){try{await lstat(path);return true;}catch(e){if((e as NodeJS.ErrnoException).code==='ENOENT')return false;throw e;}}
async function loadCatalog(path:string,key:'CampaignId'|'DispositionId',label:'CAMPAIGN'|'DISPOSITION'){
 try{return catalog(recordsFromJson(parseLossless(JSON.stringify(await privateJson(path))),true),key,'Description');}
 catch(error){throw Error(`${label}_CATALOG_${(error as NodeJS.ErrnoException).code==='ENOENT'?'MISSING':'INVALID'}`);}
}
export function parseCorporateArguments(args:readonly string[],env:NodeJS.ProcessEnv){
 const [action,...rest]=args,flags:Record<string,string>={};
 if(!action || !['once','start','recover','status','inspect-log'].includes(action) || env.SYNC_ENABLED!==undefined && env.SYNC_ENABLED!=='false' || env.NODE_TLS_REJECT_UNAUTHORIZED==='0')throw Error('CORPORATE_COMMAND_REJECTED');
 for(let i=0;i<rest.length;i+=2){const k=rest[i],v=rest[i+1];if(!k || !v || !['--work-dir','--config','--confirm','--interval-ms','--date-export'].includes(k) || Object.hasOwn(flags,k))throw Error('CORPORATE_COMMAND_REJECTED');flags[k]=v;}
 const base=join(homedir(),'.local','state','olos-worker','corporate-attempts'),root=flags['--work-dir']??join(base,'first-run');
 if(!root || resolve(root)!==root || !root.startsWith(`${base}/`) || root.slice(base.length+1).includes('/'))throw Error('CORPORATE_WORK_DIR_REJECTED');
 if(action!=='inspect-log' && flags['--confirm']==='AUTHORIZE_LOG_INSPECTION')throw Error('CORPORATE_COMMAND_REJECTED');
 if(action==='inspect-log' && (flags['--interval-ms']!==undefined || flags['--work-dir']!==undefined))throw Error('CORPORATE_COMMAND_REJECTED');
 const intervalMs=Number(flags['--interval-ms']??'60000');if(!Number.isSafeInteger(intervalMs) || intervalMs<60000 || intervalMs>3600000)throw Error('CORPORATE_INTERVAL_REJECTED');
 // The explicit CLI action plus the existing private authorization is sufficient; old confirmations remain accepted.
 if(action!=='status' && flags['--confirm']!==undefined && flags['--confirm']!==({once:'AUTHORIZE_ONE_ATTEMPTS_CURRENT_DAY',start:'AUTHORIZE_ATTEMPTS_RECURRING',recover:'LOCAL_RECOVERY_ONLY','inspect-log':'AUTHORIZE_LOG_INSPECTION'} as const)[action])throw Error('CORPORATE_AUTHORIZATION_REQUIRED');
 if (action === 'inspect-log' && flags['--confirm'] !== 'AUTHORIZE_LOG_INSPECTION')
   throw Error('CORPORATE_AUTHORIZATION_REQUIRED');
 if (action === 'inspect-log' && (!flags['--date-export'] || !validDate(flags['--date-export'])))
   throw Error('LOG_INSPECTION_DATE_REQUIRED');
 if (action !== 'inspect-log' && flags['--date-export'] !== undefined)
   throw Error('CORPORATE_COMMAND_REJECTED');
 return {action,base,root,configFile:flags['--config']??join(homedir(),'.config','olos-worker','attempts-corporate.json'),intervalMs,dateExport:flags['--date-export']};
}
async function readCorporateConfiguration(path:string|undefined){
 if(!path)throw Error('CORPORATE_CONFIGURATION_MISSING');
 try{
   let input=await privateJson(path);
   if(input && typeof input==='object' && Object.hasOwn(input,'preparationFile')){
     const {preparationFile,...overrides}=input as Record<string,unknown>;
     if(typeof preparationFile!=='string' || !preparationFile.startsWith('/') || preparationFile===path)throw Error('CORPORATE_PREPARATION_INVALID');
     let preparation:unknown;
     try{preparation=await privateJson(preparationFile);}catch(error){throw Error((error as NodeJS.ErrnoException).code==='ENOENT'?'CORPORATE_PREPARATION_MISSING':'CORPORATE_PREPARATION_INVALID');}
     const saved=z.object({kind:z.literal('olos-attempts-config-preparation/v1'),executable:z.literal(false),available:z.object({
       version:z.literal('olos-corporate-local/v1'),endpoint:z.unknown(),profileFile:z.string().startsWith('/'),campaignFile:z.string().startsWith('/'),dispositionFile:z.string().startsWith('/')
     }).strict()}).passthrough().safeParse(preparation);
     if(!saved.success)throw Error('CORPORATE_PREPARATION_INVALID');
     // Import only connection/mapping inputs. Never import approvals, contracts or permissions from a preparation.
     const defaults:Record<string,unknown>={...saved.data.available};
     if(Object.hasOwn(overrides,'profile'))delete defaults.profileFile;
     input={...defaults,...overrides};
   }
   if(input && typeof input==='object' && Object.hasOwn(input,'profileFile')){
     const {profileFile,profile,...rest}=input as Record<string,unknown>;
     if(profile!==undefined || typeof profileFile!=='string' || !profileFile.startsWith('/'))throw Error('CORPORATE_CONFIGURATION_INVALID');
     try{input={...rest,profile:profileSchema.parse(await privateJson(profileFile))};}
     catch(error){throw Error((error as NodeJS.ErrnoException).code==='ENOENT'?'CORPORATE_PROFILE_MISSING':'CORPORATE_PROFILE_INVALID');}
   }
   return z.union([corporateObservationSchema,corporateConfigSchema]).parse(input);
 }catch(error){
   if(error instanceof Error && ['CORPORATE_PROFILE_MISSING','CORPORATE_PROFILE_INVALID','CORPORATE_PREPARATION_MISSING','CORPORATE_PREPARATION_INVALID'].includes(error.message))throw error;
   throw Error((error as NodeJS.ErrnoException).code==='ENOENT'?'CORPORATE_CONFIGURATION_MISSING':'CORPORATE_CONFIGURATION_INVALID');
 }
}
/** Filesystem-only observations are not authorization or a PostgreSQL/source readiness probe. */
async function corporateStatus(options:ReturnType<typeof parseCorporateArguments>){
 const blockers=new Set<string>();
 const inspect=async(path:string):Promise<boolean|null>=>{try{return await exists(path);}catch{blockers.add('LOCAL_DIAGNOSTIC_STATE_UNREADABLE');return null;}};
 const configFile=options.configFile??join(homedir(),'.config','olos-worker','attempts-corporate.json');
 const configuration={exists:await inspect(configFile),valid:false,validationScope:'schema-only'};
 const tlsApproval:{exists:boolean|null;valid:boolean|null}={exists:null,valid:null};
 let cfg:Awaited<ReturnType<typeof readCorporateConfiguration>>|undefined;
 try{cfg=await readCorporateConfiguration(configFile);configuration.valid=true;}catch(error){blockers.add(corporateErrorCode(error));}
 if(cfg){
   tlsApproval.exists=await inspect(cfg.pinApprovalFile);
   if(tlsApproval.exists===false)blockers.add('TLS_APPROVAL_MISSING');
   else if(tlsApproval.exists){
     try{const pin=await loadOlosPinApproval(cfg.pinApprovalFile);tlsApproval.valid=pin.purpose==='OLOS_ATTEMPTS_TDS_ACQUISITION';
       if(!tlsApproval.valid)blockers.add('TLS_APPROVAL_PURPOSE_INVALID');
       try{if('contractMode' in cfg)validateCorporateObservation(cfg,pin);else validateCorporateConfiguration(cfg,pin);}catch(error){blockers.add(corporateErrorCode(error));}
     }catch(error){tlsApproval.valid=false;blockers.add(corporateErrorCode(error));}
   }
   for(const file of [cfg.campaignFile,cfg.dispositionFile])if(await inspect(file)!==true)blockers.add('LOCAL_CATALOG_UNAVAILABLE');
 }else blockers.add('TLS_APPROVAL_NOT_CHECKED_WITHOUT_VALID_CONFIGURATION');
 const remoteIntentPending=await inspect(join(options.base,'remote-pending.json'));
 if(remoteIntentPending!==false)blockers.add('RECOVERY_REQUIRED');
 const attempts:{attemptId:string;procedure:string;controlDate:string;confirmed:boolean}[]=[];
 const rootExists=await inspect(options.root);
 if(rootExists){try{
   await privateDirectory(options.root);
   const files=(await readdir(options.root)).filter(f=>/^attempt-[a-f0-9-]+\.json$/.test(f));
   if(files.length>10000)throw Error('CASE_LIMIT_REACHED');
   for(const f of files){const i=await privateJson(join(options.root,f)) as CorporateAttemptIntent;
     if(typeof i.attemptId!=='string' || !/^[a-f0-9-]{36}$/.test(i.attemptId) || f!==`attempt-${i.attemptId}.json` || !['ReadDataCurrentDay','ReadDataPreviousDays'].includes(i.procedure) || !validDate(i.controlDate))throw Error('RECOVERY_REQUIRES_REVIEW');
     const ackFile=join(options.root,`confirmed-${i.attemptId}.json`);
     let confirmed=false;
     if(await exists(ackFile)){const ack=await privateJson(ackFile) as {configurationDigest?:string};confirmed=/^[a-f0-9]{64}$/.test(i.configurationDigest) && ack.configurationDigest===i.configurationDigest;}
     attempts.push({attemptId:i.attemptId,procedure:i.procedure,controlDate:i.controlDate,confirmed});
     if(!confirmed)blockers.add('RECOVERY_REQUIRED');
   }
 }catch{blockers.add('RECOVERY_REQUIRES_REVIEW');}}
 let firstAcquisitionConfirmed:boolean|null=false;
 if(rootExists===null)firstAcquisitionConfirmed=null;
 else if(rootExists){try{
   const path=join(options.root,'first-confirmed.json');
   if(await exists(path)){
     await privateDirectory(options.root);
     const first=await privateJson(path) as {attemptId?:string;configurationDigest?:string};
     if(!attempts.some(a=>a.attemptId===first.attemptId && a.procedure==='ReadDataCurrentDay' && a.confirmed))throw Error('INVALID');
     const intent=await privateJson(join(options.root,`attempt-${first.attemptId}.json`)) as CorporateAttemptIntent;
     if(first.configurationDigest!==intent.configurationDigest)throw Error('INVALID');
     firstAcquisitionConfirmed=true;
   }
 }catch{firstAcquisitionConfirmed=null;blockers.add('RECOVERY_REQUIRES_REVIEW');}}
 return {event:'corporate_attempts_status',configuration,tlsApproval,remoteIntentPending,firstAcquisitionConfirmed,attempts,
   reconciliationRequired:remoteIntentPending!==false || blockers.has('RECOVERY_REQUIRED') || blockers.has('RECOVERY_REQUIRES_REVIEW'),
   blockingPreconditions:[...blockers],unverifiedPreconditions:['POSTGRES_SCHEMA_ROLES_AND_SOURCE_CONTRACT','CATALOG_CONTENTS_AND_CONFIGURATION_BINDING','SOURCE_TLS_HANDSHAKE','SOURCE_LAYOUT_SHARED_CONSUMPTION_AND_RECOVERY_EVIDENCE'],
   acquisitionAuthorized:false,networkAccessed:false};
}
/** No source operation occurs on import, boot, status or recovery. */
export async function corporateCommand(args:readonly string[],env:NodeJS.ProcessEnv){
 const options=parseCorporateArguments(args,env),{action,base,root}=options;
 if(action==='status'){
   process.stdout.write(`${JSON.stringify(await corporateStatus(options))}\n`);return;
 }
 if(action==='inspect-log'){const result=await runLogInspection(options.configFile,base,options.dateExport!);process.stdout.write(`${JSON.stringify(result)}\n`);return result;}

 if(action==='recover'){
   await privateDirectory(root);
   if(await exists(join(root,'spool','active-capture.json')) || await exists(join(root,'replay-confirmed.json'))){await recoverObservedResponse(options);return;}
   const raw=await privateJson(join(root,'local-delivery.json'));
   const schema=z.object({profile:profileSchema,campaigns:z.array(z.tuple([z.string(),z.string()])),dispositions:z.array(z.tuple([z.string(),z.string()])),digest:z.string().regex(/^[a-f0-9]{64}$/)}).strict();
   const saved=schema.parse(raw),{digest:checksum,...mapping}=saved;if(digest(mapping)!==checksum)throw Error('MAPPING_CHANGED');
   const binding:AttemptsBinding={kind:'isolated-fixture',tlsVerified:true,recoveryValidated:true,
     destinationId:'local.olos_worker.raw.olos_calls',mappingVersion:'local-approved-16/v1',sourceNamespace:'olos.ExportDataV3.AttemptsRawData',
     profile:saved.profile,catalogs:{campaigns:new Map(saved.campaigns),dispositions:new Map(saved.dispositions),agents:new Map()},
     plan:()=>{throw Error('LOCAL_RECOVERY_ONLY');},source:()=>{throw Error('LOCAL_RECOVERY_ONLY');}};
   const spool=await LocalSpool.open(join(root,'spool'),spoolLimits());
   if(!await exists(join(base,'remote-pending.json')))throw Error('RECOVERY_REQUIRES_REVIEW');
   const pending=await privateJson(join(base,'remote-pending.json')) as {workDir:string};if(pending.workDir!==root)throw Error('REMOTE_INTENT_CHANGED');
   const {passwords,postgres}=await localSettings();await runLocalCycle(action,createLocalRuntimePools(passwords,()=>{},postgres),spool,binding,'review');return;
 }
 // Everything is explicit, private and validated before touching PostgreSQL or opening a source.
 const cfg=await readCorporateConfiguration(options.configFile);
 let pin:Awaited<ReturnType<typeof loadOlosPinApproval>>;
 try{pin=await loadOlosPinApproval(cfg.pinApprovalFile);}catch(error){if((error as NodeJS.ErrnoException).code==='ENOENT')throw Error('TLS_APPROVAL_MISSING');throw error;}
 const catalogs={campaigns:await loadCatalog(cfg.campaignFile,'CampaignId','CAMPAIGN'),
  dispositions:await loadCatalog(cfg.dispositionFile,'DispositionId','DISPOSITION'),agents:new Map<string,string>()};
 if('contractMode' in cfg){
   if(action!=='once')throw Error('OBSERVED_CONTRACT_REQUIRES_REVIEW');
   await observeCorporateResponse(options,cfg,pin,catalogs);return;
 }
 const state:{next:'current'|'previous';pending:string|null}={next:'current',pending:null};
 const pendingFile=join(base,'remote-pending.json');
 const binding=createCorporateAttemptsBinding(cfg,pin,catalogs,state,async intent=>{
   await durableExclusive(pendingFile,{attemptId:intent.attemptId,workDir:root,configurationDigest:intent.configurationDigest});
   const intentPath=join(root,`attempt-${intent.attemptId}.json`);await durableExclusive(intentPath,intent);return {intentPath,pendingPath:pendingFile};
 },async(intent,result)=>{
   await durableExclusive(join(root,`confirmed-${intent.attemptId}.json`),{configurationDigest:intent.configurationDigest,acquisitionScopeDigest:binding.acquisitionScopeDigest,result});
   if(!(await exists(join(root,'first-confirmed.json'))))await durableExclusive(join(root,'first-confirmed.json'),{configurationDigest:intent.configurationDigest,acquisitionScopeDigest:binding.acquisitionScopeDigest,attemptId:intent.attemptId});
   const pending=await privateJson(pendingFile) as {attemptId:string};if(pending.attemptId!==intent.attemptId)throw Error('REMOTE_INTENT_CHANGED');
   await unlink(pendingFile);const dir=await open(base,constants.O_RDONLY|constants.O_DIRECTORY|constants.O_NOFOLLOW);try{await dir.sync();}finally{await dir.close();}
 });
 if(await exists(pendingFile))throw Error('RECOVERY_REQUIRED');
 if(action==='start'){
   if(!cfg.previous)throw Error('PREVIOUS_CONTRACT_UNCONFIRMED');
   if(!await exists(join(root,'first-confirmed.json')))throw Error('FIRST_MANUAL_CONFIRMATION_REQUIRED');
   await privateDirectory(root);const first=await privateJson(join(root,'first-confirmed.json')) as {configurationDigest:string;acquisitionScopeDigest?:string;attemptId:string};
   if((first.acquisitionScopeDigest?first.acquisitionScopeDigest!==binding.acquisitionScopeDigest:first.configurationDigest!==binding.configurationDigest) || typeof first.attemptId!=='string' || !/^[a-f0-9-]{36}$/.test(first.attemptId))throw Error('FIRST_MANUAL_CONFIRMATION_REQUIRED');
   const original=await privateJson(join(root,`attempt-${first.attemptId}.json`)) as CorporateAttemptIntent;
   const ack=await privateJson(join(root,`confirmed-${first.attemptId}.json`)) as {configurationDigest:string;acquisitionScopeDigest?:string};
   if(original.procedure!=='ReadDataCurrentDay' || original.configurationDigest!==first.configurationDigest || ack.configurationDigest!==original.configurationDigest ||
     (ack.acquisitionScopeDigest?ack.acquisitionScopeDigest!==binding.acquisitionScopeDigest:ack.configurationDigest!==binding.configurationDigest))throw Error('FIRST_MANUAL_CONFIRMATION_REQUIRED');
 }else if(action==='once' && await exists(join(root,'first-confirmed.json')))throw Error('FIRST_MANUAL_ALREADY_CONFIRMED_USE_START');
 await mkdir(base,{recursive:true,mode:0o700});await privateDirectory(base);
 if(!(await exists(root)))await privateDirectory(root,true);else await privateDirectory(root);
 const mapping={profile:cfg.profile,campaigns:[...catalogs.campaigns],dispositions:[...catalogs.dispositions]},mappingFile=join(root,'local-delivery.json');
 if(await exists(mappingFile)){const saved=await privateJson(mappingFile) as {digest:string};if(saved.digest!==digest(mapping))throw Error('MAPPING_CHANGED');}
 else await durableExclusive(mappingFile,{...mapping,digest:digest(mapping)});
 const attempts=(await readdir(root)).filter(f=>/^attempt-[a-f0-9-]+\.json$/.test(f));if(attempts.length>10000)throw Error('CASE_LIMIT_REACHED');
 let confirmed=0;
 for(const f of attempts){const intent=await privateJson(join(root,f)) as CorporateAttemptIntent;
   if(typeof intent.attemptId!=='string' || !/^[a-f0-9-]{36}$/.test(intent.attemptId))throw Error('RECOVERY_REQUIRED');
   if(!(await exists(join(root,`confirmed-${intent.attemptId}.json`))))state.pending=intent.attemptId;
   else{const ack=await privateJson(join(root,`confirmed-${intent.attemptId}.json`)) as {configurationDigest:string;acquisitionScopeDigest?:string};
     if(ack.configurationDigest!==intent.configurationDigest || (ack.acquisitionScopeDigest?ack.acquisitionScopeDigest!==binding.acquisitionScopeDigest:ack.configurationDigest!==binding.configurationDigest))throw Error('RECOVERY_REQUIRED');confirmed++;}
 }
 if(state.pending)throw Error('RECOVERY_REQUIRED');state.next=confirmed%2?'previous':'current';
 const {config,passwords,postgres}=await localSettings();
 const spool=await LocalSpool.open(join(root,'spool'),spoolLimits()),pools=createLocalRuntimePools(passwords,()=>{},postgres);
 if(action==='start'){
   const runtime=createAutomatedAttemptsService(pools,spool,spoolLimits(),binding,options.intervalMs);
   await serveWorker({...config,SYNC_INTERVAL_MS:options.intervalMs},[{name:'postgres',configured:true,probe:()=>runtime.probe(),close:async()=>{}}],runtime);return;
 }
 await runLocalCycle(action,pools,spool,binding,state.pending);
}
async function recoverObservedResponse(options:ReturnType<typeof parseCorporateArguments>){
 const {root,base}=options,cfg=await readCorporateConfiguration(options.configFile);
 if('contractMode' in cfg)throw Error('OBSERVED_CONTRACT_REQUIRES_REVIEW');
 const captureClaim=join(root,'spool','active-capture.json');
 const claim=await privateJson(await exists(captureClaim)?captureClaim:join(root,'replay-confirmed.json')) as {attemptId:string};
 if(typeof claim.attemptId!=='string' || !/^[a-f0-9-]{36}$/.test(claim.attemptId))throw Error('CORRUPT');
 const intent=await privateJson(join(root,`attempt-${claim.attemptId}.json`)) as CorporateAttemptIntent & {contractMode?:string};
 const pendingPath=join(base,'remote-pending.json');
 if(intent.contractMode!=='observe')throw Error('RECOVERY_REQUIRED');
 if(await exists(pendingPath)){
   const pending=await privateJson(pendingPath) as {attemptId:string;workDir:string;configurationDigest:string};
   if(pending.attemptId!==claim.attemptId || pending.workDir!==root || pending.configurationDigest!==intent.configurationDigest)throw Error('RECOVERY_REQUIRED');
 }else if(!await exists(join(root,`confirmed-${claim.attemptId}.json`)))throw Error('RECOVERY_REQUIRED');
 const capture=await LocalSpool.open(join(root,'spool'),spoolLimits()),observed=await capture.responseCapture(claim.attemptId);
 const catalogs={campaigns:await loadCatalog(cfg.campaignFile,'CampaignId','CAMPAIGN'),dispositions:await loadCatalog(cfg.dispositionFile,'DispositionId','DISPOSITION'),agents:new Map<string,string>()};
 const mapping={profile:cfg.profile,campaigns:[...catalogs.campaigns],dispositions:[...catalogs.dispositions]};
 const savedMapping=await privateJson(join(root,'local-delivery.json')) as {digest:string};
 if(savedMapping.digest!==digest(mapping))throw Error('MAPPING_CHANGED');
 const source=replayTdsResponse(cfg.current.fields,()=>capture.responseChunks(claim.attemptId),observed.received);
 const binding=createCorporateResponseReplayBinding(cfg,catalogs,source,observed.fields,intent.controlDate);
 const spool=await LocalSpool.open(join(root,'replay-spool'),spoolLimits());
 const replayDigest=digest({attemptId:claim.attemptId,sha256:observed.sha256,plan:cfg.current.plan,profile:cfg.profile,
   campaigns:[...catalogs.campaigns],dispositions:[...catalogs.dispositions]});
 const confirmedPath=join(root,'replay-confirmed.json'),startedPath=join(root,'replay-started.json');
 if(await exists(confirmedPath)){
   const confirmed=await privateJson(confirmedPath) as {digest:string};if(confirmed.digest!==replayDigest)throw Error('MAPPING_CHANGED');
   process.stdout.write('{"event":"observed_response_already_delivered","remoteCalled":false,"checkpointAdvanced":false}\n');
 }else{
 const started=await exists(startedPath);
 if(started){
   const saved=await privateJson(startedPath) as {digest:string};if(saved.digest!==replayDigest)throw Error('MAPPING_CHANGED');
   if(!await spool.active())throw Error('RECOVERY_REQUIRES_REVIEW'); // never blindly create a second run after uncertain local commit
 }else await durableExclusive(startedPath,{digest:replayDigest});
 const {passwords,postgres}=await localSettings();
 await runLocalCycle(started?'recover':'capture-replay',createLocalRuntimePools(passwords,()=>{},postgres),spool,binding,null);
 await durableExclusive(confirmedPath,{attemptId:claim.attemptId,digest:replayDigest});
 }
 // Tie the original observed acquisition to the subsequently reviewed contract, without rewriting its intent.
 const pin=await loadOlosPinApproval(cfg.pinApprovalFile);
 const approved=createCorporateAttemptsBinding(cfg,pin,catalogs,{next:'previous',pending:null},async()=>{throw Error('LOCAL_RECOVERY_ONLY');},async()=>{});
 const acknowledgment={configurationDigest:intent.configurationDigest,acquisitionScopeDigest:approved.acquisitionScopeDigest,
   captureSha256:observed.sha256,localReplay:true};
 const saveOnce=async(path:string,value:unknown)=>{
   if(await exists(path)){if(digest(await privateJson(path))!==digest(value))throw Error('RECOVERY_REQUIRES_REVIEW');}
   else await durableExclusive(path,value);
 };
 await saveOnce(join(root,`confirmed-${claim.attemptId}.json`),acknowledgment);
 await saveOnce(join(root,'first-confirmed.json'),{attemptId:claim.attemptId,configurationDigest:intent.configurationDigest,acquisitionScopeDigest:approved.acquisitionScopeDigest});
 // Retain the claims as audit evidence; release them only after both ingestion and delivery confirmed.
 for(const file of ['active-capture.json','active.json']){
   const from=join(root,'spool',file);if(await exists(from))await rename(from,join(root,'spool',`${claim.attemptId}-${file}`));
 }
 const spoolDir=await open(join(root,'spool'),constants.O_RDONLY|constants.O_DIRECTORY|constants.O_NOFOLLOW);try{await spoolDir.sync();}finally{await spoolDir.close();}
 if(await exists(pendingPath)){await unlink(pendingPath);const d=await open(base,constants.O_RDONLY|constants.O_DIRECTORY|constants.O_NOFOLLOW);try{await d.sync();}finally{await d.close();}}
 process.stdout.write(`${JSON.stringify({event:'observed_response_local_replay_completed',attemptId:claim.attemptId,
   remoteCalled:false,recurringAuthorized:false,previousContractRequired:!cfg.previous})}\n`);
}

/** Existing one-shot transport and POSIX spool, with no source contract fabricated before RPC. */
async function observeCorporateResponse(options:ReturnType<typeof parseCorporateArguments>,cfg:CorporateObservationConfig,
 pin:Awaited<ReturnType<typeof loadOlosPinApproval>>,catalogs:import('../src/offline/model.js').Catalogs){
 validateCorporateObservation(cfg,pin);
 const signature=reportedSignature('ReadDataCurrentDay',{evidence:'runtime acceptance policy: TableName required, null OUTPUTs allowed',
   nullable:{TableName:false,Message:true,ErrorMessage:true}});
 const values={TableName:'AttemptsRawData'},ep=prepareCorporateAttemptsEndpoint(cfg.endpoint,pin,signature,values);
 const {root,base}=options,pendingPath=join(base,'remote-pending.json');
 if(await exists(pendingPath))throw Error('RECOVERY_REQUIRED');
 if(await exists(join(root,'first-confirmed.json')))throw Error('FIRST_MANUAL_ALREADY_CONFIRMED_USE_START');
 if(await exists(root)){
   await privateDirectory(root);
   if((await readdir(root)).some(name=>/^attempt-[a-f0-9-]+\.json$/.test(name)))throw Error('RECOVERY_REQUIRED');
 }
 const {passwords,postgres}=await localSettings(),abort=new AbortController(),cancel=()=>abort.abort();
 const pools=createLocalRuntimePools(passwords,cancel,postgres),probe=new AttemptsService(pools);
 let lock:PoolClient|undefined,capture:Awaited<ReturnType<LocalSpool['createResponseCapture']>>|undefined,storageFailure:unknown;
 process.once('SIGTERM',cancel);process.once('SIGINT',cancel);
 try{
   lock=await pools.ingest.connect();lock.on('error',cancel);
   if(!(await lock.query('SELECT pg_try_advisory_lock(740219,1) AS acquired')).rows[0].acquired)throw Error('INSTANCE_BUSY');
   await probe.probe(); // fixed local database, exact 16 columns and least-privilege roles, read-only
   if(probe.snapshot().recoveryPending)throw Error('RECOVERY_REQUIRED');
   if(abort.signal.aborted)throw Error('RECOVERY_REQUIRED');
   await mkdir(base,{recursive:true,mode:0o700});await privateDirectory(base);
   await privateDirectory(root,!await exists(root));
   const mapping={profile:cfg.profile,campaigns:[...catalogs.campaigns],dispositions:[...catalogs.dispositions]};
   const mappingFile=join(root,'local-delivery.json');
   if(await exists(mappingFile)){const saved=await privateJson(mappingFile) as {digest:string};if(saved.digest!==digest(mapping))throw Error('MAPPING_CHANGED');}
   else await durableExclusive(mappingFile,{...mapping,digest:digest(mapping)});
   const spool=await LocalSpool.open(join(root,'spool'),spoolLimits());
   const attemptId=randomUUID(),configurationDigest=digest({cfg,pinApprovalId:pin.approvalId,fingerprint:pin.fingerprintSha256,mapping});
   const controlDate=new Intl.DateTimeFormat('en-CA',{timeZone:cfg.authorization.sourceTimeZone,year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date());
   const intent:CorporateAttemptIntent={attemptId,configurationDigest,procedure:'ReadDataCurrentDay',controlDate,
     operator:cfg.authorization.operator,approvalReference:cfg.authorization.reference,pinApprovalId:pin.approvalId,
     createdAt:new Date().toISOString(),next:'previous'};
   await authorizeCorporateAttemptsOnce(ep,()=>{validateCorporateObservation(cfg,pin);if(abort.signal.aborted)throw Error('RECOVERY_REQUIRED');},async()=>{
     await durableExclusive(pendingPath,{attemptId,workDir:root,configurationDigest});
     const intentPath=join(root,`attempt-${attemptId}.json`);await durableExclusive(intentPath,{...intent,contractMode:'observe'});
     capture=await spool.createResponseCapture(attemptId); // storage must exist BEFORE the permit/RPC
     return {intentPath,pendingPath};
   });
   const response=capture!;
   const source=new IsolatedTdsSource(ep,[],parserLimits,async chunk=>{try{await response.append(chunk);}catch(error){storageFailure=error;throw error;}});
   let received=0,completion:{returnValue:number;datasetCount:number;message:string|null;errorMessage:string|null}|undefined;
   for await(const event of source.call(signature,values,abort.signal)){
     if(event.kind==='row')received++;
     else completion={returnValue:event.returnValue,datasetCount:event.datasetCount,message:event.message,errorMessage:event.errorMessage};
   }
   if(!completion || received!==source.metrics.rows)throw Error('INCOMPLETE');
   const sealed=await response.seal({received,fields:source.observedFields,metadata:source.observedMetadata,
     transportSecurity:source.transportSecurity,...completion,
     metadataState:'observed',recovery:'LOCAL_REPLAY_REQUIRES_REVIEWED_CONTRACT'});
   process.stdout.write(`${JSON.stringify({event:'corporate_response_quarantined',attemptId,received,persisted:0,...sealed,
     metadataState:'observed',contractApproved:false,checkpointAdvanced:false,retryRemote:false,reviewRequired:true})}\n`);
   // No ACK, no first-confirmed, no pending removal: observation never authorizes recurrence.
 }catch(error){await capture?.interrupt().catch(()=>{});throw storageFailure??error;}
 finally{
   abort.abort();process.removeListener('SIGTERM',cancel);process.removeListener('SIGINT',cancel);
   if(lock){await lock.query('SELECT pg_advisory_unlock(740219,1)').catch(()=>{});lock.release(true);}await probe.stop();
 }
}

async function runLocalCycle(action:string,pools:import('../src/integration/attempts/service.js').RuntimePools,spool:LocalSpool,binding:AttemptsBinding,pending:string|null){
 const abort=new AbortController(),cancel=()=>abort.abort();
 const probe=new AttemptsService(pools);let lock:PoolClient|undefined;
 process.once('SIGTERM',cancel);process.once('SIGINT',cancel);
 try{
   lock=await pools.ingest.connect();lock.on('error',cancel);
   if(!(await lock.query('SELECT pg_try_advisory_lock(740219,1) AS acquired')).rows[0].acquired)throw Error('INSTANCE_BUSY');
   await probe.probe();const pipeline=createAttemptsPipeline(pools.ingest,pools.project,pools.receiver,spool,spoolLimits(),binding);
   if(action==='once' && (await spool.active() || pending))throw Error('RECOVERY_REQUIRED');
   // With an active spool cycle() only recovers/delivers locally, never calls binding.source().
   const result=action==='recover'?await pipeline.recoverLocal(abort.signal):await pipeline.cycle(abort.signal);
   process.stdout.write(`${JSON.stringify({event:action==='recover'?'local_recovery_requires_review':'manual_cycle_completed',...result,retryRemote:false})}\n`);
 }finally{
   abort.abort();process.removeListener('SIGTERM',cancel);process.removeListener('SIGINT',cancel);
   if(lock){await lock.query('SELECT pg_advisory_unlock(740219,1)').catch(()=>{});lock.release(true);}await probe.stop();
 }
}
export function corporateErrorCode(error:unknown):string{
   const detail=error && typeof error==='object' && 'diagnosticCode' in error?error.diagnosticCode:undefined;
   if(typeof detail==='string' && ['SOURCE_CONTRACT_MISSING','SOURCE_CONTRACT_NOT_APPROVED','SOURCE_CONTRACT_MISMATCH','SOURCE_CONTRACT_INVALID','PERSISTENCE_CONNECTION_READ_ONLY','PERSISTENCE_ROLE_PRIVILEGES_MISSING'].includes(detail))return detail;
   const safe=new Set(['RECOVERY_REQUIRED','FIRST_MANUAL_CONFIRMATION_REQUIRED','FIRST_MANUAL_ALREADY_CONFIRMED_USE_START','CORPORATE_APPROVAL_EXPIRED',
     'CORPORATE_CONTRACT_OR_SOURCE_DAY_UNCONFIRMED','DESTINATION_MAPPING_NOT_APPROVED','INSTANCE_BUSY','MAPPING_CHANGED','POSTGRES_RUNTIME_NOT_READY',
     'CORPORATE_CONFIGURATION_MISSING','CORPORATE_CONFIGURATION_INVALID','TLS_APPROVAL_MISSING','PIN_APPROVAL_REQUIRED','PIN_APPROVAL_INVALID','PIN_APPROVAL_EXPIRED','PIN_MISMATCH','CERT_HAS_EXPIRED',
     'CORPORATE_PROFILE_MISSING','CORPORATE_PROFILE_INVALID','CAMPAIGN_CATALOG_MISSING','CAMPAIGN_CATALOG_INVALID','DISPOSITION_CATALOG_MISSING','DISPOSITION_CATALOG_INVALID',
     'CORPORATE_PREPARATION_MISSING','CORPORATE_PREPARATION_INVALID','OBSERVED_CONTRACT_REQUIRES_REVIEW','PREVIOUS_CONTRACT_UNCONFIRMED',
     'CONTRACT_UNCONFIRMED','SCHEMA_NOT_READY','LEASE_LOST','COMMIT_UNKNOWN','SOURCE_INTERRUPTED','STORAGE_FAILED','DISK_FULL','QUOTA_EXCEEDED','CORRUPT','INCOMPLETE','RECOVERY_AFTER_EXPIRED_LEASE_OR_HARD_CRASH_REQUIRES_REVIEW','DELIVERY_REQUIRES_RECONCILIATION',
     'CONNECTION_FAILED','EXECUTION_FAILED','INVALID_RESPONSE','TIMEOUT','ABORTED','OUTPUT_ERROR','REAL_EXECUTION_BLOCKED',
     'LOG_INSPECTION_PRIVATE_AUTHORIZATION_REQUIRED','LOG_INSPECTION_DATE_MISMATCH','REMOTE_OPERATION_REQUIRES_REVIEW','LOG_INSPECTION_DATE_REQUIRED','CORPORATE_COMMAND_REJECTED','CORPORATE_WORK_DIR_REJECTED','CORPORATE_AUTHORIZATION_REQUIRED','CORPORATE_INTERVAL_REJECTED','RECOVERY_REQUIRES_REVIEW']);
   if(error instanceof Error && safe.has(error.message))return error.message;
   const code=error && typeof error==='object' && 'code' in error?error.code:undefined;
   if(typeof code==='string' && safe.has(code))return code;
   if(code==='42P01' || code==='3F000')return 'SCHEMA_NOT_READY';
   if(code==='42501')return 'PERSISTENCE_ROLE_PRIVILEGES_MISSING';
   return 'CORPORATE_CONFIGURATION_OR_EXECUTION_REJECTED';
}
if(process.argv[1] && import.meta.url===pathToFileURL(resolve(process.argv[1])).href){
 void corporateCommand(process.argv.slice(2),process.env).then(result=>{if(result && result.state!=='RESPONSE_SEALED')process.exitCode=1;}).catch(error=>{
   const code=corporateErrorCode(error);
   process.stderr.write(`${JSON.stringify({event:'corporate_attempts_stopped',code,retryRemote:false,reviewRequired:true})}\n`);process.exitCode=1;
 });
}
