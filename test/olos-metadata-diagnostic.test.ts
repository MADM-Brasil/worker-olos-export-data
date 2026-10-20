import {afterAll,beforeAll,beforeEach,expect,it,vi} from 'vitest';
import {X509Certificate} from 'node:crypto';
import {syntheticCertificates} from './support/tls-diagnostic-fixture.js';
const state=vi.hoisted(()=>({configs:[] as unknown[],requests:[] as import('tedious').Request[],catalogRequests:[] as import('tedious').Request[],catalogRows:[] as Record<string,unknown>[],catalogError:undefined as number|string|undefined,connects:0,logins:0,closes:0,raw:Buffer.alloc(0),protocol:'TLSv1.2',error:false}));
vi.mock('tedious',async original=>{
 const actual=await original<typeof import('tedious')>();
 const {EventEmitter}=await import('node:events');
 class FakeConnection extends EventEmitter{
   config:import('tedious').ConnectionConfiguration;
   messageIo={tlsNegotiationComplete:true,securePair:{cleartext:{authorized:false,getProtocol:()=>state.protocol,getPeerCertificate:()=>({raw:state.raw})}}};
   constructor(config:import('tedious').ConnectionConfiguration){super();this.config=config;state.configs.push(config);}
   connect(callback:(error?:Error)=>void){state.connects++;this.once('connect',callback);this.sendLogin7Packet();}
   sendLogin7Packet(){state.logins++;this.emit('connect');}
   close(){state.closes++;}
   execSql(request:import('tedious').Request){
     state.catalogRequests.push(request);const cb=(request as unknown as {callback:(error?:Error)=>void}).callback;
     if(state.catalogError){cb.call(request,Object.assign(Error('PRIVATE_PII_AND_PASSWORD'),typeof state.catalogError==='number'?{code:'EREQUEST',number:state.catalogError}:{code:state.catalogError}));return;}
     for(const row of state.catalogRows)request.emit('row',Object.entries(row).map(([colName,value])=>({metadata:{colName},value})));
     cb.call(request);
   }
   callProcedure(request:import('tedious').Request){state.requests.push(request);
     const cb=(request as unknown as {callback:(error?:Error)=>void}).callback;
     if(state.error){cb.call(request,Object.assign(Error('PRIVATE_PII_AND_PASSWORD'),{number:11526}));return;}
     request.emit('row',[
       {metadata:{colName:'column_ordinal'},value:1},{metadata:{colName:'name'},value:'AttemptsRawDataID'},
       {metadata:{colName:'system_type_name'},value:'int'},{metadata:{colName:'is_nullable'},value:false},
       {metadata:{colName:'unexpected'},value:'PRIVATE_PII_AND_PASSWORD'}]);cb.call(request);
   }
 }
 return {...actual,Connection:FakeConnection};
});
import {validateMetadataConfig,diagnoseOlosMetadata,metadataErrorCode,metadataErrorDetails,metadataFailure,attemptsDescriptionSql,attemptsCatalogSql} from '../src/diagnostics/olos-metadata.js';
import {corporateConfigSchema} from '../src/integration/attempts/corporate.js';
let certs:Awaited<ReturnType<typeof syntheticCertificates>>;
beforeAll(async()=>{certs=await syntheticCertificates();state.raw=new X509Certificate(certs.selfSigned).raw;},30000);
afterAll(async()=>{await certs?.stop();});
beforeEach(()=>{state.configs=[];state.requests=[];state.catalogRequests=[];state.catalogRows=[];state.catalogError=undefined;state.connects=0;state.logins=0;state.closes=0;state.protocol='TLSv1.2';state.error=false;});
const confirmation='SERVER_IDENTITY_NOT_VALIDATED';
function config(){return {version:1,tlsMode:'internal-homologation',password:'PRIVATE_PII_AND_PASSWORD',authorization:{operator:'synthetic',reference:'synthetic',issuedAt:new Date(Date.now()-1000).toISOString(),expiresAt:new Date(Date.now()+60000).toISOString()}};}
it('temporary TLS requires explicit risk confirmation, short-lived approval and fixed corporate endpoint',()=>{
 expect(()=>validateMetadataConfig(config(),'describe')).toThrow('INTERNAL_HOMOLOGATION_AUTHORIZATION_REQUIRED');
 expect(()=>validateMetadataConfig({...config(),authorization:undefined},'describe',confirmation)).toThrow();
 const expired=config();expired.authorization.expiresAt='2000-01-01T00:00:00.000Z';expect(()=>validateMetadataConfig(expired,'describe',confirmation)).toThrow('EXPIRED');
 for(const overrides of [{server:'127.0.0.1'},{port:1434},{database:'other'},{user:'sa'},{trustServerCertificate:true},{encrypt:false}])expect(()=>validateMetadataConfig({...config(),...overrides},'describe',confirmation)).toThrow();
 expect(corporateConfigSchema.safeParse(config()).success).toBe(false);
 expect(state.connects).toBe(0);
});
it('pinned is the default and never falls back to temporary trust',()=>{
 const {tlsMode:_,authorization:_a,...base}=config();
 expect(()=>validateMetadataConfig(base,'describe',confirmation)).toThrow('PIN_APPROVAL_REQUIRED');
 expect(validateMetadataConfig({...base,pinApprovalFile:'/private/approval.json'},'describe').tlsMode).toBe('pinned');
});
it('only two static-analysis RPCs execute in one encrypted diagnostic session; no acquisition approval or raw data',async()=>{
 const result=await diagnoseOlosMetadata(config(),'describe',confirmation);
 expect(result).toMatchObject({code:'METADATA_DESCRIBED',encrypted:true,serverIdentityValidated:false,acquisitionAuthorized:false,schedulerEnabled:false,sourceProceduresExecuted:false});
 expect(state.connects).toBe(1);expect(state.logins).toBe(1);expect(state.closes).toBe(1);
 expect(state.configs[0]).toMatchObject({server:'10.196.234.11',options:{database:'ExportDataV3',port:1433,encrypt:true,trustServerCertificate:true,maxRetriesOnTransientErrors:0,cryptoCredentialsDetails:{minVersion:'TLSv1.2',maxVersion:'TLSv1.2'}}});
 expect(state.requests).toHaveLength(2);
 for(const [index,request] of state.requests.entries()){
   expect(request.sqlTextOrProcedure).toBe('sys.sp_describe_first_result_set');
   expect(request.parameters[0]?.value).toBe(attemptsDescriptionSql(index?'ReadDataPreviousDays':'ReadDataCurrentDay'));
   expect(request.parameters[1]?.value).toBe('@m varchar(4000), @e varchar(4000)');
 }
 expect(JSON.stringify(result)).not.toContain('PRIVATE_PII_AND_PASSWORD');
});
it('TLS downgrade stops before login and RPC',async()=>{
 state.protocol='TLSv1.1';await expect(diagnoseOlosMetadata(config(),'describe',confirmation)).rejects.toThrow('METADATA_TLS_REQUIRED');
 expect(state.logins).toBe(0);expect(state.requests).toHaveLength(0);expect(state.connects).toBe(1);
});
it('real Tedious constructor accepts the diagnostic options without opening any connection',async()=>{
 await diagnoseOlosMetadata(config(),'describe',confirmation);
 const actual=await vi.importActual<typeof import('tedious')>('tedious');
 const options=state.configs[0] as import('tedious').ConnectionConfiguration;
 expect(()=>new actual.Connection({...options,options:{...options.options,connectionRetryInterval:0}})).toThrow('greater than 0');
 const connection=new actual.Connection(options);expect(connection.config.options.maxRetriesOnTransientErrors).toBe(0);connection.close();
});
it.each([
 ['configuration',new TypeError('PRIVATE_PII_AND_PASSWORD'),'METADATA_CONFIGURATION_REJECTED'],
 ['tls',Object.assign(Error('PRIVATE_PII_AND_PASSWORD'),{code:'ESOCKET',cause:Object.assign(Error('PRIVATE_PII_AND_PASSWORD'),{code:'ERR_SSL_UNSUPPORTED_PROTOCOL'})}),'METADATA_TLS_HANDSHAKE_FAILED'],
 ['login7',Object.assign(Error('PRIVATE_PII_AND_PASSWORD'),{code:'ELOGIN',number:18456}),'METADATA_LOGIN7_FAILED'],
 ['session-initialization',Object.assign(Error('PRIVATE_PII_AND_PASSWORD'),{number:5701}),'METADATA_SESSION_INITIALIZATION_FAILED'],
 ['describe',Object.assign(Error('METADATA_PRIVATE_SQL'),{code:'EREQUEST',number:11526}),'METADATA_DESCRIPTION_FAILED'],
 ['connection',Object.assign(Error('PRIVATE_PII_AND_PASSWORD'),{code:'ETIMEOUT'}),'METADATA_CONNECTION_TIMEOUT'],
 ['describe',Object.assign(Error('PRIVATE_PII_AND_PASSWORD'),{code:'ETIMEOUT'}),'METADATA_QUERY_TIMEOUT'],
] as const)('classifies %s errors using phase and safe codes only',(phase,error,code)=>{
 const failure=metadataFailure(error,phase),details=metadataErrorDetails(metadataFailure(failure,phase));
 expect(details).toMatchObject({code,phase});expect(JSON.stringify(details)).not.toContain('PRIVATE');expect(metadataErrorCode(failure)).toBe(code);
 if('number' in error)expect(details.sqlErrorNumber).toBe(error.number);
 if(phase==='tls')expect(details).toMatchObject({driverCode:'ESOCKET',tlsCode:'TLS_INCOMPATIBLE'});
});
it('description failure aborts without retry or second RPC and exposes only a stable code/number',async()=>{
 state.error=true;
 await expect(diagnoseOlosMetadata(config(),'describe',confirmation)).rejects.toMatchObject({message:'METADATA_DESCRIPTION_FAILED',sqlErrorNumber:11526});
 expect(state.connects).toBe(1);expect(state.requests).toHaveLength(1);expect(state.closes).toBe(1);
 expect(metadataErrorCode(Error('PRIVATE_PII_AND_PASSWORD'))).toBe('METADATA_CONFIGURATION_OR_EXECUTION_REJECTED');
 expect(()=>attemptsDescriptionSql('ResetReadData' as 'ReadDataCurrentDay')).toThrow('METADATA_OPERATION_REJECTED');
});
it('catalog discovers visible table/view candidates without assuming dbo or executing describe/consumption procedures',async()=>{
 state.catalogRows=[{schema_name:'source_a',object_name:'AttemptsRawData',object_id:10,object_type:'U',column_ordinal:1,name:'AttemptsRawDataID',type_name:'int',max_length:4,is_nullable:false},
   {schema_name:'source_b',object_name:'AttemptsRawData',object_id:11,object_type:'V',column_ordinal:1,name:'CampaignData',type_name:'varchar',max_length:-1,is_nullable:true,collation_name:'synthetic',collation_code_page:1252,unexpected:'PRIVATE_PII_AND_PASSWORD'}];
 const result=await diagnoseOlosMetadata(config(),'catalog',confirmation);
 expect(result).toMatchObject({code:'CATALOG_CANDIDATES_VISIBLE',sourceObjectRelationshipConfirmed:false,contractAutomaticallyApproved:false,sourceProceduresExecuted:false,acquisitionAuthorized:false});
 expect(JSON.stringify(result)).not.toContain('PRIVATE_PII_AND_PASSWORD');
 expect(state.requests).toHaveLength(0);expect(state.catalogRequests).toHaveLength(1);expect(state.connects).toBe(1);
 expect(state.catalogRequests[0]?.sqlTextOrProcedure).toBe(attemptsCatalogSql);
 expect(state.catalogRequests[0]?.parameters[0]).toMatchObject({name:'documentedName',value:'AttemptsRawData'});
 expect(attemptsCatalogSql).not.toMatch(/\bEXEC(?:UTE)?\b|sp_describe|usp_Export|FROM\s+dbo\./i);
 for(const object of ['objects','schemas','columns','types'])expect(attemptsCatalogSql).toContain(`[ExportDataV3].sys.${object}`);
});
it('empty catalog does not claim object absence or sufficient OEC visibility',async()=>{
 const result=await diagnoseOlosMetadata(config(),'catalog',confirmation);
 expect(result).toMatchObject({code:'CATALOG_OBJECT_NOT_VISIBLE_OR_ABSENT',rows:[],sourceObjectRelationshipConfirmed:false});
 expect(state.requests).toHaveLength(0);expect(state.catalogRequests).toHaveLength(1);
});
it('visible object without column metadata is reported as inconclusive',async()=>{
 state.catalogRows=[{schema_name:'source',object_name:'AttemptsRawData',object_id:10,object_type:'U',column_ordinal:null,name:null,type_name:null}];
 expect(await diagnoseOlosMetadata(config(),'catalog',confirmation)).toMatchObject({code:'CATALOG_COLUMNS_NOT_VISIBLE',sourceObjectRelationshipConfirmed:false});
});
it.each([[229,'METADATA_CATALOG_PERMISSION_DENIED'],['ETIMEOUT','METADATA_QUERY_TIMEOUT']] as const)('catalog failure %s stops without fallback or second query',async(error,code)=>{
 state.catalogError=error;
 await expect(diagnoseOlosMetadata(config(),'catalog',confirmation)).rejects.toMatchObject({message:code,phase:'catalog'});
 expect(state.requests).toHaveLength(0);expect(state.catalogRequests).toHaveLength(1);expect(state.connects).toBe(1);
});
it('catalog retains temporary authorization and credential requirements before connection',async()=>{
 await expect(diagnoseOlosMetadata(config(),'catalog')).rejects.toThrow('INTERNAL_HOMOLOGATION_AUTHORIZATION_REQUIRED');
 const {password:_,...withoutPassword}=config();
 await expect(diagnoseOlosMetadata(withoutPassword,'catalog',confirmation)).rejects.toThrow('METADATA_CREDENTIAL_REQUIRED');expect(state.connects).toBe(0);
});
