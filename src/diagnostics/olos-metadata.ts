import {createRequire} from 'node:module';
import {X509Certificate} from 'node:crypto';
import type {Socket} from 'node:net';
import {Connection,Request,TYPES,type ConnectionConfiguration} from 'tedious';
import {z} from 'zod';
import {PinnedMessageIO} from './pinned-message-io.js';
import {diagnoseTdsTls,classifyTlsError} from './tds-tls.js';
import {loadOlosPinApproval,type OlosPinApproval} from '../security/olos-certificate-pin.js';

export const metadataConfigSchema=z.object({
 version:z.literal(1),tlsMode:z.enum(['pinned','internal-homologation']).default('pinned'),
 server:z.literal('10.196.234.11').default('10.196.234.11'),port:z.literal(1433).default(1433),
 database:z.literal('ExportDataV3').default('ExportDataV3'),user:z.literal('OEC').default('OEC'),
 password:z.string().min(1).optional(),pinApprovalFile:z.string().startsWith('/').optional(),
 authorization:z.object({operator:z.string().min(1).max(100),reference:z.string().min(1).max(200),issuedAt:z.string().datetime(),expiresAt:z.string().datetime()}).strict().optional()
}).strict();
type Config=z.infer<typeof metadataConfigSchema>;
export type MetadataAction='connect'|'describe'|'catalog';
export type MetadataPhase='configuration'|'connection'|'tls'|'login7'|'session-initialization'|'describe'|'catalog';
const driverCodes=new Set(['ELOGIN','ETIMEOUT','ESOCKET','EENCRYPT','EREQUEST','ECANCEL','ECONNRESET','ECONNREFUSED','ETIMEDOUT','EHOSTUNREACH','ENETUNREACH','ENOTFOUND','EAI_AGAIN','EPIPE','ETDS','EINTERFACENOTSUPP']);
const diagnosticCodes=new Set(['METADATA_CONFIGURATION_REJECTED','METADATA_CREDENTIAL_REQUIRED','METADATA_OPERATION_REJECTED','METADATA_REDIRECT_OR_RETRY_REJECTED',
 'METADATA_TLS_REQUIRED','METADATA_DESCRIPTION_FAILED','METADATA_NOT_DETERMINED','METADATA_RESPONSE_REJECTED','METADATA_CONNECTION_FAILED',
 'METADATA_QUERY_TIMEOUT','METADATA_CONNECTION_TIMEOUT','METADATA_TLS_HANDSHAKE_FAILED','METADATA_LOGIN7_FAILED','METADATA_SESSION_INITIALIZATION_FAILED','METADATA_CATALOG_FAILED','METADATA_CATALOG_PERMISSION_DENIED',
 'METADATA_COMMAND_REJECTED','METADATA_AUTHORIZATION_REQUIRED','INTERNAL_HOMOLOGATION_AUTHORIZATION_REQUIRED','INTERNAL_HOMOLOGATION_AUTHORIZATION_EXPIRED',
 'PIN_APPROVAL_REQUIRED','PIN_APPROVAL_INVALID','PIN_APPROVAL_EXPIRED','PIN_APPROVAL_PURPOSE_REJECTED','PIN_CONFIGURATION_INVALID','PIN_MISMATCH','CERT_HAS_EXPIRED']);
function errorChain(error:unknown):Record<string,unknown>[]{
 const found:Record<string,unknown>[]=[],queue=[error];
 while(queue.length && found.length<10){const e=queue.shift();if(!e || typeof e!=='object' || found.includes(e as Record<string,unknown>))continue;
   const value=e as Record<string,unknown>;found.push(value);if(value.cause)queue.push(value.cause);if(Array.isArray(value.errors))queue.push(...value.errors.slice(0,10));
 }return found;
}
/** Preserve only phase, allowlisted driver codes and numeric SQL errors, never original messages/causes. */
export function metadataFailure(error:unknown,phase:MetadataPhase):Error{
 const chain=errorChain(error),codes=chain.flatMap(e=>[e.code,e.driverCode]).filter((c):c is string=>typeof c==='string');
 const tls=chain.flatMap(e=>[e.tlsCode,classifyTlsError(e)]).find((c):c is string=>typeof c==='string' && ['CERT_UNTRUSTED','CERT_NAME_MISMATCH','CERT_EXPIRED','CERT_INVALID','TLS_INCOMPATIBLE','TLS_NOT_SUPPORTED','PIN_REJECTED'].includes(c));
 const existing=error instanceof Error && diagnosticCodes.has(error.message)?error.message:undefined;
 const timeout=codes.some(c=>c==='ETIMEOUT' || c==='ETIMEDOUT');
 const code=timeout?(['describe','catalog'].includes(phase)?'METADATA_QUERY_TIMEOUT':'METADATA_CONNECTION_TIMEOUT'):
   existing??(tls || codes.includes('EENCRYPT')?'METADATA_TLS_HANDSHAKE_FAILED':codes.includes('ELOGIN')?'METADATA_LOGIN7_FAILED':
     phase==='configuration'?'METADATA_CONFIGURATION_REJECTED':phase==='session-initialization'?'METADATA_SESSION_INITIALIZATION_FAILED':
     phase==='describe'?'METADATA_DESCRIPTION_FAILED':phase==='catalog'?'METADATA_CATALOG_FAILED':phase==='login7'?'METADATA_LOGIN7_FAILED':phase==='tls'?'METADATA_TLS_HANDSHAKE_FAILED':'METADATA_CONNECTION_FAILED');
 const number=chain.map(e=>e.sqlErrorNumber??e.number).find(n=>typeof n==='number' && Number.isSafeInteger(n));
 const driverCode=codes.find(c=>driverCodes.has(c));
 return Object.assign(Error(code),{phase,...(driverCode?{driverCode}:{}),...(tls?{tlsCode:tls}:{}),...(number!==undefined?{sqlErrorNumber:number}:{})});
}
const procedures=['ReadDataCurrentDay','ReadDataPreviousDays'] as const;
// These strings are arguments to static analysis, NEVER submitted as executable batches.
export const attemptsDescriptionSql=(procedure:typeof procedures[number])=>{
 if(!procedures.includes(procedure))throw Error('METADATA_OPERATION_REJECTED');
 return `EXEC dbo.usp_ExportData_${procedure} @TableName='AttemptsRawData', @Message=@m OUTPUT, @ErrorMessage=@e OUTPUT;`;
};
const columns=new Set(['column_ordinal','name','is_nullable','system_type_id','system_type_name','max_length','precision','scale','collation_name','tds_type_id','tds_length','tds_collation_id','tds_collation_sort_id']);
export function validateMetadataConfig(input:unknown,action:MetadataAction,confirmation?:string):Config{
 const c=metadataConfigSchema.parse(input);
 if(process.env.NODE_TLS_REJECT_UNAUTHORIZED==='0' || createRequire(import.meta.url)('tedious/package.json').version!=='18.6.2')throw Error('METADATA_CONFIGURATION_REJECTED');
 if(action!=='connect' && !c.password)throw Error('METADATA_CREDENTIAL_REQUIRED');
 if(c.tlsMode==='internal-homologation'){
   const a=c.authorization,now=Date.now();
   if(confirmation!=='SERVER_IDENTITY_NOT_VALIDATED' || !a)throw Error('INTERNAL_HOMOLOGATION_AUTHORIZATION_REQUIRED');
   if(Date.parse(a.issuedAt)>now || Date.parse(a.expiresAt)<=now || Date.parse(a.expiresAt)<=Date.parse(a.issuedAt) || Date.parse(a.expiresAt)-Date.parse(a.issuedAt)>4*3600000)throw Error('INTERNAL_HOMOLOGATION_AUTHORIZATION_EXPIRED');
   if(c.pinApprovalFile)throw Error('METADATA_CONFIGURATION_REJECTED');
 }else if(!c.pinApprovalFile)throw Error('PIN_APPROVAL_REQUIRED');
 return c;
}
/** Private diagnostic session; never returned to callers or used by acquisition/runtime. */
class MetadataConnection extends Connection{
 private connectedOnce=false;
 private encrypted=false;
 diagnosticPhase:MetadataPhase='connection';
 constructor(config:ConnectionConfiguration,private readonly pin?:OlosPinApproval){super(config);}
 override connectOnPort(port:number,multi:boolean,signal:AbortSignal,connector?:()=>Promise<Socket>):void{
   if(this.connectedOnce || this.routingData || port!==1433 || multi || connector){this.emit('connect',Error('METADATA_REDIRECT_OR_RETRY_REJECTED'));this.close();return;}
   this.connectedOnce=true;super.connectOnPort(port,multi,signal,connector);
 }
 override socketHandlingForSendPreLogin(socket:Socket):void{
   if(!this.pin)return super.socketHandlingForSendPreLogin(socket);
   socket.on('error',error=>this.socketError(error));socket.on('close',()=>this.socketClose());socket.on('end',()=>this.socketEnd());
   socket.setKeepAlive(true,30000);this.messageIo=new PinnedMessageIO(socket,this.config.options.packetSize,this.debug,this.pin);
   this.messageIo.on('secure',tls=>this.emit('secure',tls));this.socket=socket;this.closed=false;this.sendPreLogin();this.transitionTo(this.STATE.SENT_PRELOGIN);
 }
 override sendLogin7Packet():void{
   this.diagnosticPhase='tls';
   const tls=this.messageIo.securePair?.cleartext;
   const stop=(code:string)=>{this.emit('connect',Error(code));this.close();};
   if(!this.messageIo.tlsNegotiationComplete || tls?.getProtocol()!=='TLSv1.2' || this.pin && !tls.authorized){stop('METADATA_TLS_REQUIRED');return;}
   const raw=tls.getPeerCertificate().raw;
   if(!raw){stop('METADATA_TLS_REQUIRED');return;}
   const cert=new X509Certificate(raw);
   if(Date.parse(cert.validFrom)>Date.now() || Date.parse(cert.validTo)<=Date.now()){stop('CERT_HAS_EXPIRED');return;}
   this.encrypted=true;this.diagnosticPhase='login7';super.sendLogin7Packet();
 }
 override sendPreLogin():void{this.diagnosticPhase='tls';super.sendPreLogin();}
 override sendInitialSql():void{this.diagnosticPhase='session-initialization';super.sendInitialSql();}
 get tlsConfirmed(){return this.encrypted;}
}
async function describe(connection:Connection,procedure:typeof procedures[number]){
 return new Promise<Record<string,unknown>[]>((resolve,reject)=>{
   const rows:Record<string,unknown>[]=[];
   const request=new Request('sys.sp_describe_first_result_set',error=>{
     if(error){reject(metadataFailure(error,'describe'));return;}
     if(!rows.length){reject(Error('METADATA_NOT_DETERMINED'));return;}resolve(rows);
   });
   request.addParameter('tsql',TYPES.NVarChar,attemptsDescriptionSql(procedure),{length:4000});
   request.addParameter('params',TYPES.NVarChar,'@m varchar(4000), @e varchar(4000)',{length:4000});
   request.addParameter('browse_information_mode',TYPES.TinyInt,0);
   request.on('row',(cells:{metadata:{colName:string};value:unknown}[])=>{
     const row:Record<string,unknown>={};
     for(const cell of cells){if(!columns.has(cell.metadata.colName))continue;
       const v=cell.value;if(!(v===null || typeof v==='boolean' || typeof v==='number' && Number.isSafeInteger(v) || typeof v==='string' && Buffer.byteLength(v)<=2048)){
         reject(Error('METADATA_RESPONSE_REJECTED'));connection.close();return;}
       row[cell.metadata.colName]=v;
     }
     if(rows.length>=512 || typeof row.column_ordinal!=='number' || !('system_type_name' in row)){
       reject(Error('METADATA_RESPONSE_REJECTED'));connection.close();return;}
     rows.push(row);
   });
   connection.callProcedure(request);
 });
}
// Exact logical name from the manual; discover visible schemas instead of assuming dbo or a physical source.
export const attemptsCatalogSql=`SELECT TOP (513)
 s.name AS schema_name, o.name AS object_name, o.object_id, o.type AS object_type,
 c.column_id AS column_ordinal, c.name AS name,
 t.name AS type_name, c.system_type_id, c.user_type_id,
 c.max_length, c.precision, c.scale, c.is_nullable, c.collation_name,
 CONVERT(int, COLLATIONPROPERTY(c.collation_name, 'CodePage')) AS collation_code_page
 FROM [ExportDataV3].sys.objects AS o
 JOIN [ExportDataV3].sys.schemas AS s ON s.schema_id=o.schema_id
 LEFT JOIN [ExportDataV3].sys.columns AS c ON c.object_id=o.object_id
 LEFT JOIN [ExportDataV3].sys.types AS t ON t.user_type_id=c.user_type_id
 WHERE DB_NAME()=N'ExportDataV3' AND o.type IN ('U','V') AND o.is_ms_shipped=0 AND o.name=@documentedName
 ORDER BY s.name,o.object_id,c.column_id;`;
const catalogColumns=new Set(['schema_name','object_name','object_id','object_type','column_ordinal','name','type_name','system_type_id','user_type_id',
 'max_length','precision','scale','is_nullable','collation_name','collation_code_page']);
async function inspectCatalog(connection:Connection){
 return new Promise<Record<string,unknown>[]>((resolve,reject)=>{
   const rows:Record<string,unknown>[]=[];
   const request=new Request(attemptsCatalogSql,error=>{
     if(error){const failure=metadataFailure(error,'catalog') as Error & {sqlErrorNumber?:number};
       if([229,230].includes(failure.sqlErrorNumber??0))failure.message='METADATA_CATALOG_PERMISSION_DENIED';
       reject(failure);return;}resolve(rows);
   });
   request.addParameter('documentedName',TYPES.NVarChar,'AttemptsRawData',{length:128});
   request.on('row',(cells:{metadata:{colName:string};value:unknown}[])=>{
     const row:Record<string,unknown>={};
     for(const cell of cells){if(!catalogColumns.has(cell.metadata.colName))continue;
       const value=cell.value;
       if(!(value===null || typeof value==='boolean' || typeof value==='number' && Number.isSafeInteger(value) || typeof value==='string' && Buffer.byteLength(value)<=2048)){
         reject(Error('METADATA_RESPONSE_REJECTED'));connection.close();return;}
       row[cell.metadata.colName]=value;
     }
     if(rows.length>=512 || typeof row.schema_name!=='string' || typeof row.object_name!=='string' || row.object_name.toLowerCase()!=='attemptsrawdata' || !['U','V'].includes(String(row.object_type).trim()) ||
       typeof row.object_id!=='number' || !(row.column_ordinal===null || typeof row.column_ordinal==='number' && row.column_ordinal>0)){
       reject(Error('METADATA_RESPONSE_REJECTED'));connection.close();return;}
     rows.push(row);
   });
   connection.execSql(request);
 });
}
export async function diagnoseOlosMetadata(input:unknown,action:MetadataAction,confirmation?:string){
 const c=validateMetadataConfig(input,action,confirmation),temporary=c.tlsMode==='internal-homologation';
 const pin=c.pinApprovalFile?await loadOlosPinApproval(c.pinApprovalFile):undefined;
 if(pin && pin.purpose!=='OLOS_TDS_TLS_METADATA_ONLY')throw Error('PIN_APPROVAL_PURPOSE_REJECTED');
 const security={tlsMode:c.tlsMode,serverIdentityValidated:!temporary,acquisitionAuthorized:false,schedulerEnabled:false};
 if(action==='connect'){
   const result=await diagnoseTdsTls({host:c.server,port:c.port,serverName:c.server,mode:temporary?'inspect-unverified':'verify-pinned',...(pin?{pinApproval:pin}:{})});
   return {...security,serverIdentityValidated:result.verified,encrypted:['PIN_VERIFIED','INSPECTED_UNVERIFIED'].includes(result.code),...result,sqlExecuted:false,sqlAuthentication:false};
 }
 let connection:MetadataConnection;
 try{connection=new MetadataConnection({server:c.server,authentication:{type:'default',options:{userName:c.user,password:c.password!}},
   options:{port:c.port,database:c.database,serverName:c.server,encrypt:true,trustServerCertificate:temporary,
     cryptoCredentialsDetails:{minVersion:'TLSv1.2',maxVersion:'TLSv1.2'},tdsVersion:'7_4',connectTimeout:10000,requestTimeout:15000,
     maxRetriesOnTransientErrors:0,readOnlyIntent:true}},pin);
 }catch(error){throw metadataFailure(error,'configuration');}
 let timer:ReturnType<typeof setTimeout>|undefined;
 const stopped=new Promise<never>((_,reject)=>{
   timer=setTimeout(()=>{reject(metadataFailure({code:'ETIMEOUT'},connection.diagnosticPhase));connection.close();},45000);
   connection.on('error',error=>reject(metadataFailure(error,connection.diagnosticPhase)));
   connection.on('errorMessage',token=>{if(connection.diagnosticPhase==='session-initialization'){reject(metadataFailure(token,'session-initialization'));connection.close();}});
   connection.on('end',()=>reject(metadataFailure({},connection.diagnosticPhase)));
 });
 try{return await Promise.race([(async()=>{
   await new Promise<void>((resolve,reject)=>connection.connect(error=>error?reject(metadataFailure(error,connection.diagnosticPhase)):resolve()));
   if(!connection.tlsConfirmed)throw Error('METADATA_TLS_REQUIRED');
   if(action==='catalog'){
     connection.diagnosticPhase='catalog';validateMetadataConfig(c,action,confirmation);
     const rows=await inspectCatalog(connection),hasColumns=rows.length>0 && rows.every(row=>typeof row.column_ordinal==='number' && typeof row.type_name==='string' && typeof row.name==='string');
     return {...security,encrypted:true,code:!rows.length?'CATALOG_OBJECT_NOT_VISIBLE_OR_ABSENT':hasColumns?'CATALOG_CANDIDATES_VISIBLE':'CATALOG_COLUMNS_NOT_VISIBLE',
       database:'ExportDataV3',documentedName:'AttemptsRawData',rows,sourceProceduresExecuted:false,sourceObjectRelationshipConfirmed:false,contractAutomaticallyApproved:false,
       limitation:!rows.length?'NO_VISIBLE_EXACT_MATCH_OBJECT_MAY_BE_HIDDEN_ABSENT_OR_DIFFERENTLY_NAMED':hasColumns?'CATALOG_LAYOUT_DOES_NOT_PROVE_PROCEDURE_RESULT':'OBJECT_VISIBLE_WITHOUT_COMPLETE_COLUMN_METADATA'};
   }
   connection.diagnosticPhase='describe';
   const datasets=[];for(const procedure of procedures){validateMetadataConfig(c,action,confirmation);datasets.push({procedure,columns:await describe(connection,procedure)});}
   return {...security,encrypted:true,code:'METADATA_DESCRIBED',datasets,sourceProceduresExecuted:false,contractAutomaticallyApproved:false};
 })(),stopped]);}catch(error){throw metadataFailure(error,(error as {phase?:MetadataPhase})?.phase??connection.diagnosticPhase);}finally{clearTimeout(timer);connection.close();}
}
export function metadataErrorCode(error:unknown){
 if(error instanceof z.ZodError || error instanceof SyntaxError || error instanceof TypeError || errorChain(error).some(e=>['ENOENT','EACCES','EPERM','ELOOP'].includes(String(e.code))))return 'METADATA_CONFIGURATION_REJECTED';
 const message=error instanceof Error?error.message:'';
 if(diagnosticCodes.has(message))return message;
 const tls=classifyTlsError(error);return tls==='INVALID_TDS'?'METADATA_CONFIGURATION_OR_EXECUTION_REJECTED':tls;
}
export function metadataErrorDetails(error:unknown){
 const details:Record<string,unknown>={code:metadataErrorCode(error)};
 if(error && typeof error==='object'){
   const e=error as Record<string,unknown>;
   if(['configuration','connection','tls','login7','session-initialization','describe','catalog'].includes(String(e.phase)))details.phase=e.phase;
   if(typeof e.driverCode==='string' && driverCodes.has(e.driverCode))details.driverCode=e.driverCode;
   if(typeof e.sqlErrorNumber==='number' && Number.isSafeInteger(e.sqlErrorNumber))details.sqlErrorNumber=e.sqlErrorNumber;
   if(typeof e.tlsCode==='string' && ['CERT_UNTRUSTED','CERT_NAME_MISMATCH','CERT_EXPIRED','CERT_INVALID','TLS_INCOMPATIBLE','TLS_NOT_SUPPORTED','PIN_REJECTED'].includes(e.tlsCode))details.tlsCode=e.tlsCode;
 }return details;
}
