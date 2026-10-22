import {open,realpath} from 'node:fs/promises';
import {constants,openSync,closeSync,fstatSync,readFileSync,existsSync,lstatSync,realpathSync} from 'node:fs';
import {dirname,join,isAbsolute} from 'node:path';
import { z } from 'zod';
import { TYPES, type ConnectionConfiguration, type Connection, type Request } from 'tedious';
import { assertOlosPinApproval, type OlosPinApproval } from '../../security/olos-certificate-pin.js';
import { prepareCall } from '../client.js';
import { compareSignatures, verifiedProcedureContracts } from '../procedure-signatures.js';
import type { PreparedCall, ProcedureSignature } from '../types.js';
import { OlosError } from '../types.js';
import { digest as requireIntentDigest, validDate, canonical } from '../../persistence/validation.js';

const schema = z.object({ server: z.literal('10.196.234.11'),port: z.literal(1433),database: z.literal('ExportDataV3'),
  user: z.literal('OEC'),password: z.string().min(1) }).strict();
export type PreparedAttemptsEndpoint = Readonly<z.infer<typeof schema> & { ca: string; serverName: '10.196.234.11' }>;
const prepared = new WeakMap<object,{ pin: OlosPinApproval; call: PreparedCall }>();

/** Original inspection evidence is immutable. Only an append-only, externally evidenced technical review can clear exclusion. */
export function blockingCorporateInspection(base:string):boolean{
 const path=join(base,'log-inspection-active.json');if(!existsSync(path))return false;
 const read=(file:string)=>{if(realpathSync(file)!==file)throw new OlosError('REAL_EXECUTION_BLOCKED');const fd=openSync(file,constants.O_RDONLY|constants.O_NOFOLLOW);
  try{const st=fstatSync(fd);if(!st.isFile() || st.uid!==process.getuid?.() || st.mode&0o077 || st.size>32768)throw new OlosError('REAL_EXECUTION_BLOCKED');return JSON.parse(readFileSync(fd,'utf8')) as Record<string,unknown>;}finally{closeSync(fd);}};
 try{
  const marker=read(path),clearance=read(join(base,'log-inspection-clearance.json'));
  if(clearance.kind!=='olos-inspection-clearance/v1' || clearance.caseId!==marker.attemptId || clearance.markerDigest!==requireIntentDigest(marker) ||
   typeof clearance.recordPath!=='string' || !clearance.recordPath.startsWith(join(base,'inspection-reconciliations')+'/'))return true;
  const r=read(clearance.recordPath),audit=r.externalEvidence as Record<string,unknown>|null;
  if(r.decision!=='TECHNICAL_CLEARANCE' || r.remoteOutcome!=='REMOTELY_ESTABLISHED' || !r.operator || !r.reference ||
   typeof r.issuedAt!=='string' || !Number.isFinite(Date.parse(r.issuedAt)) || Date.parse(r.issuedAt)>Date.now() ||
   clearance.recordDigest!==requireIntentDigest(r) || r.markerDigest!==requireIntentDigest(marker) || !audit ||
   audit.kind!=='olos-remote-audit/v1' || audit.caseId!==marker.attemptId || audit.markerDigest!==requireIntentDigest(marker) ||
   audit.conclusion!=='NO_UNRESOLVED_INTERFERENCE' || !audit.operator || !audit.reference || !audit.rpcAuditEvidence || !audit.consumptionImpactEvidence ||
   r.externalEvidenceDigest!==requireIntentDigest(audit) || marker.workDir!==join(base,'log-inspections',String(marker.attemptId)))return true;
  const source={intent:read(join(String(marker.workDir),'intent.json')),pending:read(join(String(marker.workDir),'pending.json')),result:read(join(String(marker.workDir),'result.json'))};
  return r.sourceCaseDigest!==requireIntentDigest(source);
 }catch{return true;}
}

interface AttemptPermit { validate: () => void; call: boolean; connection: boolean; socket: boolean; login: boolean; rpc: boolean; }
const permits = new WeakMap<object,AttemptPermit>();
/** Issued only AFTER the adapter fsyncs its exclusive intent. No permit for recovery RPCs. */
async function authorizeCorporateAttemptsFor(
  endpoint: PreparedAttemptsEndpoint,
  validate: () => void,
  durableIntent: () => Promise<{intentPath:string;pendingPath:string}>,
  purpose: 'acquisition' | 'log-inspection',
  expectedIntent?: Readonly<Record<string,unknown>>
): Promise<void> {
  let inspectionPermitIssued=false;
  const entry=prepared.get(endpoint);
  if (!entry || permits.has(endpoint) ||
    (purpose === 'acquisition'
      ? !['ReadDataCurrentDay','ReadDataPreviousDays'].includes(entry.call.procedure)
      : entry.call.procedure !== 'LogReadData'))
    throw new OlosError('REAL_EXECUTION_BLOCKED');
  attemptsPin(endpoint);validate();const paths=await durableIntent();
  const read=async(path:string)=>{
    if(!isAbsolute(path) || await realpath(path)!==path)throw new OlosError('REAL_EXECUTION_BLOCKED');
    const f=await open(path,constants.O_RDONLY|constants.O_NOFOLLOW);
    try{const stat=await f.stat();if(!stat.isFile() || stat.uid!==process.getuid?.() || stat.mode&0o077 || stat.size>32768)throw new OlosError('REAL_EXECUTION_BLOCKED');
      const value=JSON.parse(await f.readFile('utf8')) as Record<string,unknown>;await f.sync();
      const d=await open(dirname(path),constants.O_RDONLY|constants.O_DIRECTORY|constants.O_NOFOLLOW);try{await d.sync();}finally{await d.close();}return value;
    }finally{await f.close();}
  };
  const intent=await read(paths.intentPath),pending=await read(paths.pendingPath);
  if(purpose==='acquisition' && blockingCorporateInspection(dirname(paths.pendingPath)))throw new OlosError('REAL_EXECUTION_BLOCKED');
  if(typeof intent.attemptId!=='string' || !/^[a-f0-9-]{36}$/.test(intent.attemptId) || intent.procedure!==entry.call.procedure || intent.pinApprovalId!==entry.pin.approvalId ||
    typeof intent.configurationDigest!=='string' || !/^[a-f0-9]{64}$/.test(intent.configurationDigest) ||
    typeof intent.operator!=='string' || !intent.operator || typeof intent.approvalReference!=='string' || !intent.approvalReference ||
    pending.attemptId!==intent.attemptId || pending.configurationDigest!==intent.configurationDigest || pending.workDir!==dirname(paths.intentPath) ||
    paths.intentPath!==join(String(pending.workDir),purpose==='acquisition'?`attempt-${intent.attemptId}.json`:'intent.json'))throw new OlosError('REAL_EXECUTION_BLOCKED');
  if (purpose === 'log-inspection') {
    const date = entry.call.values.DateExport;
    if (!expectedIntent || canonical(intent)!==canonical(expectedIntent) ||
        canonical(pending)!==canonical({purpose:'log-inspection',attemptId:intent.attemptId,configurationDigest:intent.configurationDigest,workDir:dirname(paths.intentPath),intentDigest:requireIntentDigest(intent)}) ||
        !String(intent.operator).trim() || !String(intent.approvalReference).trim() ||
        typeof intent.issuedAt!=='string' || !Number.isFinite(Date.parse(intent.issuedAt)) || Date.parse(intent.issuedAt)>Date.now() ||
        typeof intent.expiresAt!=='string' || !Number.isFinite(Date.parse(intent.expiresAt)) ||
        Date.parse(intent.expiresAt)<=Date.parse(intent.issuedAt) || Date.parse(intent.expiresAt)-Date.parse(intent.issuedAt)>12*3600000 ||
        intent.kind !== 'olos-log-inspection/v1' || intent.scope!=='OLOS_LOGREADDATA_INSPECTION_ONLY' ||
        intent.tableName!=='AttemptsRawData' || intent.fingerprintSha256!==entry.pin.fingerprintSha256 ||
        typeof date!=='string' || !validDate(date) || intent.dateExport!==date)
      throw new OlosError('REAL_EXECUTION_BLOCKED');
    const original=validate;
    validate=()=>{
      original();
      const documents:[string,Record<string,unknown>][]=[[paths.intentPath,intent],[paths.pendingPath,pending]];
      if(inspectionPermitIssued)documents.push([join(dirname(paths.intentPath),'execution-permit.json'),{attemptId:intent.attemptId,intentDigest:requireIntentDigest(intent)}]);
      for(const [path,value] of documents){
        const dir=lstatSync(dirname(path));if(!dir.isDirectory() || dir.isSymbolicLink() || dir.uid!==process.getuid?.() || dir.mode&0o077 || realpathSync(dirname(path))!==dirname(path))throw new OlosError('REAL_EXECUTION_BLOCKED');
        const fd=openSync(path,constants.O_RDONLY|constants.O_NOFOLLOW);
        try{const st=fstatSync(fd);if(!st.isFile() || st.uid!==process.getuid?.() || (st.mode&0o777)!==0o600 || st.size>32768 || canonical(JSON.parse(readFileSync(fd,'utf8')))!==canonical(value))throw new OlosError('REAL_EXECUTION_BLOCKED');}finally{closeSync(fd);}
      }
      if(typeof intent.expiresAt!=='string' || !Number.isFinite(Date.parse(intent.expiresAt)) || Date.parse(intent.expiresAt)<=Date.now())throw new OlosError('REAL_EXECUTION_BLOCKED');
    };
  }
  if(purpose==='acquisition'){const original=validate;validate=()=>{original();if(blockingCorporateInspection(dirname(paths.pendingPath)))throw new OlosError('REAL_EXECUTION_BLOCKED');};}
  validate();attemptsPin(endpoint);
  if(purpose==='log-inspection'){
    // Durable one-use claim: even a fresh endpoint/process cannot reuse this intent.
    const claim=await open(join(dirname(paths.intentPath),'execution-permit.json'),constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|constants.O_NOFOLLOW,0o600);
    try{await claim.writeFile(JSON.stringify({attemptId:intent.attemptId,intentDigest:requireIntentDigest(intent)}));await claim.sync();}finally{await claim.close();}
    const dir=await open(dirname(paths.intentPath),constants.O_RDONLY|constants.O_DIRECTORY|constants.O_NOFOLLOW);try{await dir.sync();}finally{await dir.close();}
    inspectionPermitIssued=true;validate();attemptsPin(endpoint);
  }
  if(permits.has(endpoint))throw new OlosError('REAL_EXECUTION_BLOCKED');
  permits.set(endpoint,{validate,call:false,connection:false,socket:false,login:false,rpc:false});
}
export async function authorizeCorporateAttemptsOnce(
  endpoint: PreparedAttemptsEndpoint,
  validate: () => void,
  durableIntent: () => Promise<{intentPath:string;pendingPath:string}>
): Promise<void> {
  return authorizeCorporateAttemptsFor(endpoint, validate, durableIntent, 'acquisition');
}

export async function authorizeCorporateLogInspection(
  endpoint: PreparedAttemptsEndpoint,
  validate: () => void,
  durableIntent: () => Promise<{intentPath:string;pendingPath:string}>,
  expectedIntent: Readonly<Record<string,unknown>>
): Promise<void> {
  return authorizeCorporateAttemptsFor(endpoint, validate, durableIntent, 'log-inspection',expectedIntent);
}

export function claimAttemptsConnection(endpoint: PreparedAttemptsEndpoint): void {
  assertCorporateAttemptsExecution(endpoint);const p=permits.get(endpoint)!;
  if(!p.call || p.connection)throw new OlosError('REAL_EXECUTION_BLOCKED');p.connection=true;
}
export function claimAttemptsSocket(endpoint: PreparedAttemptsEndpoint): void {
  assertCorporateAttemptsExecution(endpoint);const p=permits.get(endpoint)!;
  if(!p.connection || p.socket)throw new OlosError('REAL_EXECUTION_BLOCKED');p.socket=true;
}
export function claimAttemptsLogin(endpoint: PreparedAttemptsEndpoint): void {
  assertCorporateAttemptsExecution(endpoint);const p=permits.get(endpoint)!;
  if(!p.socket || p.login)throw new OlosError('REAL_EXECUTION_BLOCKED');p.login=true;
}

/** Validate configuration/signature without inventing a date/range or creating a socket. */
export function validateCorporateAttemptsPreparation(config: unknown, pin: OlosPinApproval, signature: ProcedureSignature) {
  const endpoint = schema.safeParse(config);
  assertOlosPinApproval(pin);
  if (!endpoint.success || pin.purpose !== 'OLOS_ATTEMPTS_TDS_ACQUISITION' ||
    !['ReadDataCurrentDay','ReadDataPreviousDays','LogReadData','ReReadData'].includes(signature.procedure)) throw new OlosError('REAL_EXECUTION_BLOCKED');
  if (compareSignatures(verifiedProcedureContracts[signature.procedure].server.parameters,signature.parameters.map(p => ({ ...p,nullable: null }))).length) throw new OlosError('CONTRACT_UNCONFIRMED');
  return endpoint.data;
}
/** Preparation is NOT an execution permit. Issuance requires the durable one-shot intent above. */
export function prepareCorporateAttemptsEndpoint(config: unknown, pin: OlosPinApproval, signature: ProcedureSignature,
  values: PreparedCall['values']): PreparedAttemptsEndpoint {
  const endpoint = validateCorporateAttemptsPreparation(config,pin,signature);
  const call = prepareCall(signature,values,300000);
  if ((call.procedure === 'LogReadData' ? call.values.tablename : call.values.TableName) !== 'AttemptsRawData') throw new OlosError('CONTRACT_UNCONFIRMED');
  const result = Object.freeze({ ...endpoint,ca: pin.certificatePem,serverName: '10.196.234.11' as const });
  prepared.set(result,{ pin,call }); return result;
}
export const isLogInspectionEndpoint = (endpoint: object): boolean => prepared.get(endpoint)?.call.procedure==='LogReadData';
export const isPreparedAttemptsEndpoint = (endpoint: object): endpoint is PreparedAttemptsEndpoint => prepared.has(endpoint);
export function attemptsPin(endpoint: PreparedAttemptsEndpoint): OlosPinApproval {
  const value = prepared.get(endpoint); if (!value) throw new OlosError('REAL_EXECUTION_BLOCKED');
  assertOlosPinApproval(value.pin); return value.pin;
}
export function matchesPreparedAttemptsConnection(config: ConnectionConfiguration | Connection['config'], endpoint: PreparedAttemptsEndpoint): boolean {
  return prepared.has(endpoint) && config.server === endpoint.server && config.options?.port === endpoint.port &&
    config.options.database === endpoint.database && config.options.serverName === endpoint.serverName &&
    config.options.encrypt === true && config.options.trustServerCertificate === false && config.options.tdsVersion === '7_4' &&
    (!isLogInspectionEndpoint(endpoint) || config.options.useUTC===true) && config.options.maxRetriesOnTransientErrors === 0 && !config.options.connector && !config.options.multiSubnetFailover && !config.options.instanceName &&
    config.options.cryptoCredentialsDetails?.minVersion === 'TLSv1.2' && config.options.cryptoCredentialsDetails.maxVersion === 'TLSv1.2' &&
    Object.keys(config.options.cryptoCredentialsDetails).every(k => ['minVersion','maxVersion'].includes(k)) &&
    config.authentication?.type === 'default' && config.authentication.options.userName === 'OEC' && config.authentication.options.password === endpoint.password;
}
export function assertPreparedAttemptsCall(endpoint: PreparedAttemptsEndpoint, call: PreparedCall): void {
  const value = prepared.get(endpoint);
  if (!value || canonical(value.call) !== canonical(call)) throw new OlosError('PROCEDURE_BLOCKED');
  assertCorporateAttemptsExecution(endpoint);
  const permit=permits.get(endpoint)!;if(permit.call)throw new OlosError('REAL_EXECUTION_BLOCKED');permit.call=true;
}
export function assertCorporateAttemptsExecution(endpoint: PreparedAttemptsEndpoint): void {
  attemptsPin(endpoint);
  const permit=permits.get(endpoint);
  if(!permit)throw new Error('ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED');permit.validate();
}
export function assertPreparedAttemptsRpc(request: Request, endpoint: PreparedAttemptsEndpoint): void {
  assertCorporateAttemptsExecution(endpoint);
  const p=permits.get(endpoint)!,call=prepared.get(endpoint)!.call,params=request.parameters;
  if (!p.login || p.rpc ||
      request.sqlTextOrProcedure !== `dbo.usp_ExportData_${call.procedure}`)
    throw new OlosError('PROCEDURE_BLOCKED');

  if (call.procedure === 'LogReadData') {
    const date = params[1]?.value;

    if (params.length !== 4 ||
        params[0]?.name !== 'tablename' ||
        params[0].value !== 'AttemptsRawData' ||
        params[0].output ||
        params[0].type !== TYPES.NVarChar ||
        params[0].length !== 128 ||
        params[1]?.name !== 'DateExport' ||
        params[1].output ||
        params[1].type !== TYPES.Date ||
        !(date instanceof Date) ||
        !Number.isFinite(date.getTime()) || date.toISOString()!==`${call.values.DateExport}T00:00:00.000Z` ||
        params[2]?.name !== 'Message' || params[2].value!==undefined ||
        !params[2].output ||
        params[2].type !== TYPES.VarChar ||
        params[2].length !== 4000 ||
        params[3]?.name !== 'ErrorMessage' || params[3].value!==undefined ||
        !params[3].output ||
        params[3].type !== TYPES.VarChar ||
        params[3].length !== 4000)
      throw new OlosError('PROCEDURE_BLOCKED');

    p.rpc = true;
    return;
  }

  if (params.length !== 3 ||
    params[0]?.name!=='TableName' || params[0].value!=='AttemptsRawData' || params[0].output || params[0].type.name!=='VarChar' || params[0].length!==128 ||
    params[1]?.name!=='Message' || !params[1].output || params[1].type.name!=='VarChar' || params[1].length!==4000 ||
    params[2]?.name!=='ErrorMessage' || !params[2].output || params[2].type.name!=='VarChar' || params[2].length!==4000)throw new OlosError('PROCEDURE_BLOCKED');
  p.rpc=true;
}
