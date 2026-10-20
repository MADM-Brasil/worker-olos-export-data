import {open,realpath} from 'node:fs/promises';
import {constants} from 'node:fs';
import {dirname,join,isAbsolute} from 'node:path';
import { z } from 'zod';
import type { ConnectionConfiguration, Connection, Request } from 'tedious';
import { assertOlosPinApproval, type OlosPinApproval } from '../../security/olos-certificate-pin.js';
import { prepareCall } from '../client.js';
import { compareSignatures, verifiedProcedureContracts } from '../procedure-signatures.js';
import type { PreparedCall, ProcedureSignature } from '../types.js';
import { OlosError } from '../types.js';
import { canonical } from '../../persistence/validation.js';

const schema = z.object({ server: z.literal('10.196.234.11'),port: z.literal(1433),database: z.literal('ExportDataV3'),
  user: z.literal('OEC'),password: z.string().min(1) }).strict();
export type PreparedAttemptsEndpoint = Readonly<z.infer<typeof schema> & { ca: string; serverName: '10.196.234.11' }>;
const prepared = new WeakMap<object,{ pin: OlosPinApproval; call: PreparedCall }>();
interface AttemptPermit { validate: () => void; call: boolean; connection: boolean; socket: boolean; login: boolean; rpc: boolean; }
const permits = new WeakMap<object,AttemptPermit>();
/** Issued only AFTER the adapter fsyncs its exclusive intent. No permit for recovery RPCs. */
export async function authorizeCorporateAttemptsOnce(endpoint: PreparedAttemptsEndpoint, validate: () => void, durableIntent: () => Promise<{intentPath:string;pendingPath:string}>): Promise<void> {
  const entry=prepared.get(endpoint);
  if (!entry || permits.has(endpoint) || !['ReadDataCurrentDay','ReadDataPreviousDays'].includes(entry.call.procedure)) throw new OlosError('REAL_EXECUTION_BLOCKED');
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
  if(typeof intent.attemptId!=='string' || !/^[a-f0-9-]{36}$/.test(intent.attemptId) || intent.procedure!==entry.call.procedure || intent.pinApprovalId!==entry.pin.approvalId ||
    typeof intent.configurationDigest!=='string' || !/^[a-f0-9]{64}$/.test(intent.configurationDigest) ||
    typeof intent.operator!=='string' || !intent.operator || typeof intent.approvalReference!=='string' || !intent.approvalReference ||
    pending.attemptId!==intent.attemptId || pending.configurationDigest!==intent.configurationDigest || pending.workDir!==dirname(paths.intentPath) ||
    paths.intentPath!==join(String(pending.workDir),`attempt-${intent.attemptId}.json`))throw new OlosError('REAL_EXECUTION_BLOCKED');
  validate();attemptsPin(endpoint);
  if(permits.has(endpoint))throw new OlosError('REAL_EXECUTION_BLOCKED');
  permits.set(endpoint,{validate,call:false,connection:false,socket:false,login:false,rpc:false});
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
export const isPreparedAttemptsEndpoint = (endpoint: object): endpoint is PreparedAttemptsEndpoint => prepared.has(endpoint);
export function attemptsPin(endpoint: PreparedAttemptsEndpoint): OlosPinApproval {
  const value = prepared.get(endpoint); if (!value) throw new OlosError('REAL_EXECUTION_BLOCKED');
  assertOlosPinApproval(value.pin); return value.pin;
}
export function matchesPreparedAttemptsConnection(config: ConnectionConfiguration | Connection['config'], endpoint: PreparedAttemptsEndpoint): boolean {
  return prepared.has(endpoint) && config.server === endpoint.server && config.options?.port === endpoint.port &&
    config.options.database === endpoint.database && config.options.serverName === endpoint.serverName &&
    config.options.encrypt === true && config.options.trustServerCertificate === false && config.options.tdsVersion === '7_4' &&
    config.options.maxRetriesOnTransientErrors === 0 && !config.options.connector && !config.options.multiSubnetFailover && !config.options.instanceName &&
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
  if(!p.login || p.rpc || request.sqlTextOrProcedure!==`dbo.usp_ExportData_${call.procedure}` || params.length!==3 ||
    params[0]?.name!=='TableName' || params[0].value!=='AttemptsRawData' || params[0].output || params[0].type.name!=='VarChar' || params[0].length!==128 ||
    params[1]?.name!=='Message' || !params[1].output || params[1].type.name!=='VarChar' || params[1].length!==4000 ||
    params[2]?.name!=='ErrorMessage' || !params[2].output || params[2].type.name!=='VarChar' || params[2].length!==4000)throw new OlosError('PROCEDURE_BLOCKED');
  p.rpc=true;
}
