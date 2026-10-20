import { z } from 'zod';
import { resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import type { ConnectionConfiguration, Request } from 'tedious';
import type { DispositionEndpoint } from './disposition-access.js';
import type { IsolatedEndpoint } from './source.js';
import { OlosError } from '../types.js';

export const exceptionalTlsMode = 'exceptional-unverified-test' as const;
export const exceptionalTlsRisk = 'I_ACCEPT_ENCRYPTION_WITHOUT_SERVER_IDENTITY_VALIDATION' as const;
export const exceptionalSourceSchema = z.object({ server: z.literal('10.196.234.11'),port: z.literal(1433),
  database: z.literal('ExportDataV3'),user: z.literal('OEC'),password: z.string().min(1),
  ca: z.string().optional(),serverName: z.string().regex(/^[A-Za-z0-9][A-Za-z0-9.-]{0,252}$/) }).strict();
export const exceptionalApprovalSchema = z.object({ action: z.literal("ReadData('Disposition')"),testId: z.string().uuid(),
  reference: z.string().regex(/^[A-Za-z0-9._:/-]{1,120}$/),operator: z.string().regex(/^[A-Za-z0-9._@/-]{1,120}$/),
  issuedAt: z.string().datetime(),expiresAt: z.string().datetime(),snapshotDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/),
  tlsMode: z.literal(exceptionalTlsMode),riskConfirmation: z.literal(exceptionalTlsRisk),
  configurationDigest: z.string().regex(/^[a-f0-9]{64}$/) }).strict();
export type ExceptionalApproval = z.infer<typeof exceptionalApprovalSchema>;
export function validateExceptionalApproval(input: unknown, testId: string): ExceptionalApproval {
  const a = exceptionalApprovalSchema.safeParse(input), now = Date.now();
  if (!a.success || a.data.testId !== testId || Date.parse(a.data.issuedAt) > now ||
    Date.parse(a.data.expiresAt) <= now || Date.parse(a.data.expiresAt) - Date.parse(a.data.issuedAt) > 15*60*1000 ||
    Date.parse(a.data.expiresAt) <= Date.parse(a.data.issuedAt)) throw new OlosError('REAL_EXECUTION_BLOCKED');
  return Object.freeze(a.data);
}
export interface ExceptionalClaim {
  readonly testId: string; readonly configurationDigest: string; readonly reference: string;
  readonly action: "ReadData('Disposition')"; readonly tlsMode: typeof exceptionalTlsMode;
}
type Permit = { approval: ExceptionalApproval; callUsed: boolean; connectionUsed: boolean; rpcUsed: boolean;
  endpoint: Readonly<{ server: string; port: number; database: string; user: string; password: string; ca: string; serverName: string }> };
const permits = new WeakMap<object,Permit>();
const cliEntry = fileURLToPath(new URL('../../../adapters/disposition-integration.ts',import.meta.url));
export function isExceptionalDispositionEndpoint(endpoint: object): boolean { return permits.has(endpoint); }
/** CLI capability after the durable claim and isolated-destination checks. No environment toggle issues it. */
export function authorizeExceptionalDispositionEndpoint(config: unknown, input: unknown, claim: ExceptionalClaim): DispositionEndpoint {
  if (!process.argv[1] || resolve(process.argv[1]) !== cliEntry || process.env.SYNC_ENABLED !== 'false' ||
    process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new OlosError('REAL_EXECUTION_BLOCKED');
  const parsed = exceptionalSourceSchema.safeParse(config);
  if (!parsed.success) throw new OlosError('REAL_EXECUTION_BLOCKED');
  return issue(parsed.data,input,claim) as DispositionEndpoint;
}
/** Fixture-only capability: can never authorize a corporate host/database/user. No CLI flag exposes this path. */
export function authorizeIsolatedExceptionalDispositionEndpoint(config: IsolatedEndpoint, input: unknown, claim: ExceptionalClaim): IsolatedEndpoint {
  if (config.server !== '127.0.0.1' || config.database !== 'phase3d_fixture' || !Number.isInteger(config.port) || config.port < 1024 || config.port > 65535 ||
    config.user !== 'sa' || !config.password || process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new OlosError('REAL_EXECUTION_BLOCKED');
  return issue({ ...config,serverName: 'localhost' },input,claim) as IsolatedEndpoint;
}
function issue(config: { server: string; port: number; database: string; user: string; password: string; ca?: string | undefined; serverName: string },
  input: unknown, claim: ExceptionalClaim): object {
  const approval = validateExceptionalApproval(input,claim.testId);
  if (claim.action !== approval.action || claim.reference !== approval.reference || claim.tlsMode !== exceptionalTlsMode ||
    claim.configurationDigest !== approval.configurationDigest) throw new OlosError('REAL_EXECUTION_BLOCKED');
  const endpoint = Object.freeze({ ...config,ca: config.ca ?? '' });
  permits.set(endpoint,{ approval,endpoint,callUsed: false,connectionUsed: false,rpcUsed: false }); return endpoint;
}
function live(endpoint: object): Permit {
  const permit = permits.get(endpoint);
  if (!permit || Date.parse(permit.approval.expiresAt) <= Date.now() || process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new OlosError('REAL_EXECUTION_BLOCKED');
  return permit;
}
export function assertExceptionalCall(endpoint: object, procedure: string, values: Readonly<Record<string,unknown>>): void {
  const p = live(endpoint);
  if (p.callUsed || procedure !== 'ReadData' || Object.keys(values).length !== 1 || values.TableName !== 'Disposition') throw new OlosError('REAL_EXECUTION_BLOCKED');
  p.callUsed = true;
}
export function claimExceptionalConnection(config: ConnectionConfiguration, endpoint: object): void {
  const p = live(endpoint), e = p.endpoint, options = config.options;
  if (!p.callUsed || p.connectionUsed || config.server !== e.server || options?.port !== e.port || options.database !== e.database ||
    config.authentication?.type !== 'default' || config.authentication.options.userName !== e.user || config.authentication.options.password !== e.password ||
    options.encrypt !== true || options.trustServerCertificate !== true || options.cryptoCredentialsDetails?.minVersion !== 'TLSv1.2' ||
    options.cryptoCredentialsDetails.ca !== e.ca || options.serverName !== e.serverName || options.maxRetriesOnTransientErrors !== 0 ||
    options.tdsVersion !== '7_4' || options.multiSubnetFailover === true || options.connector !== undefined || options.instanceName !== undefined ||
    options.cryptoCredentialsDetails.maxVersion !== undefined || options.cryptoCredentialsDetails.ciphers !== undefined ||
    options.cryptoCredentialsDetails.secureOptions !== undefined) throw new OlosError('REAL_EXECUTION_BLOCKED');
  p.connectionUsed = true;
}
export function assertExceptionalRpc(request: Request, endpoint: object): void {
  const p = live(endpoint), parameters = request.parameters;
  if (!p.callUsed || !p.connectionUsed || p.rpcUsed || request.sqlTextOrProcedure !== 'dbo.usp_ExportData_ReadData' || parameters.length !== 3 ||
    parameters[0]?.name !== 'TableName' || parameters[0].value !== 'Disposition' || parameters[0].output || parameters[0].type.name !== 'VarChar' ||
    parameters[1]?.name !== 'Message' || !parameters[1].output || parameters[1].type.name !== 'VarChar' ||
    parameters[2]?.name !== 'ErrorMessage' || !parameters[2].output || parameters[2].type.name !== 'VarChar') throw new OlosError('PROCEDURE_BLOCKED');
  p.rpcUsed = true;
}
