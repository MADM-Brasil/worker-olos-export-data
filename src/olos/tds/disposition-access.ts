import { z } from 'zod';
import type { ConnectionConfiguration, Request } from 'tedious';
import { OlosError } from '../types.js';
import { isExceptionalDispositionEndpoint, assertExceptionalCall, assertExceptionalRpc } from './disposition-exception.js';

export const sourceConfigSchema = z.object({ server: z.literal('10.196.234.11'), port: z.literal(1433), database: z.literal('ExportDataV3'),
  user: z.literal('OEC'), password: z.string().min(1), ca: z.string().includes('BEGIN CERTIFICATE'),
  serverName: z.string().regex(/^[A-Za-z0-9][A-Za-z0-9.-]{0,252}$/) }).strict();
export type DispositionEndpoint = z.infer<typeof sourceConfigSchema>;
export const approvalSchema = z.object({ action: z.literal("ReadData('Disposition')"), testId: z.string().uuid(),
  reference: z.string().regex(/^[A-Za-z0-9._:/-]{1,120}$/), expiresAt: z.string().datetime(), snapshotDate: z.string().regex(/^\d{4}-\d{2}-\d{2}$/) }).strict();
export type DispositionApproval = z.infer<typeof approvalSchema>;
const permits = new WeakMap<object,{ approval: DispositionApproval; used: boolean; rpcUsed: boolean }>();
export function approveDispositionEndpoint(config: unknown, input: unknown, testId: string): DispositionEndpoint {
  const c = sourceConfigSchema.safeParse(config), a = approvalSchema.safeParse(input);
  if (!c.success || !a.success || a.data.testId !== testId || Date.parse(a.data.expiresAt) <= Date.now() ||
      Date.parse(a.data.expiresAt) > Date.now()+12*60*60*1000) throw new OlosError('REAL_EXECUTION_BLOCKED');
  const endpoint = Object.freeze(c.data); permits.set(endpoint,{ approval: Object.freeze(a.data), used: false,rpcUsed: false }); return endpoint;
}
export const isDispositionEndpoint = (endpoint: object): boolean => permits.has(endpoint) || isExceptionalDispositionEndpoint(endpoint);
export function assertDispositionCall(endpoint: object, procedure: string, values: Readonly<Record<string,unknown>>): void {
  if (isExceptionalDispositionEndpoint(endpoint)) return assertExceptionalCall(endpoint,procedure,values);
  const permit = permits.get(endpoint);
  if (!permit || permit.used || Date.parse(permit.approval.expiresAt) <= Date.now() || procedure !== 'ReadData' ||
    Object.keys(values).length !== 1 || values.TableName !== 'Disposition') throw new OlosError('REAL_EXECUTION_BLOCKED');
  permit.used = true; // uma tentativa, inclusive falha de conexão; nunca retry remoto
}
export function matchesDispositionConnection(config: ConnectionConfiguration, endpoint?: DispositionEndpoint): boolean {
  return !!endpoint && permits.has(endpoint) && config.server === endpoint.server && config.options?.port === endpoint.port &&
    config.options.database === endpoint.database && config.options.serverName === endpoint.serverName &&
    config.options.cryptoCredentialsDetails?.ca === endpoint.ca && config.options.encrypt === true && config.options.trustServerCertificate === false &&
    config.options.cryptoCredentialsDetails.minVersion === 'TLSv1.2' && config.options.maxRetriesOnTransientErrors === 0 && config.authentication?.type === 'default' &&
    config.authentication.options.userName === endpoint.user && config.authentication.options.password === endpoint.password;
}
export function assertDispositionRpc(request: Request, endpoint: DispositionEndpoint): void {
  if (isExceptionalDispositionEndpoint(endpoint)) return assertExceptionalRpc(request,endpoint);
  const permit = permits.get(endpoint);
  if (!permit?.used || permit.rpcUsed || Date.parse(permit.approval.expiresAt) <= Date.now()) throw new OlosError('REAL_EXECUTION_BLOCKED');
  const p = request.parameters;
  if (request.sqlTextOrProcedure !== 'dbo.usp_ExportData_ReadData' || p.length !== 3 ||
    p[0]?.name !== 'TableName' || p[0].value !== 'Disposition' || p[0].output || p[0].type.name !== 'VarChar' ||
    p[1]?.name !== 'Message' || !p[1].output || p[1].type.name !== 'VarChar' ||
    p[2]?.name !== 'ErrorMessage' || !p[2].output || p[2].type.name !== 'VarChar') throw new OlosError('PROCEDURE_BLOCKED');
  permit.rpcUsed = true;
}
