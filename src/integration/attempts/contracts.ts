import { z } from 'zod';
import { documentedAttempts } from '../../olos/documented-tables.js';
import { reportedSignature, verifiedProcedureContracts } from '../../olos/procedure-signatures.js';
import type { Procedure } from '../../olos/contracts.js';
import type { TdsField } from '../../olos/tds/raw-parser.js';
import type { LargePlan } from '../../large-batch/types.js';
import type { SourceContract } from '../../persistence/types.js';
import { validDate } from '../../persistence/validation.js';

export const attemptsBlockers = Object.freeze([
  'CURRENT_ATTEMPTS_LAYOUT_NULLABILITY_COLLATION_UNVERIFIED',
  'PARTITION_DATE_TIMEZONE_AND_PK_SCOPE_UNVERIFIED',
  'LOG_LAYOUT_AND_SUCCESS_OUTPUT_POLICY_UNVERIFIED',
  'LOG_OWNERSHIP_OTHER_CONSUMERS_UNCOORDINATED',
  'REREAD_RANGE_INCLUSIVITY_RETENTION_AND_COMPLETENESS_UNVERIFIED',
  'CORPORATE_TLS_CHAIN_AND_NAME_NOT_VALIDATED',
  'RECOVERY_AFTER_EXPIRED_LEASE_OR_HARD_CRASH_REQUIRES_REVIEW',
]);
/** Operational preconditions, not booleans that authorize consumption. No remote calls. */
export function firstAttemptsAcquisitionRequirements(requestedDate: string) {
  if (!validDate(requestedDate)) throw new Error('ATTEMPTS_DATE_REJECTED');
  return {
    requestedDate,procedure: 'ReadDataCurrentDay',parameters: { TableName: 'AttemptsRawData' },
    destination: { host: '127.0.0.1',port: 5434,database: 'olos_worker' },
    executionAllowed: false,remoteDateParameter: false,wholeDayGuaranteed: false,
    maxRowsPerResponse: 200000,
    missing: [
      { code: 'SOURCE_CURRENT_DAY_NOT_ATTESTED',information: 'Data civil e fuso atuais do SQL Server e vínculo da partição CurrentDay com a data solicitada; a procedure não recebe DateExport.' },
      { code: 'CORPORATE_TLS_NOT_ATTESTED',information: 'PEM/fingerprint aprovados independentemente para aquisição e handshake autenticado comprovado; observação anterior não é aprovação.' },
      { code: 'CURRENT_ATTEMPTS_CONTRACT_NOT_ACCEPTED',information: 'Ordem, tipos, nulabilidade, collation e política de sucesso/outputs do dataset corporativo atual; não usar o contrato de fixture.' },
      { code: 'CONSUMED_RANGE_RECOVERY_NOT_PROVEN',information: 'Como atribuir uma faixa ao consumo deste worker após queda antes do selo: coordenação com outros consumidores, LogReadData, retenção/inclusividade/completude de ReReadData e recuperação após hard crash/lease expirada.' },
      { code: 'CORPORATE_ONE_SHOT_PERMIT_NOT_ISSUED',information: 'Permit de tentativa única emitido por worker:attempts após configuração privada aprovada e intenção durável; não há liberação por flag genérica.' },
    ],
  } as const;
}
export class FirstAttemptsAcquisitionBlocked extends Error {
  readonly report: ReturnType<typeof firstAttemptsAcquisitionRequirements>;
  constructor(requestedDate: string) {
    super('ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED');
    this.report = firstAttemptsAcquisitionRequirements(requestedDate);
  }
}
export function requireAttemptsFixture(mode: string): void {
  if (mode !== 'isolated-fixture') throw new Error('ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED');
}
export const approvalSchema = z.object({ testId: z.string().uuid(),operation: z.enum(['read','log','reread']),
  operator: z.string().regex(/^[A-Za-z0-9._/-]{1,100}$/),reference: z.string().regex(/^[A-Za-z0-9._:/-]{1,120}$/),
  issuedAt: z.string().datetime(),expiresAt: z.string().datetime(),configurationDigest: z.string().regex(/^[a-f0-9]{64}$/) }).strict();
export type AttemptsApproval = z.infer<typeof approvalSchema>;
export function validateApproval(input: unknown, testId: string, operation: AttemptsApproval['operation'], configurationDigest: string): AttemptsApproval {
  const a = approvalSchema.parse(input), now = Date.now();
  if (a.testId !== testId || a.operation !== operation || a.configurationDigest !== configurationDigest ||
    Date.parse(a.issuedAt) > now || Date.parse(a.expiresAt) <= now || Date.parse(a.expiresAt)-Date.parse(a.issuedAt) > 15*60000 ||
    Date.parse(a.expiresAt) <= Date.parse(a.issuedAt)) throw new Error('ATTEMPTS_APPROVAL_REJECTED');
  return Object.freeze(a);
}
export function signature(name: Procedure) {
  return reportedSignature(name,{ evidence: 'ISOLATED FIXTURE ONLY: required inputs, nullable outputs, return=0 and null outputs',
    nullable: Object.fromEntries(verifiedProcedureContracts[name].server.parameters.map(p => [p.name,p.direction === 'output'])) });
}
/** Full manual layout, with fixture-specific nullability/collation; NEVER an approved Olos contract. */
export function attemptsFixtureFields(collationHex: string): readonly TdsField[] {
  if (!/^[a-f0-9]{10}$/.test(collationHex)) throw new Error('ATTEMPTS_LAYOUT_REJECTED');
  return documentedAttempts.fields.map(f => {
    if (f.sqlType === 'int' || f.sqlType === 'datetime') return { name: f.name,type: f.sqlType,nullable: f.name !== 'AttemptsRawDataID' };
    const match = /^varchar\((max|\d+)\)$/.exec(f.sqlType);
    if (!match) throw new Error('ATTEMPTS_LAYOUT_REJECTED');
    return { name: f.name,type: 'varchar',nullable: true,sqlLength: match[1] === 'max' ? 'max' : Number(match[1]),
      maxBytes: match[1] === 'max' ? 65536 : Number(match[1]),encoding: 'windows-1252',collationHex };
  });
}
export function attemptsFixtureContract(): SourceContract {
  return { source_table: 'AttemptsRawData',version: 'controlled-attempts-fixture/v1',approved: true,
    fields: Object.fromEntries(documentedAttempts.fields.map(f => [f.name,{ type: f.sqlType === 'int' ? 'int64' : f.sqlType === 'datetime' ? 'sql_datetime_ticks' : 'text',nullable: f.name !== 'AttemptsRawDataID' }])),
    key_fields: ['AttemptsRawDataID'],layout_evidence: 'ISOLATED FIXTURE ONLY: manual 2020 fields; generated metadata/nullability/collation',
    key_evidence: 'ISOLATED FIXTURE ONLY: unique IDs within this partition; NOT CallId or CallIdTelecom' };
}
export function attemptsFixturePlan(date: string, collationHex: string): LargePlan {
  if (!validDate(date)) throw new Error('ATTEMPTS_DATE_REJECTED');
  const contract = attemptsFixtureContract();
  return { fields: attemptsFixtureFields(collationHex).map(({ sqlLength: _,collationHex: _c,...f }) => { if (f.type === 'date') throw new Error('ATTEMPTS_LAYOUT_REJECTED'); return { ...f,type: f.type }; }),contract,
    run: { table: 'AttemptsRawData',contractVersion: contract.version,readType: 'current_day',exportDate: date,pkRange: null },
    signature: signature('ReadDataCurrentDay'),parameters: { TableName: 'AttemptsRawData' },successReturnValues: [0],
    acquisitionEvidence: 'existing exact TDS parser; ISOLATED FIXTURE',responseEvidence: 'ISOLATED FIXTURE: 1 dataset, return=0, outputs null',
    partitionEvidence: 'ISOLATED FIXTURE: explicit export date; never inferred from StartDate; no remote date/page parameter' };
}
export function logFixtureFields(collationHex: string): readonly TdsField[] {
  if (!/^[a-f0-9]{10}$/.test(collationHex)) throw new Error('ATTEMPTS_LAYOUT_REJECTED');
  return [{ name: 'TableName',type: 'varchar',sqlLength: 128,maxBytes: 128,encoding: 'windows-1252',collationHex,nullable: false },
    { name: 'PKColumnName',type: 'varchar',sqlLength: 128,maxBytes: 128,encoding: 'windows-1252',collationHex,nullable: false },
    { name: 'DateExport',type: 'date',nullable: false },{ name: 'PKIDStart',type: 'bigint',nullable: false },
    { name: 'PKIDEnd',type: 'bigint',nullable: false },{ name: 'DateRead',type: 'datetime',nullable: false },{ name: 'Quantity',type: 'int',nullable: false }];
}
