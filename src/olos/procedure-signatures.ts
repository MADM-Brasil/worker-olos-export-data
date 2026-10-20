import type { Procedure } from './contracts.js';
import { manualEvidence } from './documented-tables.js';
import { OlosError, type ParameterType, type ProcedureSignature } from './types.js';

export interface SignatureParameter {
  readonly name: string; // sem @, preservando o case informado
  readonly direction: 'input' | 'output';
  readonly type: ParameterType;
  readonly nullable: null; // metadado não fornecido; não confundir com política de aceitação runtime
}
const text = (name: string, kind: 'varchar' | 'nvarchar', length: number | 'max', output = false): SignatureParameter =>
  Object.freeze({ name, type: Object.freeze({ kind, length }), direction: output ? 'output' : 'input', nullable: null });
const scalar = (name: string, kind: 'date' | 'bigint'): SignatureParameter =>
  Object.freeze({ name, type: Object.freeze({ kind }), direction: 'input', nullable: null });
const outs = (length: number | 'max') => [text('Message','varchar',length,true), text('ErrorMessage','varchar',length,true)];
const normal = () => [text('TableName','varchar',128), ...outs(4000)];
const recovery = () => [text('TableName','varchar',128), scalar('DateExport','date')];
// Tipos abaixo são relatos de sys.parameters fornecidos pela empresa, não consultas feitas pelo worker.
const server: Record<Procedure, readonly SignatureParameter[]> = {
  ReadData: [text('TableName','varchar',128), ...outs('max')],
  ReadDataCurrentDay: normal(), ReadDataPreviousDays: normal(),
  LogReadData: [text('tablename','nvarchar',128), scalar('DateExport','date'), ...outs(4000)],
  ReReadData: [...recovery(), scalar('PKIDStart','bigint'), scalar('PKIDEnd','bigint'), ...outs(4000)],
  ResetReadData: [...recovery(), ...outs(4000)], DataAvailable: recovery(),
};
const manual: Record<Procedure, { readonly pages: readonly number[]; readonly parameters: readonly SignatureParameter[] | null; readonly notes: readonly string[] }> = {
  ReadData: { pages: [4,5], parameters: normal(), notes: [] },
  ReadDataCurrentDay: { pages: [5,6], parameters: normal(), notes: ['Error table references static tables despite transactional method.'] },
  ReadDataPreviousDays: { pages: [6,7,8], parameters: normal(), notes: [] },
  LogReadData: { pages: [10,11], parameters: [text('TableName','varchar',128), scalar('DateExport','date'), text('Message','varchar',4000,true), text('ErrorMessage','varchar','max',true)],
    notes: ['Page 10 parameter table: varchar(128), ErrorMessage varchar(max); returns/example: varchar(4000); page 11 example declares tablename nvarchar(128).', 'DateExport=null is documented as all available dates; this is not a SQL default.'] },
  ReReadData: { pages: [11,12], parameters: [...recovery(), scalar('PKIDStart','bigint'), scalar('PKIDEnd','bigint'), ...outs('max')],
    notes: ['Page 11 parameter table uses varchar(max) outputs; page 12 returns/example uses varchar(4000).'] },
  ResetReadData: { pages: [8,9], parameters: [...recovery(), ...outs(4000)], notes: ['Description says @DataExport; parameter table/example uses @DateExport.'] },
  DataAvailable: { pages: [], parameters: null, notes: ['Absent from the 2020 manual. Behavior unknown.'] },
};
export interface SignatureDifference { readonly parameter: string; readonly aspect: 'name_case' | 'type' | 'direction' | 'presence'; readonly documented: string; readonly reported: string; }
export function sqlDeclaration(t: ParameterType): string { return 'length' in t ? `${t.kind}(${t.length})` : t.kind; }
export function compareSignatures(documented: readonly SignatureParameter[], reported: readonly SignatureParameter[]): readonly SignatureDifference[] {
  const result: SignatureDifference[] = [];
  for (let i = 0; i < Math.max(documented.length,reported.length); i++) {
    const a = documented[i], b = reported[i];
    if (!a || !b || a.name.toLowerCase() !== b.name.toLowerCase()) {
      result.push({ parameter: b?.name ?? a!.name, aspect: 'presence', documented: a?.name ?? 'absent', reported: b?.name ?? 'absent' }); continue;
    }
    if (a.name !== b.name) result.push({ parameter: b.name, aspect: 'name_case', documented: a.name, reported: b.name });
    if (sqlDeclaration(a.type) !== sqlDeclaration(b.type)) result.push({ parameter: b.name, aspect: 'type', documented: sqlDeclaration(a.type), reported: sqlDeclaration(b.type) });
    if (a.direction !== b.direction) result.push({ parameter: b.name, aspect: 'direction', documented: a.direction, reported: b.direction });
  }
  return Object.freeze(result.map(d => Object.freeze(d)));
}
function entry(name: Procedure) {
  const documented = manual[name];
  return Object.freeze({ version: `${name}/v2`, procedure: name, manualEvidence,
    documented: Object.freeze({ ...documented, pages: Object.freeze([...documented.pages]), notes: Object.freeze([...documented.notes]),
      parameters: documented.parameters === null ? null : Object.freeze([...documented.parameters]) }),
    server: Object.freeze({ evidence: 'company:phase3b:sys.parameters', independentlyQueried: false,
      parameters: Object.freeze([...server[name]]) }),
    differences: documented.parameters === null ? null : compareSignatures(documented.parameters,server[name]),
    signatureReported: true, semanticsValidated: false, executionApproved: false, realExecutionAllowed: false });
}
export const verifiedProcedureContracts = Object.freeze({ ReadData: entry('ReadData'), ReadDataCurrentDay: entry('ReadDataCurrentDay'),
  ReadDataPreviousDays: entry('ReadDataPreviousDays'), LogReadData: entry('LogReadData'), ReReadData: entry('ReReadData'),
  ResetReadData: entry('ResetReadData'), DataAvailable: entry('DataAvailable') });
export const documentedReadSemantics = Object.freeze({
  normal: Object.freeze({ ReadData: Object.freeze({ tableKind: 'reference', maxRows: 'unbounded', pages: Object.freeze([4,5]) }),
    ReadDataCurrentDay: Object.freeze({ tableKind: 'transactional', maxRows: 200_000, pages: Object.freeze([5,6]) }),
    ReadDataPreviousDays: Object.freeze({ tableKind: 'transactional', maxRows: 200_000, pages: Object.freeze([6,7,8]) }) }),
  recovery: Object.freeze({ ReReadData: Object.freeze({ maxRows: 200_000, pages: Object.freeze([11,12]) }),
    recurringUseDocumented: false }),
  dailyPartitions: true, transactionalImmutableAccordingToManual: true, automaticPurgeDocumented: true,
  retentionDays: null, sourceControlExclusive: false, currentBehaviorValidated: false,
  callerCursorOrLimitParameter: false, dataAvailableBehavior: null,
});

/** Converte max_length (BYTES do catálogo) para unidades da declaração, sem dobrar nvarchar(128) para 256 caracteres. */
export function textTypeFromCatalog(kind: 'varchar' | 'nvarchar', maxLengthBytes: number): Extract<ParameterType, { kind: 'varchar' | 'nvarchar' }> {
  if (kind !== 'varchar' && kind !== 'nvarchar' || !Number.isInteger(maxLengthBytes)) throw new OlosError('INVALID_PARAMETER');
  if (maxLengthBytes === -1) return { kind, length: 'max' };
  if (maxLengthBytes < 1 || maxLengthBytes > 8000 || kind === 'nvarchar' && maxLengthBytes % 2 !== 0) throw new OlosError('INVALID_PARAMETER');
  return { kind, length: kind === 'nvarchar' ? maxLengthBytes / 2 : maxLengthBytes };
}
/** Nenhum nullable físico é inferido. Cada parâmetro precisa de uma política explícita para testes/composição futura. */
export function reportedSignature(name: Procedure, policy: { readonly evidence: string; readonly nullable: Readonly<Record<string, boolean>> }): ProcedureSignature {
  if (name === 'DataAvailable') throw new OlosError('PROCEDURE_BLOCKED');
  const c = verifiedProcedureContracts[name];
  if (!c || !policy.evidence || Object.keys(policy.nullable).length !== c.server.parameters.length ||
    c.server.parameters.some(p => !Object.hasOwn(policy.nullable,p.name) || typeof policy.nullable[p.name] !== 'boolean')) throw new OlosError('CONTRACT_UNCONFIRMED');
  return { procedure: name, version: c.version, evidence: `${c.server.evidence};policy:${policy.evidence}`,
    parameters: c.server.parameters.map(p => ({ ...p, type: { ...p.type }, nullable: policy.nullable[p.name]! })),
    messageOutput: 'Message', errorOutput: 'ErrorMessage' };
}
