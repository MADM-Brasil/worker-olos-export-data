import { documentedTableContracts } from '../../src/olos/documented-tables.js';
import { reportedSignature, verifiedProcedureContracts } from '../../src/olos/procedure-signatures.js';
import type { ValidatedTableContract } from '../../src/olos/adapter.js';
import type { ReadPlan } from '../../src/olos/extraction.js';
import type { Column, ProcedureSignature } from '../../src/olos/types.js';
import type { Procedure } from '../../src/olos/contracts.js';
import type { ExportTable } from '../../src/sync/contracts.js';
import type { FieldDefinition } from '../../src/persistence/types.js';

// Esta política vale SOMENTE para a fixture: não é nulabilidade observada no servidor.
export function fixtureSignature(name: Procedure): ProcedureSignature {
  return reportedSignature(name, { evidence: 'SYNTHETIC TEST ONLY: input required; outputs nullable', nullable:
    Object.fromEntries(verifiedProcedureContracts[name].server.parameters.map(p => [p.name,p.direction === 'output'])) });
}
export function documentedFixture(table: ExportTable): ValidatedTableContract {
  const c = documentedTableContracts[table];
  const columns: Column[] = c.fields.map(field => {
    const text = /^(varchar|nvarchar|char)\((\d+|max)\)$/.exec(field.sqlType);
    if (text) return { name: field.name, type: text[1] as 'varchar' | 'nvarchar' | 'char', length: text[2] === 'max' ? 'max' : Number(text[2]) };
    return { name: field.name, type: field.sqlType as 'int' | 'datetime' };
  });
  return { source: { source_table: table, version: `${c.version}:synthetic-only`, approved: true,
    fields: Object.fromEntries(columns.map(column => [column.name, { type: column.type === 'int' ? 'int64' : column.type === 'datetime' ? 'local_timestamp' : 'text',
      nullable: column.name !== c.documentedKey } satisfies FieldDefinition])),
    key_fields: [c.documentedKey], layout_evidence: 'SYNTHETIC ONLY: names/types from manual; null policy defined by fixture',
    key_evidence: 'SYNTHETIC ONLY: keys generated uniquely in this fixture' }, columns,
    acquisitionEvidence: 'SYNTHETIC ONLY: whole seconds are exact datetime ticks; strings generated before driver' };
}
export function documentedRecord(table: ExportTable, key = '2147483647'): Record<string,string | null> {
  const c = documentedTableContracts[table];
  return Object.fromEntries(c.fields.map(field => [field.name, field.name === c.documentedKey ? key : field.sqlType === 'int' ? '0' :
    field.sqlType === 'datetime' ? '2020-06-03T12:00:00.000' : field.sqlType === 'char(16)' ? '000000000000001 ' :
      field.sqlType.startsWith('nvarchar') ? 'Agente Á' : 'X']));
}
export function documentedPlan(table: ExportTable): ReadPlan {
  const c = documentedTableContracts[table];
  return { run: { table, contractVersion: `${c.version}:synthetic-only`, readType: c.kind === 'reference' ? 'reference_snapshot' : 'current_day',
    exportDate: '2020-06-03', pkRange: null }, signature: fixtureSignature(c.kind === 'reference' ? 'ReadData' : 'ReadDataCurrentDay'),
    tableContract: documentedFixture(table), parameters: () => ({ TableName: table }), responseEvidence: 'SYNTHETIC dataset; return=0 and empty ErrorMessage',
    readTypeEvidence: 'SYNTHETIC labels; documented static/transactional method association', successfulReturnValues: [0],
    datasetIndex: 0, timeoutMs: 5000, repetition: null };
}
