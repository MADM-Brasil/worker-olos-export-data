import { vi } from 'vitest';
import { tableContracts, type Procedure } from '../../src/olos/contracts.js';
import type { ValidatedTableContract } from '../../src/olos/adapter.js';
import type { ReadPlan } from '../../src/olos/extraction.js';
import type { ProcedureResponse, ProcedureSignature, ProcedureTransport, Dataset } from '../../src/olos/types.js';
import type { ExportTable } from '../../src/sync/contracts.js';

// Assinaturas, campos, tipos e semântica EXCLUSIVAMENTE sintéticos. Não são o contrato ExportDataV3.
export function signature(procedure: Procedure = 'ReadData'): ProcedureSignature {
  return { procedure, version: 'synthetic/v1', evidence: 'SYNTHETIC TEST ONLY', parameters: [
    { name: 'FixtureTable', direction: 'input', type: { kind: 'nvarchar', length: 128 }, nullable: false },
    { name: 'FixtureAfter', direction: 'input', type: { kind: 'bigint' }, nullable: true },
    { name: 'Message', direction: 'output', type: { kind: 'nvarchar', length: 1000 }, nullable: true },
    { name: 'ErrorMessage', direction: 'output', type: { kind: 'nvarchar', length: 1000 }, nullable: true },
  ], messageOutput: 'Message', errorOutput: 'ErrorMessage' };
}
export function tableContract(table: ExportTable = 'AttemptsRawData'): ValidatedTableContract {
  const key = tableContracts[table].documentedKey;
  return { source: { source_table: table, version: 'synthetic/v3a', approved: true,
    fields: { [key]: { type: 'int64', nullable: false }, FixtureText: { type: 'text', nullable: true },
      FixtureAmount: { type: 'decimal', nullable: true }, FixtureLocal: { type: 'local_timestamp', nullable: true },
      FixtureOffset: { type: 'offset_timestamp', nullable: true } }, key_fields: [key],
    layout_evidence: 'SYNTHETIC TEST ONLY', key_evidence: 'unique in fixture only' },
    acquisitionEvidence: 'synthetic exact text before JS conversion', columns: [
      { name: key, type: 'bigint' }, { name: 'FixtureText', type: 'nvarchar' },
      { name: 'FixtureAmount', type: 'decimal', precision: 38, scale: 7 },
      { name: 'FixtureLocal', type: 'datetime2', scale: 7 }, { name: 'FixtureOffset', type: 'datetimeoffset', scale: 7 },
    ] };
}
export function record(id = '9007199254740993', table: ExportTable = 'AttemptsRawData') {
  return { [tableContracts[table].documentedKey]: id, FixtureText: null as string | null,
    FixtureAmount: '12345678901234567890.1234000', FixtureLocal: '2026-10-15T09:30:00.1234567',
    FixtureOffset: '2026-10-15T09:30:00.1234567-03:00' };
}
export function dataset(rows: Dataset['rows'] = [record()], table: ExportTable = 'AttemptsRawData'): Dataset {
  return { columns: tableContract(table).columns, rows, representation: 'exact-text/v1', exactEncodingEvidence: 'SYNTHETIC TEST ONLY' };
}
export function response(data: Dataset = dataset()): ProcedureResponse {
  return { datasets: [data], output: { Message: 'synthetic message', ErrorMessage: null }, returnValue: 0 };
}
export function transport(value: ProcedureResponse = response()) {
  return { kind: 'synthetic' as const, connect: vi.fn(async () => {}), invoke: vi.fn<ProcedureTransport['invoke']>(async () => value), close: vi.fn(async () => {}) };
}
export function readPlan(table: ExportTable = 'AttemptsRawData'): ReadPlan {
  return { run: { table, contractVersion: 'synthetic/v3a', readType: 'investigation', exportDate: '2026-10-15', pkRange: null },
    tableContract: tableContract(table), signature: signature(), parameters: () => ({ FixtureTable: table, FixtureAfter: null }),
    responseEvidence: 'SYNTHETIC single dataset, empty ErrorMessage and returnValue=0',
    readTypeEvidence: 'SYNTHETIC investigation label only', successfulReturnValues: [0],
    datasetIndex: 0, timeoutMs: 5000, repetition: null };
}
