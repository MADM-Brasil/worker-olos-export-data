import { randomUUID } from 'node:crypto';
import { documentedTableContracts } from '../../src/olos/documented-tables.js';
import type { SourceContract, FieldDefinition } from '../../src/persistence/types.js';
import type { LargePlan, WireField, RawSource, SpoolIdentity } from '../../src/large-batch/types.js';
import { fixtureSignature } from './olos-documented-fixture.js';

// Nulabilidade/identidade/encoding aprovados SOMENTE para os registros gerados neste fixture.
export function largePlan(): LargePlan {
  const d = documentedTableContracts.AttemptsRawData;
  const fields: WireField[] = d.fields.map(f => {
    if (f.sqlType === 'int' || f.sqlType === 'datetime') return { name: f.name, type: f.sqlType, nullable: f.name !== d.documentedKey };
    const match = /^varchar\((\d+|max)\)$/.exec(f.sqlType)!;
    return { name: f.name, type: 'varchar', maxBytes: match[1] === 'max' ? 65536 : Number(match[1]), encoding: 'utf-8', nullable: true };
  });
  const contract: SourceContract = { source_table: 'AttemptsRawData', version: 'phase3c-synthetic-v1', approved: true,
    fields: Object.fromEntries(fields.map(f => [f.name,{ type: f.type === 'datetime' ? 'sql_datetime_ticks' : f.type === 'int' ? 'int64' : 'text', nullable: f.nullable } satisfies FieldDefinition])),
    key_fields: ['AttemptsRawDataID'], layout_evidence: 'SYNTHETIC TEST: raw-body codec v1; manual names/types', key_evidence: 'SYNTHETIC TEST: unique fixture IDs only' };
  return { run: { table: 'AttemptsRawData', contractVersion: contract.version, readType: 'current_day', exportDate: '2020-06-03', pkRange: null },
    contract, fields, signature: fixtureSignature('ReadDataCurrentDay'), parameters: { TableName: 'AttemptsRawData' },
    acquisitionEvidence: 'SYNTHETIC raw value bodies before Tedious; not a live transport', responseEvidence: 'SYNTHETIC complete event and return 0',
    partitionEvidence: 'SYNTHETIC one partition assigned explicitly', successReturnValues: [0] };
}
export function rawRow(plan = largePlan(), id = 1): readonly (Uint8Array | null)[] {
  return plan.fields.map(f => {
    if (f.type === 'int') { const b = Buffer.alloc(4); b.writeInt32LE(f.name === 'AttemptsRawDataID' ? id : 0); return b; }
    if (f.type === 'datetime') { const b = Buffer.alloc(8); b.writeUInt32LE(1,4); return b; } // tick exato 1/300s, nunca Date
    return Buffer.from(f.name === 'CampaignData' ? '{"synthetic":"Á"}' : 'X','utf8');
  });
}
export function rawSource(rows: number, options: { failAt?: number; complete?: boolean; duplicate?: boolean } = {}): RawSource & { calls: number } {
  const source = { kind: 'synthetic' as const, calls: 0,
    async *read(plan: LargePlan, signal: AbortSignal) {
      source.calls++;
      const base = rawRow(plan), index = plan.fields.findIndex(f => f.name === 'AttemptsRawDataID');
      for (let i = 0; i < rows; i++) {
        if (signal.aborted || i === options.failAt) throw Object.assign(new Error('DO NOT LOG: synthetic VPN/private failure'),{ code: 'ECONNRESET' });
        const values = [...base], id = Buffer.alloc(4); id.writeInt32LE(options.duplicate ? 1 : i+1); values[index] = id;
        yield { kind: 'row' as const, values };
      }
      if (options.complete !== false) yield { kind: 'complete' as const, datasetCount: 1, returnValue: 0, errorMessage: null };
    } };
  return source;
}
export function spoolIdentity(plan = largePlan()): SpoolIdentity {
  return { format: 'olos-spool/v1', batchId: randomUUID(), runId: randomUUID(), streamId: randomUUID(), fencingToken: '1', plan,
    expected: { version: '0', cursor: null } };
}
