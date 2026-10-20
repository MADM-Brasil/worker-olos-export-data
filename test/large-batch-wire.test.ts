import { describe, expect, it } from 'vitest';
import { decodeWireRow, validateLargePlan } from '../src/large-batch/wire.js';
import { spoolLimits } from '../src/large-batch/config.js';
import { largePlan, rawRow } from './support/large-batch-fixture.js';
import { prepareRecords, deduplicate } from '../src/persistence/validation.js';
import { defaultLimits } from '../src/persistence/types.js';

describe('corpos binários anteriores ao parser, contratos explícitos e tokens sem perda', () => {
  it('leva todos os campos documentados de Attempts ao envelope, preservando tick 1/300 e strings', () => {
    const plan = largePlan(); validateLargePlan(plan);
    const row = decodeWireRow(plan,rawRow(plan,2147483647));
    expect(Object.keys(row)).toHaveLength(18); expect(row.AttemptsRawDataID).toBe('2147483647');
    expect(row.StartDate).toBe('sql-datetime/v1:0000000001000000');
    expect(row.CampaignData).toBe('{"synthetic":"Á"}');
    expect(deduplicate(prepareRecords([row,row],plan.contract,defaultLimits).rows)).toHaveLength(1);
  });
  it('aceita null e vazio separadamente e rejeita Date/number em vez de reconstruir precisão', () => {
    const p = largePlan(), bodies = [...rawRow(p)]; bodies[0] = null;
    expect(decodeWireRow(p,bodies).CallId).toBeNull();
    const text = p.fields.findIndex(f => f.name === 'CampaignData'); bodies[text] = Buffer.alloc(0);
    expect(decodeWireRow(p,bodies).CampaignData).toBe('');
    bodies[text] = new Date() as never; expect(() => decodeWireRow(p,bodies)).toThrow('INVALID_VALUE');
    bodies[text] = 9007199254740992 as never; expect(() => decodeWireRow(p,bodies)).toThrow('INVALID_VALUE');
  });
  it('BIGINT e DECIMAL são decodificados a partir dos bytes sem números JS comuns', () => {
    const p = largePlan();
    const fields = [...p.fields,{ name: 'ExactId', type: 'bigint' as const, nullable: false },{ name: 'ExactAmount', type: 'decimal' as const, precision: 19, scale: 2, nullable: false }];
    const plan = { ...p, fields, contract: { ...p.contract, fields: { ...p.contract.fields, ExactId: { type: 'int64' as const, nullable: false }, ExactAmount: { type: 'decimal' as const, nullable: false } } } };
    validateLargePlan(plan);
    const id = Buffer.alloc(8); id.writeBigInt64LE(9007199254740993n);
    const amount = Buffer.alloc(9); amount[0] = 1; amount.writeBigUInt64LE(9007199254740993n,1);
    const row = decodeWireRow(plan,[...rawRow(p),id,amount]);
    expect(row.ExactId).toBe('9007199254740993'); expect(row.ExactAmount).toBe('90071992547409.93');
  });
  it('não inclui Password nem seus bytes; política de string depende de encoding explícito', () => {
    const p = largePlan(), plan = { ...p, fields: [...p.fields,{ name: 'Password', type: 'nvarchar' as const, nullable: true }] };
    validateLargePlan(plan);
    expect(JSON.stringify(decodeWireRow(plan,[...rawRow(p),Buffer.from('secret')]))).not.toContain('secret');
    expect(() => validateLargePlan({ ...p, fields: p.fields.map(f => f.type === 'varchar' ? { ...f, encoding: undefined } : f) } as never)).toThrow('INVALID_VALUE');
    expect(() => validateLargePlan({ ...p, contract: { ...p.contract, approved: false } })).toThrow('CONTRACT_UNCONFIRMED');
  });
  it('preserva NVARCHAR, CHAR com espaços e VARCHAR com codepage explicitamente aprovada', () => {
    const p = largePlan(), additions = [
      { name: 'Unicode', type: 'nvarchar' as const, maxBytes: 20, nullable: false },
      { name: 'Fixed', type: 'char' as const, maxBytes: 16, encoding: 'ascii' as const, nullable: false },
      { name: 'Latin', type: 'varchar' as const, maxBytes: 20, encoding: 'windows-1252' as const, nullable: false },
    ];
    const plan = { ...p, fields: [...p.fields,...additions], contract: { ...p.contract, fields: { ...p.contract.fields,
      Unicode: { type: 'text' as const, nullable: false }, Fixed: { type: 'text' as const, nullable: false }, Latin: { type: 'text' as const, nullable: false } } } };
    validateLargePlan(plan);
    const row = decodeWireRow(plan,[...rawRow(p),Buffer.from('Á😀','utf16le'),Buffer.from('0001 '),Buffer.from([0xc1])]);
    expect(row.Unicode).toBe('Á😀'); expect(row.Fixed).toBe('0001 '); expect(row.Latin).toBe('Á');
  });
  it('recusa bytes inválidos, UTF-8 malformado, truncamento e token DATETIME forjado', () => {
    const p = largePlan(), row = [...rawRow(p)]; row[0] = Buffer.from([0]);
    expect(() => decodeWireRow(p,row)).toThrow('INVALID_VALUE');
    const rows = [...rawRow(p)], index = p.fields.findIndex(f => f.name === 'CampaignData'); rows[index] = Buffer.from([0xc3]);
    expect(() => decodeWireRow(p,rows)).toThrow('INVALID_VALUE');
    expect(() => decodeWireRow(p,rawRow(p),1)).toThrow('QUOTA_EXCEEDED');
    expect(() => validateLargePlan({ ...p, fields: p.fields.map(f => f.name === 'CampaignData' ? { ...f, type: 'float' } : f) } as never)).toThrow('INVALID_VALUE');
    expect(() => prepareRecords([{ ...decodeWireRow(p,rawRow(p)), StartDate: 'sql-datetime/v1:ffffffffffffffff' }],p.contract,defaultLimits)).toThrow('VALIDATION_FAILED');
  });
  it('limites validados permitem 200 mil, sem modificar o caminho legado ou aprovar consumo', () => {
    expect(spoolLimits().maxRows).toBe(200_000);
    for (const v of [{ maxRows: 200001 },{ bufferBytes: 0 },{ maxRecordBytes: 3 * 1024 ** 2, chunkBytes: 1024 },{ readTimeoutMs: 0 }]) expect(() => spoolLimits(v)).toThrow('INVALID_CONFIG');
  });
});
