import { describe, expect, it } from 'vitest';
import { adaptDataset, validateTableContract } from '../src/olos/adapter.js';
import { defaultLimits } from '../src/persistence/types.js';
import { tableContracts } from '../src/olos/contracts.js';
import { dataset, record, tableContract } from './support/olos.js';

describe('aquisição e envelopes exatos por contrato sintético', () => {
  it.each(Object.keys(tableContracts) as (keyof typeof tableContracts)[])('adapta %s preservando chave documentada e valores originais', table => {
    const row = record('9223372036854775807', table), before = structuredClone(row);
    const adapted = adaptDataset(dataset([row], table), tableContract(table));
    expect(adapted[0]).toEqual(row);
    expect(row).toEqual(before);
    expect(Object.isFrozen(adapted[0])).toBe(true);
  });
  it('mantém null distinto de vazio e fração/offset completos', () => {
    const row = record();
    const result = adaptDataset(dataset([row, { ...record('2'), FixtureText: '' }]), tableContract());
    expect(result[0]?.FixtureText).toBeNull();
    expect(result[1]?.FixtureText).toBe('');
    expect(result[0]?.FixtureLocal).toBe('2026-10-15T09:30:00.1234567');
    expect(result[0]?.FixtureOffset).toBe('2026-10-15T09:30:00.1234567-03:00');
    expect(result[0]?.FixtureAmount).toBe('12345678901234567890.1234000');
  });
  it.each([
    { AttemptsRawDataID: 9007199254740992 }, { AttemptsRawDataID: 1 }, { AttemptsRawDataID: 1n },
    { AttemptsRawDataID: '9223372036854775808' }, { AttemptsRawDataID: null },
    { FixtureAmount: 12345678901234567890 }, { FixtureAmount: 1 },
    { FixtureAmount: '123456789012345678901234567890123456789' }, { FixtureAmount: '1.12345678' },
    { FixtureLocal: new Date('2026-10-15T09:30:00Z') }, { FixtureLocal: '2026-10-15T09:30:00.12345678' },
    { FixtureLocal: '2026-02-30T09:30:00' }, { FixtureLocal: '2026-10-15T09:30:00Z' },
    { FixtureOffset: '2026-10-15T09:30:00' }, { FixtureOffset: '2026-10-15T09:30:00+15:00' },
    { UnknownField: 'unapproved' }, { Password: 'forbidden' }, { FixtureText: undefined },
  ])('rejeita representação inexata/schema drift antes da persistência %#', patch => {
    expect(() => adaptDataset(dataset([{ ...record(), ...patch }]), tableContract())).toThrow();
  });
  it('ausente não vira null', () => {
    const { FixtureText: _omitted, ...row } = record();
    expect(() => adaptDataset(dataset([row]), tableContract())).toThrow('INVALID_RESPONSE');
  });
  it('bloqueia DECIMAL e datas do parser nativo sem aquisição textual comprovada', () => {
    expect(() => adaptDataset({ ...dataset(), representation: 'mssql-native' }, tableContract())).toThrow('PRECISION_UNSAFE');
    expect(() => adaptDataset({ ...dataset(), exactEncodingEvidence: '' }, tableContract())).toThrow('PRECISION_UNSAFE');
  });
  it('BIGINT nativo textual é aceito; inteiro SQL de 32 bits pode ser convertido exatamente', () => {
    const c = tableContract();
    const columns = [{ name: 'AttemptsRawDataID', type: 'bigint' as const }, { name: 'FixtureCount', type: 'int' as const }];
    const contract = { ...c, source: { ...c.source, fields: { AttemptsRawDataID: { type: 'int64' as const, nullable: false }, FixtureCount: { type: 'int64' as const, nullable: false } } }, columns };
    const data = { columns, rows: [{ AttemptsRawDataID: '9007199254740993', FixtureCount: 2147483647 }], representation: 'mssql-native' as const };
    expect(adaptDataset(data, contract)[0]).toEqual({ AttemptsRawDataID: '9007199254740993', FixtureCount: '2147483647' });
    expect(() => adaptDataset({ ...data, rows: [{ AttemptsRawDataID: '1', FixtureCount: 2147483648 }] }, contract)).toThrow('PRECISION_UNSAFE');
  });
  it('não converte TEXT em número nem confunde CallId com PK de extração', () => {
    const c = tableContract();
    const columns = [{ name: 'AttemptsRawDataID', type: 'bigint' as const }, { name: 'CallId', type: 'varchar' as const }];
    const source = { ...c.source, fields: { AttemptsRawDataID: { type: 'int64' as const, nullable: false }, CallId: { type: 'text' as const, nullable: true } } };
    expect(adaptDataset({ columns, rows: [{ AttemptsRawDataID: '9007199254740993', CallId: '000AbC' }], representation: 'mssql-native' }, { ...c, source, columns })[0]?.CallId).toBe('000AbC');
  });
  it('aceita duplicatas iguais sem apagar contagem recebida e rejeita conflito', () => {
    expect(adaptDataset(dataset([record(), record()]), tableContract())).toHaveLength(2);
    expect(() => adaptDataset(dataset([record(), { ...record(), FixtureText: 'changed' }]), tableContract())).toThrow('VALIDATION_FAILED');
  });
  it('rejeita mudança de metadados e precisão, inclusive sem linhas', () => {
    const data = dataset([]);
    expect(() => adaptDataset({ ...data, columns: [...data.columns, { name: 'X', type: 'varchar' }] }, tableContract())).toThrow('INVALID_RESPONSE');
    const c = tableContract();
    expect(() => validateTableContract({ ...c, source: { ...c.source, approved: false } })).toThrow('CONTRACT_UNCONFIRMED');
    expect(() => validateTableContract({ ...c, columns: c.columns.map(col => col.type === 'decimal' ? { ...col, precision: 39 } : col) })).toThrow('CONTRACT_UNCONFIRMED');
  });
  it('limita registros e bytes antes de persistir', () => {
    expect(() => adaptDataset(dataset([record(), record('2')]), tableContract(), { ...defaultLimits, maxRecords: 1 })).toThrow('LIMIT_EXCEEDED');
    expect(() => adaptDataset(dataset(), tableContract(), { ...defaultLimits, maxBytes: 10 })).toThrow('VALIDATION_FAILED');
  });
});
