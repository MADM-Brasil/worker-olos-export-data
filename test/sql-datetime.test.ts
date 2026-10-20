import { describe, expect, it } from 'vitest';
import { sqlDatetimeToCivil } from '../src/offline/sql-datetime.js';

function encode(days: number, ticks: number): string {
  const bytes = Buffer.alloc(8);
  bytes.writeInt32LE(days, 0);
  bytes.writeUInt32LE(ticks, 4);
  return `sql-datetime/v1:${bytes.toString('hex')}`;
}

describe('SQL DATETIME -> horario civil', () => {
  it('preserva a representacao fracionaria com arredondamento explicito', () => {
    expect(sqlDatetimeToCivil(encode(0, 1)))
      .toBe('1900-01-01T00:00:00.003333');

    expect(sqlDatetimeToCivil(encode(0, 2)))
      .toBe('1900-01-01T00:00:00.006667');

    expect(sqlDatetimeToCivil(encode(0, 300)))
      .toBe('1900-01-01T00:00:01.000000');
  });

  it('interpreta os dias e a transicao do calendario', () => {
    expect(sqlDatetimeToCivil(encode(1, 0)))
      .toBe('1900-01-02T00:00:00.000000');

    expect(sqlDatetimeToCivil(encode(0, 25_919_999)))
      .toBe('1900-01-01T23:59:59.996667');
  });

  it('rejeita valores fora do contrato', () => {
    expect(() => sqlDatetimeToCivil('sql-datetime/v1:1234'))
      .toThrow('INVALID_SQL_DATETIME');

    expect(() => sqlDatetimeToCivil(encode(0, 25_920_000)))
      .toThrow();

    expect(sqlDatetimeToCivil(null)).toBeNull();
  });
});
