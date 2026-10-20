import { createRequire } from 'node:module';
import sql from 'mssql';
import { describe, expect, it, vi } from 'vitest';
import { decimalFromTdsBody, legacyDatetimeFromTdsBody } from '../src/olos/exact-value-prototypes.js';
import { MssqlResponseCollector } from '../src/olos/mssql-collector.js';

// Usa somente funções locais do parser instalado, sobre buffers sintéticos, sem conectar a SQL Server.
const require = createRequire(import.meta.url), mssqlRequire = createRequire(require.resolve('mssql/package.json'));
const tediousRequire = createRequire(mssqlRequire.resolve('tedious/package.json'));
const { readValue } = tediousRequire('./lib/value-parser.js') as {
  readValue(buf: Buffer, offset: number, metadata: { type: { name: string }; precision?: number; scale?: number }, options: { useUTC: boolean }): { value: unknown };
};
function decimalBody(value: bigint, precision: number) {
  const bytes = Buffer.alloc(precision <= 9 ? 5 : precision <= 19 ? 9 : precision <= 28 ? 13 : 17);
  bytes[0] = value < 0n ? 0 : 1;
  let rest = value < 0n ? -value : value;
  for (let i = 1; i < bytes.length; i++) { bytes[i] = Number(rest % 256n); rest /= 256n; }
  return bytes;
}
describe('investigação reprodutível de precisão antes do parser', () => {
  it.each([['0',1,0,'0'],['1',1,1,'0.1'],['-123400',6,4,'-12.3400'],['9007199254740993',19,0,'9007199254740993'],
    ['99999999999999999999999999999999999999',38,7,'9999999999999999999999999999999.9999999']] as const)('prototype DECIMAL %s p=%s s=%s conserva valor/escala', (value,precision,scale,expected) => {
    expect(decimalFromTdsBody(decimalBody(BigInt(value),precision),precision,scale)).toBe(expected);
  });
  it('preserva NULL e recusa comprimento, sinal, precisão e escala inválidos', () => {
    expect(decimalFromTdsBody(null,38,7)).toBeNull();
    for (const [body,p,s] of [[Buffer.alloc(5),0,0],[Buffer.alloc(5),9,10],[Buffer.alloc(4),9,0],[Buffer.from([2,0,0,0,0]),9,0],[decimalBody(1000n,3),3,0]] as const) {
      expect(() => decimalFromTdsBody(body,p,s)).toThrow('PRECISION_UNSAFE');
    }
  });
  it('demonstra perda REAL no parser instalado de DECIMAL, sem SQL Server', () => {
    const body = decimalBody(9007199254740993n,19);
    const parsed = readValue(Buffer.concat([Buffer.from([body.length]),body]),0,{ type: { name: 'DecimalN' }, precision: 19, scale: 0 },{ useUTC: true }).value;
    expect(parsed).toBe(9007199254740992);
    expect(decimalFromTdsBody(body,19,0)).toBe('9007199254740993');
  });
  it('DATETIME conserva tick de 1/300s em tupla bruta; Date do driver já o arredondou', () => {
    const body = Buffer.alloc(8); body.writeUInt32LE(1,4);
    expect(legacyDatetimeFromTdsBody(body)).toEqual({ representation: 'sql-datetime-ticks/v1', daysSince1900: '0', ticks300: '1', rawHex: '0000000001000000' });
    const parsed = readValue(body,0,{ type: { name: 'DateTime' } },{ useUTC: true }).value as Date;
    expect(parsed.toISOString()).toBe('1900-01-01T00:00:00.003Z');
    expect(legacyDatetimeFromTdsBody(null)).toBeNull();
    const invalid = Buffer.alloc(8); invalid.writeUInt32LE(86_400 * 300,4);
    expect(() => legacyDatetimeFromTdsBody(invalid)).toThrow('PRECISION_UNSAFE');
  });
  it('DATETIME2 perde o sétimo dígito no ISO/clone apesar de nanosecondsDelta existir no parser', () => {
    const value = Buffer.alloc(9); value[0] = 8; value.writeUIntLE(1234567,1,5); value.writeUIntLE(737578,6,3);
    const parsed = readValue(value,0,{ type: { name: 'DateTime2' }, scale: 7 },{ useUTC: true }).value as Date & { nanosecondsDelta?: number };
    expect(parsed.toISOString()).toMatch(/\.123Z$/); expect(parsed.nanosecondsDelta).toBeCloseTo(0.0004567,10);
    expect((structuredClone(parsed) as typeof parsed).nanosecondsDelta).toBeUndefined();
  });
  it('DATETIMEOFFSET com dois offsets distintos devolve o mesmo Date pelo parser instalado', () => {
    const a = Buffer.alloc(11); a[0] = 10; a.writeUIntLE(1234567,1,5); a.writeUIntLE(737578,6,3); a.writeInt16LE(-180,9);
    const b = Buffer.from(a); b.writeInt16LE(180,9);
    const parse = (v: Buffer) => readValue(v,0,{ type: { name: 'DateTimeOffset' }, scale: 7 },{ useUTC: true }).value as Date;
    expect(a.equals(b)).toBe(false); expect(parse(a).getTime()).toBe(parse(b).getTime());
  });
  it('coletor normaliza NVARCHAR 256 bytes para 128 unidades, sem liberar DECIMAL/tempo', () => {
    const request = new sql.Request(), cancel = vi.spyOn(request,'cancel').mockImplementation(() => {});
    const collector = new MssqlResponseCollector(request,new AbortController().signal,{ maxRows: 10, maxBytes: 1000 });
    try {
      request.emit('recordset',[{ name: 'Value', type: sql.NVarChar, length: 256 }]); request.emit('row',['Á']);
      expect(collector.finish({ output: {}, returnValue: 0 }).datasets[0]?.columns).toEqual([{ name: 'Value', type: 'nvarchar', length: 128 }]);
      request.emit('recordset',[{ name: 'Clock', type: sql.DateTime }]);
      expect(() => collector.finish({ output: {}, returnValue: 0 })).toThrow('PRECISION_UNSAFE'); expect(cancel).toHaveBeenCalled();
    } finally { collector.dispose(); cancel.mockRestore(); }
  });
});
