import { createRequire } from 'node:module';
import { expect, it } from 'vitest';
import { Request, Connection } from 'tedious';
import { Collation } from 'tedious/lib/collation.js';
import { ExactTokenParser, checkMetadata, type TdsField } from '../src/olos/tds/raw-parser.js';
import { prepareTdsRequest, IsolatedTdsSource } from '../src/olos/tds/source.js';
import { fixtureSignature } from './support/olos-documented-fixture.js';
import { largePlan } from './support/large-batch-fixture.js';
import { LargeBatchCoordinator } from '../src/large-batch/coordinator.js';
import { spoolLimits } from '../src/large-batch/config.js';
import type { ColumnMetadata } from 'tedious/lib/token/colmetadata-token-parser.js';
const require = createRequire(import.meta.url);
const Debug = require('tedious/lib/debug.js') as typeof import('tedious/lib/debug.js').default;
const options = { useUTC: true,lowerCaseGuids: false,tdsVersion: '7_4' as const,useColumnNames: false,columnNameReplacer: undefined,camelCaseColumns: false };
const collation = new Collation(1033,0,0,0), collationHex = collation.toBuffer().toString('hex');
const u16 = (n: number) => { const b = Buffer.alloc(2); b.writeUInt16LE(n); return b; };
const u32 = (n: number) => { const b = Buffer.alloc(4); b.writeUInt32LE(n); return b; };
const u64 = (n: bigint) => { const b = Buffer.alloc(8); b.writeBigUInt64LE(n); return b; };
function metadata(name: string, info: Buffer, nullable = true) { return Buffer.concat([Buffer.alloc(4),u16(nullable ? 1 : 0),info,Buffer.from([name.length]),Buffer.from(name,'utf16le')]); }
function frame(...cols: Buffer[]) { return Buffer.concat([Buffer.from([0x81]),u16(cols.length),...cols]); }
async function collect(bytes: Buffer,fields: TdsField[],fragment = bytes.length) {
  async function* chunks() { for (let i=0;i<bytes.length;i+=fragment) yield bytes.subarray(i,i+fragment); }
  const p = new ExactTokenParser(chunks(),new Debug({ data: false,payload: false,packet: false,token: false }),options,fields);
  const tokens = []; for await (const token of p.tokens()) tokens.push(token); return tokens;
}
const idField: TdsField = { name: 'Id',type: 'bigint',nullable: true };
const idMeta = metadata('Id',Buffer.from([0x26,8]));

it('ROW e NBCROW fragmentados byte a byte associam nomes/ordinais/bytes e null sem readValue',async () => {
  const dt: TdsField = { name: 'At',type: 'datetime',nullable: true };
  const body = Buffer.concat([u32(0),u32(1)]), id = Buffer.alloc(8); id.writeBigInt64LE(9223372036854775807n);
  const bytes = Buffer.concat([frame(idMeta,metadata('At',Buffer.from([0x6f,8]))),Buffer.from([0xd1,8]),id,Buffer.from([8]),body,Buffer.from([0xd2,3])]);
  const tokens = await collect(bytes,[idField,dt],1);
  const row = tokens[1] as unknown as { columns: { value: Buffer; metadata: ColumnMetadata }[] };
  expect(row.columns.map(c => c.metadata.colName)).toEqual(['Id','At']); expect(row.columns[0]!.value).toEqual(id); expect(row.columns[1]!.value).toEqual(body);
  expect((tokens[2] as unknown as typeof row).columns.map(c => c.value)).toEqual([null,null]);
});

it.each([0n,5n,0xfffffffffffffffen])('PLP VARCHAR exato conhecido/desconhecido, incluindo vazio: %s',async total => {
  const field: TdsField = { name: 'Text',type: 'varchar',nullable: true,maxBytes: 100,sqlLength: 'max',encoding: 'windows-1252',collationHex };
  const meta = frame(metadata('Text',Buffer.concat([Buffer.from([0xa7]),u16(65535),collation.toBuffer()])));
  const parts = total === 0n ? [u32(0)] : [u32(2),Buffer.from([128,88]),u32(3),Buffer.from('   '),u32(0)];
  const tokens = await collect(Buffer.concat([meta,Buffer.from([0xd1]),u64(total),...parts]),[field],1);
  const values = (tokens[1] as unknown as { columns: { value: Buffer }[] }).columns;
  expect(values[0]!.value).toEqual(total === 0n ? Buffer.alloc(0) : Buffer.from([128,88,32,32,32]));
});

it('PLP null não vira vazio; lengths truncados/inconsistentes/excessivos falham',async () => {
  const field: TdsField = { name: 'Text',type: 'nvarchar',nullable: true,maxBytes: 100,sqlLength: 'max',collationHex };
  const m = frame(metadata('Text',Buffer.concat([Buffer.from([0xe7]),u16(65535),collation.toBuffer()])));
  const ok = await collect(Buffer.concat([m,Buffer.from([0xd1]),u64(0xffffffffffffffffn)]),[field]);
  expect((ok[1] as unknown as { columns: { value: null }[] }).columns[0]!.value).toBeNull();
  for (const tail of [Buffer.from([0xd1,1]),Buffer.concat([Buffer.from([0xd1]),u64(4n),u32(2),Buffer.from([65,0]),u32(0)]),Buffer.concat([Buffer.from([0xd1]),u64(99999999n)])]) {
    await expect(collect(Buffer.concat([m,tail]),[field],1)).rejects.toMatchObject({ code: 'INVALID_RESPONSE' });
  }
});

it('não aceitar metadados trocados, duplicados, tipo/nullable/charset/collation/comprimento desconhecidos',async () => {
  await expect(collect(frame(metadata('Other',Buffer.from([0x26,8]))),[idField])).rejects.toMatchObject({ code: 'INVALID_RESPONSE' });
  await expect(collect(frame(metadata('Id',Buffer.from([0x26,4]))),[idField])).rejects.toMatchObject({ code: 'INVALID_RESPONSE' });
  await expect(collect(frame(metadata('Id',Buffer.from([0x26,8]),false)),[idField])).rejects.toMatchObject({ code: 'INVALID_RESPONSE' });
  await expect(collect(frame(idMeta,idMeta),[idField,idField])).rejects.toMatchObject({ code: 'INVALID_RESPONSE' });
  await expect(collect(Buffer.from([0x81,255,255]),[idField])).rejects.toMatchObject({ code: 'INVALID_RESPONSE' });
  const field: TdsField = { name: 'Text',type: 'varchar',nullable: true,maxBytes: 100,sqlLength: 100,encoding: 'utf-8',collationHex };
  const m = metadata('Text',Buffer.concat([Buffer.from([0xa7]),u16(100),collation.toBuffer()]));
  await expect(collect(frame(m),[field])).rejects.toMatchObject({ code: 'INVALID_RESPONSE' });
  await expect(collect(frame(m),[{ ...field,encoding: 'windows-1252',collationHex: '0000000000' }])).rejects.toMatchObject({ code: 'INVALID_RESPONSE' });
});

it('limites de memória/linha/célula e falha de stream são explícitos',async () => {
  const m = frame(idMeta), bytes = Buffer.concat([m,Buffer.from([0xd1,8]),u64(1n)]);
  const p = new ExactTokenParser([bytes],new Debug({ data: false,payload: false,packet: false,token: false }),options,[idField],{ maxCellBytes: 4,maxRowBytes: 4,maxBufferedBytes: 1024 });
  await expect((async () => { for await (const _ of p.tokens()) {} })()).rejects.toMatchObject({ code: 'INVALID_RESPONSE' });
  async function* chunks() { yield m; throw new Error('synthetic network'); }
  const interrupted = new ExactTokenParser(chunks(),new Debug({ data: false,payload: false,packet: false,token: false }),options,[idField]);
  await expect((async () => { for await (const _ of interrupted.tokens()) {} })()).rejects.toThrow('synthetic network');
});

it('parâmetros oficiais tipados: DATE civil e BIGINT não passam por number',() => {
  const r = new Request('dbo.usp_ExportData_ReReadData',() => {});
  prepareTdsRequest(r,fixtureSignature('ReReadData'),{ TableName: 'AttemptsRawData',DateExport: '2020-06-03',PKIDStart: '-9223372036854775808',PKIDEnd: '9223372036854775807' },5000);
  expect(r.parameters.find(p => p.name === 'PKIDEnd')?.value).toBe(9223372036854775807n);
  expect(r.parameters.find(p => p.name === 'DateExport')?.value).toEqual(new Date('2020-06-03T00:00:00.000Z'));
  for (const name of ['ResetReadData','DataAvailable'] as const) expect(() => prepareTdsRequest(new Request('',() => {}),fixtureSignature(name),{},5000)).toThrow();
  const drift = fixtureSignature('ReadData');
  expect(() => prepareTdsRequest(new Request('',() => {}),{ ...drift,parameters: drift.parameters.map(p => p.name === 'Message' ? { ...p,type: { kind: 'varchar',length: 4000 } } : p) },{ TableName: 'Users' },5000)).toThrow();
});

it('endpoints corporativos e falsa identidade de origem isolada são bloqueados antes de qualquer efeito',async () => {
  const config = { server: '127.0.0.1' as const,database: 'phase3d_fixture' as const,port: 12345,user: 'test',password: 'synthetic',ca: 'BEGIN CERTIFICATE' };
  expect(() => new IsolatedTdsSource({ ...config,server: '10.196.234.11' as '127.0.0.1' },[idField])).toThrow('REAL_EXECUTION_BLOCKED');
  expect(() => new IsolatedTdsSource({ ...config,database: 'ExportDataV3' as 'phase3d_fixture' },[idField])).toThrow('REAL_EXECUTION_BLOCKED');
  const coordinator = new LargeBatchCoordinator({} as never,{} as never,{} as never,spoolLimits());
  await expect(coordinator.receive(largePlan(),{ kind: 'isolated-sqlserver',async *read() { throw new Error('Must not reach'); } },new AbortController().signal)).rejects.toMatchObject({ code: 'SOURCE_BLOCKED' });
  expect(() => new IsolatedTdsSource(config,[idField],{ maxCellBytes: -1,maxRowBytes: 100,maxBufferedBytes: 100 })).toThrow('INVALID_PARAMETER');
  expect(Connection.prototype).not.toHaveProperty('acquisition'); // extensão somente na instância filha
});
