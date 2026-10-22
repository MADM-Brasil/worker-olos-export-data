import { createRequire } from 'node:module';
const StreamParser = createRequire(import.meta.url)('tedious/lib/token/stream-parser.js') as typeof import('tedious/lib/token/stream-parser.js').default;
import { RowToken, NBCRowToken, ReturnValueToken, type Token } from 'tedious/lib/token/token.js';
import type { ColumnMetadata } from 'tedious/lib/token/colmetadata-token-parser.js';
type Debug = InstanceType<typeof import('tedious/lib/debug.js').default>;
import type { ParserOptions } from 'tedious/lib/token/stream-parser.js';
import { readMetadata, type Metadata } from 'tedious/lib/metadata-parser.js';
import { NotEnoughDataError } from 'tedious/lib/token/helpers.js';
import type { WireField } from '../../large-batch/types.js';
import { OlosError } from '../types.js';

export interface TdsField extends Omit<WireField,'type'> {
  readonly type: WireField['type'] | 'date';
  /** Comprimento SQL declarado, distinto do orçamento local para MAX. */
  readonly sqlLength?: number | 'max';
  /** Bytes de collation autorizados; obrigatório para texto. Não inferido do charset local. */
  readonly collationHex?: string;
  /** NVARCHAR é UTF-16LE no wire; capturar collation sem inferir semântica de comparação. */
  readonly unicodeMetadataPolicy?: 'capture-code-units';
}
export interface RawParserLimits { readonly maxCellBytes: number; readonly maxRowBytes: number; readonly maxBufferedBytes: number; }
export const parserLimits: RawParserLimits = Object.freeze({ maxCellBytes: 1024 ** 2, maxRowBytes: 1024 ** 2, maxBufferedBytes: 2 * 1024 ** 2 });
const fail = (): never => { throw new OlosError('INVALID_RESPONSE','unknown'); };
export function validateParserLimits(bounds: RawParserLimits): void {
  if (Object.values(bounds).length !== 3 || Object.values(bounds).some(n => !Number.isSafeInteger(n) || n < 1 || n > 16 * 1024 ** 2) ||
      bounds.maxCellBytes > bounds.maxRowBytes || bounds.maxRowBytes > bounds.maxBufferedBytes) throw new OlosError('INVALID_PARAMETER');
}
export function checkMetadata(actual: readonly ColumnMetadata[], fields: readonly TdsField[]): void {
  if (!actual.length || actual.length !== fields.length || actual.length > 512 || new Set(actual.map(c => c.colName.toLowerCase())).size !== actual.length) fail();
  actual.forEach((c,i) => {
    const f = fields[i]!, n = c.type.name;
    if (c.colName !== f.name || Boolean(c.flags & 1) !== f.nullable || c.cryptoMetadata) fail();
    const fits = f.type === 'int' ? n === 'Int' || n === 'IntN' && c.dataLength === 4 :
      f.type === 'bigint' ? n === 'BigInt' || n === 'IntN' && c.dataLength === 8 :
      f.type === 'datetime' ? n === 'DateTime' || n === 'DateTimeN' && c.dataLength === 8 :
      f.type === 'decimal' ? ['DecimalN','NumericN'].includes(n) && c.precision === f.precision && c.scale === f.scale :
      f.type === 'date' ? n === 'Date' : n === ({ varchar: 'VarChar', char: 'Char', nvarchar: 'NVarChar' } as Record<string,string>)[f.type];
    if (!fits) fail();
    if (['varchar','char','nvarchar'].includes(f.type)) {
      const len = f.sqlLength === 'max' ? 65535 : typeof f.sqlLength === 'number' ? f.sqlLength * (f.type === 'nvarchar' ? 2 : 1) : undefined;
      const capturedUnicode = f.type === 'nvarchar' && f.unicodeMetadataPolicy === 'capture-code-units' && f.collationHex === undefined && c.collation?.toBuffer().length === 5;
      if (len === undefined || c.dataLength !== len || !capturedUnicode && (!f.collationHex || c.collation?.toBuffer().toString('hex') !== f.collationHex)) fail();
      if (f.type !== 'nvarchar') {
        const cp = c.collation?.codepage;
        if (f.encoding === 'utf-8' ? cp !== 'utf-8' : f.encoding === 'windows-1252' ? cp !== 'CP1252' : f.encoding !== 'ascii' || !['CP1252','utf-8'].includes(cp ?? '')) fail();
      }
    }
  });
}
/** Observed transport descriptors, NOT approval of an Olos layout or business identity. */
export function observeMetadata(columns: readonly ColumnMetadata[], bounds: RawParserLimits = parserLimits): readonly TdsField[] {
  if (!columns.length || columns.length > 512) fail();
  const fields = columns.map((c): TdsField => {
    if (/password|passwd|senha/i.test(c.colName)) fail();
    const base = { name: c.colName, nullable: Boolean(c.flags & 1) }, n = c.type.name;
    if (n === 'Int' || n === 'IntN' && c.dataLength === 4) return { ...base,type: 'int' };
    if (n === 'BigInt' || n === 'IntN' && c.dataLength === 8) return { ...base,type: 'bigint' };
    if (n === 'DateTime' || n === 'DateTimeN' && c.dataLength === 8) return { ...base,type: 'datetime' };
    if (n === 'Date') return { ...base,type: 'date' };
    if (n === 'DecimalN' || n === 'NumericN') return { ...base,type: 'decimal',precision: c.precision!,scale: c.scale! };
    if (!['VarChar','Char','NVarChar'].includes(n) || !c.dataLength || !c.collation) return fail();
    const unicode = n === 'NVarChar', cp = c.collation.codepage;
    if (!unicode && !['CP1252','utf-8'].includes(cp ?? '')) fail();
    const type = unicode ? 'nvarchar' : n === 'Char' ? 'char' : 'varchar';
    return { ...base,type,sqlLength: c.dataLength === 65535 ? 'max' : c.dataLength / (unicode ? 2 : 1),
      maxBytes: Math.min(c.dataLength === 65535 ? bounds.maxCellBytes : c.dataLength,bounds.maxCellBytes),
      collationHex: c.collation.toBuffer().toString('hex'),...(unicode ? {} : { encoding: cp === 'CP1252' ? 'windows-1252' as const : 'utf-8' as const }) };
  });
  checkMetadata(columns,fields);
  return fields;
}
/** Extensão isolada da implementação interna PINNED. Nunca modifica protótipos/imports globais. */
export class ExactTokenParser extends StreamParser {
  private ended = false;
  constructor(iterable: AsyncIterable<Buffer> | Iterable<Buffer>, debug: Debug, options: ParserOptions,
    private readonly fields: readonly TdsField[] | undefined, private readonly bounds: RawParserLimits = parserLimits,
    private readonly metadataObserved?: (columns:readonly ColumnMetadata[])=>void) {
    super(iterable,debug,options);
    validateParserLimits(bounds);
  }
  override async waitForChunk(): Promise<void> {
    const next = await this.iterator.next();
    if (next.done) { this.ended = true; fail(); }
    const b = next.value;
    if (!Buffer.isBuffer(b) || b.length + this.buffer.length - this.position > this.bounds.maxBufferedBytes) fail();
    this.buffer = this.position === this.buffer.length ? b : Buffer.concat([this.buffer.subarray(this.position),b]);
    this.position = 0;
  }
  private async take(n: number): Promise<Buffer> {
    if (!Number.isSafeInteger(n) || n < 0 || n > this.bounds.maxCellBytes) fail();
    while (this.buffer.length - this.position < n) await this.waitForChunk();
    const b = Buffer.from(this.buffer.subarray(this.position,this.position+n)); this.position += n; return b;
  }
  override async readColMetadataToken() {
    while (this.buffer.length - this.position < 2) await this.waitForChunk();
    const count = this.buffer.readUInt16LE(this.position);
    if (!count || count > 512) fail(); // inclusive NO_METADATA: não reutilizar layout de outra resposta
    const token = await super.readColMetadataToken(); this.metadataObserved?.(token.columns);checkMetadata(token.columns,this.fields ?? observeMetadata(token.columns,this.bounds)); return token;
  }
  private async body(m: Metadata, budget: number): Promise<Buffer | null> {
    const type = m.type.name;
    const fixed = ({ Int: 4, BigInt: 8, DateTime: 8 } as Record<string,number>)[type];
    if (fixed) return this.take(fixed);
    if (['IntN','DateTimeN','NumericN','DecimalN','Date'].includes(type)) {
      const length = (await this.take(1))[0]!;
      if (!length) return null;
      const expected = type === 'Date' ? 3 : m.dataLength;
      if (length !== expected || length > budget) fail();
      return this.take(length);
    }
    if (!['VarChar','Char','NVarChar'].includes(type)) fail();
    if (m.dataLength !== 65535) {
      const length = (await this.take(2)).readUInt16LE();
      if (length === 65535) return null;
      if (length > m.dataLength! || length > budget) fail();
      return this.take(length);
    }
    const total = (await this.take(8)).readBigUInt64LE();
    if (total === 0xffffffffffffffffn) return null;
    const unknown = total === 0xfffffffffffffffen;
    if (!unknown && total > BigInt(Math.min(this.bounds.maxCellBytes,budget))) fail();
    let data = Buffer.alloc(unknown ? Math.min(4096,budget,this.bounds.maxCellBytes) : Number(total)), size = 0;
    while (true) {
      const length = (await this.take(4)).readUInt32LE();
      if (!length) break;
      size += length;
      if (size > this.bounds.maxCellBytes || size > budget || !unknown && BigInt(size) > total) fail();
      if (size > data.length) {
        const grown = Buffer.alloc(Math.min(Math.max(size,data.length*2),budget,this.bounds.maxCellBytes)); data.copy(grown); data = grown;
      }
      (await this.take(length)).copy(data,size-length);
    }
    if (!unknown && BigInt(size) !== total) fail();
    return Buffer.from(data.subarray(0,size));
  }
  override async readReturnValueToken(): Promise<ReturnValueToken> {
    const paramOrdinal = (await this.take(2)).readUInt16LE();
    const length = (await this.take(1))[0]!;
    const nameBytes = await this.take(length*2);
    const paramName = new TextDecoder('utf-16le',{ fatal: true, ignoreBOM: true }).decode(nameBytes).replace(/^@/,'');
    await this.take(1); // output status
    let metadata: Metadata;
    while (true) {
      try { const result = readMetadata(this.buffer,this.position,this.options); metadata = result.value; this.position = result.offset; break; }
      catch (e) { if (!(e instanceof NotEnoughDataError)) throw e; await this.waitForChunk(); }
    }
    if (!['VarChar','NVarChar'].includes(metadata.type.name)) fail();
    const bytes = await this.body(metadata,this.bounds.maxCellBytes);
    const cp = metadata.collation?.codepage;
    if (metadata.type.name === 'VarChar' && !['CP1252','utf-8'].includes(cp ?? '')) fail();
    const encoding = metadata.type.name === 'NVarChar' ? 'utf-16le' : cp === 'CP1252' ? 'windows-1252' : 'utf-8';
    const value = bytes === null ? null : new TextDecoder(encoding,{ fatal: true, ignoreBOM: true }).decode(bytes);
    return new ReturnValueToken({ paramOrdinal,paramName,metadata,value });
  }
  private async row(nbc: boolean) {
    if (!this.colMetadata.length) fail();
    const bitmap = nbc ? await this.take(Math.ceil(this.colMetadata.length/8)) : null;
    const columns = []; let bytes = 0;
    for (let i=0; i<this.colMetadata.length; i++) {
      const metadata = this.colMetadata[i]!;
      const value = bitmap && bitmap[i >> 3]! & 1 << (i % 8) ? null : await this.body(metadata,this.bounds.maxRowBytes-bytes);
      bytes += value?.length ?? 0;
      if (bytes > this.bounds.maxRowBytes) fail();
      columns.push({ metadata, value });
    }
    return columns;
  }
  override async readRowToken(): Promise<RowToken> { return new RowToken(await this.row(false)); }
  override async readNbcRowToken(): Promise<NBCRowToken> { return new NBCRowToken(await this.row(true)); }
  async *tokens(): AsyncGenerator<Token> {
    while (true) {
      if (this.position === this.buffer.length) {
        try { await this.waitForChunk(); } catch (e) { if (this.ended) return; throw e; }
      }
      if (this.position === this.buffer.length) continue;
      const type = this.buffer[this.position++]!;
      // Estado/OUTPUTs/metadados continuam nos parsers do Tedious. ROW/NBCROW nunca chamam readValue.
      const token = await this.readToken(type);
      if (token) yield token;
    }
  }
}
