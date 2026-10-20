import sql from 'mssql';
import { sensitiveField } from './contracts.js';
import { OlosError, type Column, type Dataset, type NativeSqlType, type ProcedureResponse } from './types.js';
import { textTypeFromCatalog } from './procedure-signatures.js';

interface DriverColumn { readonly name: string; readonly type: unknown; readonly precision?: number; readonly scale?: number; readonly length?: number; }
const nativeTypes = new Map<unknown, NativeSqlType>([
  [sql.BigInt,'bigint'], [sql.Int,'int'], [sql.SmallInt,'smallint'], [sql.TinyInt,'tinyint'],
  [sql.Decimal,'decimal'], [sql.Numeric,'numeric'], [sql.NVarChar,'nvarchar'], [sql.VarChar,'varchar'],
  [sql.NChar,'nchar'], [sql.Char,'char'], [sql.UniqueIdentifier,'uniqueidentifier'], [sql.Bit,'bit'],
  [sql.DateTime2,'datetime2'], [sql.DateTimeOffset,'datetimeoffset'], [sql.DateTime,'datetime'],
  [sql.SmallDateTime,'smalldatetime'], [sql.VarBinary,'varbinary'], [sql.Binary,'binary'],
]);
/** Coletor de eventos node-mssql; não conecta nem inicia requisições. Testável com eventos sintéticos. */
export class MssqlResponseCollector {
  private readonly datasets: { columns: Column[]; rows: Record<string, unknown>[]; representation: 'mssql-native' }[] = [];
  private ordinals: { name: string; ordinal: number }[] = [];
  private width = 0;
  private failure: OlosError | undefined;
  private rows = 0;
  private bytes = 0;
  private readonly abort: () => void;
  constructor(private readonly request: sql.Request, private readonly signal: AbortSignal,
    private readonly limits: { readonly maxRows: number; readonly maxBytes: number }) {
    request.stream = true;
    request.arrayRowMode = true;
    this.abort = () => this.fail('ABORTED');
    request.on('recordset', this.metadata);
    request.on('row', this.row);
    request.on('error', this.error);
    signal.addEventListener('abort', this.abort, { once: true });
    if (signal.aborted) this.abort();
  }
  private fail(code: OlosError['code']): void {
    if (this.failure) return;
    this.failure = new OlosError(code, 'unknown');
    // cancel é solicitação; quem invocou deve aguardar conclusão nativa antes de liberar o pool/lock.
    try { this.request.cancel(); } catch { /* preservar erro sanitizado */ }
  }
  private readonly error = (error: unknown) => {
    this.fail(error !== null && typeof error === 'object' && 'code' in error && error.code === 'ETIMEOUT' ? 'TIMEOUT' : 'EXECUTION_FAILED');
  };
  private readonly metadata = (metadata: readonly DriverColumn[]) => {
    if (this.failure) return;
    if (!Array.isArray(metadata) || metadata.length > 512 || this.datasets.length >= 16) { this.fail('INVALID_RESPONSE'); return; }
    const names = new Set<string>(), columns: Column[] = [], ordinals: typeof this.ordinals = [];
    for (let ordinal = 0; ordinal < metadata.length; ordinal++) {
      const meta = metadata[ordinal]!;
      if (typeof meta.name !== 'string' || meta.name.length === 0 || meta.name.length > 128) { this.fail('INVALID_RESPONSE'); return; }
      if (sensitiveField(meta.name)) continue;
      const type = nativeTypes.get(meta.type);
      if (!type || names.has(meta.name.toLowerCase())) { this.fail('INVALID_RESPONSE'); return; }
      // Cancelar já no metadado. Não formatar valores que o parser Tedious converte destrutivamente.
      if (['decimal','numeric','datetime2','datetimeoffset','datetime','smalldatetime'].includes(type)) { this.fail('PRECISION_UNSAFE'); return; }
      names.add(meta.name.toLowerCase());
      let length: number | 'max' | undefined;
      if (['varchar','nvarchar','char','nchar'].includes(type) && meta.length !== undefined) {
        try {
          const rawLength = ['varchar','nvarchar'].includes(type) && meta.length === 0xffff ? -1 : meta.length;
          length = textTypeFromCatalog(type === 'nvarchar' || type === 'nchar' ? 'nvarchar' : 'varchar', rawLength).length;
          if (length === 'max' && ['char','nchar'].includes(type)) throw new Error();
        } catch { this.fail('INVALID_RESPONSE'); return; }
      }
      columns.push({ name: meta.name, type, ...(meta.precision === undefined ? {} : { precision: meta.precision }),
        ...(meta.scale === undefined ? {} : { scale: meta.scale }), ...(length === undefined ? {} : { length }) });
      ordinals.push({ name: meta.name, ordinal });
    }
    this.width = metadata.length;
    this.ordinals = ordinals;
    this.datasets.push({ columns, rows: [], representation: 'mssql-native' });
  };
  private readonly row = (row: readonly unknown[]) => {
    if (this.failure) return;
    const current = this.datasets.at(-1);
    if (!current || !Array.isArray(row) || row.length !== this.width) { this.fail('INVALID_RESPONSE'); return; }
    if (++this.rows > this.limits.maxRows) { this.fail('LIMIT_EXCEEDED'); return; }
    const output: Record<string, unknown> = {};
    for (const { name, ordinal } of this.ordinals) {
      const value = row[ordinal];
      if (value instanceof Date || typeof value === 'number' && !Number.isSafeInteger(value)) { this.fail('PRECISION_UNSAFE'); return; }
      if (value !== null && !['string','boolean','number'].includes(typeof value) && !(value instanceof Uint8Array)) { this.fail('INVALID_RESPONSE'); return; }
      this.bytes += Buffer.byteLength(name) + (typeof value === 'string' ? Buffer.byteLength(value) : value instanceof Uint8Array ? value.byteLength : 16);
      if (this.bytes > this.limits.maxBytes) { this.fail('LIMIT_EXCEEDED'); return; }
      Object.defineProperty(output, name, { value: value instanceof Uint8Array ? new Uint8Array(value) : value, enumerable: true });
    }
    current.rows.push(output);
  };
  // Usar somente após o término da promise de execução, inclusive em modo stream.
  finish(result: { readonly output: Record<string, unknown>; readonly returnValue: unknown }): ProcedureResponse {
    if (this.failure) throw this.failure;
    return { datasets: this.datasets as readonly Dataset[], output: result.output, returnValue: result.returnValue };
  }
  dispose(): void {
    this.request.removeListener('recordset', this.metadata);
    this.request.removeListener('row', this.row);
    this.request.removeListener('error', this.error);
    this.signal.removeEventListener('abort', this.abort);
  }
}
