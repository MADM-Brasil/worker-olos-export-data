import { assertContract, canonical, deduplicate, prepareRecords } from '../persistence/validation.js';
import { defaultLimits, type SourceContract, type PersistenceLimits, type FieldDefinition } from '../persistence/types.js';
import type { ExportRecord } from '../sync/contracts.js';
import { sensitiveField, tableContracts } from './contracts.js';
import { OlosError, type Column, type Dataset } from './types.js';

export interface ValidatedTableContract {
  readonly source: SourceContract;
  readonly columns: readonly Column[];
  readonly acquisitionEvidence: string;
}
const compatible: Record<Column['type'], FieldDefinition['type']> = {
  bigint: 'int64', int: 'int64', smallint: 'int64', tinyint: 'int64', decimal: 'decimal', numeric: 'decimal',
  nvarchar: 'text', varchar: 'text', nchar: 'text', char: 'text', uniqueidentifier: 'text', bit: 'boolean',
  datetime2: 'local_timestamp', datetime: 'local_timestamp', smalldatetime: 'local_timestamp',
  datetimeoffset: 'offset_timestamp', binary: 'bytes', varbinary: 'bytes',
};
export function validateTableContract(contract: ValidatedTableContract): void {
  const c = contract.source;
  if (!Object.hasOwn(tableContracts, c.source_table) || !contract.acquisitionEvidence) throw new OlosError('CONTRACT_UNCONFIRMED');
  assertContract(c);
  // O nome confirmado não prova chave global: a aprovação de key_evidence continua necessária.
  if (!c.key_fields.includes(tableContracts[c.source_table].documentedKey)) throw new OlosError('CONTRACT_UNCONFIRMED');
  if (contract.columns.length !== Object.keys(c.fields).length ||
      new Set(contract.columns.map(x => x.name.toLowerCase())).size !== contract.columns.length) throw new OlosError('CONTRACT_UNCONFIRMED');
  for (const column of contract.columns) {
    if (sensitiveField(column.name) || !Object.hasOwn(c.fields, column.name) || compatible[column.type] !== c.fields[column.name]?.type) throw new OlosError('CONTRACT_UNCONFIRMED');
    if (column.length !== undefined && (!['varchar','nvarchar','char','nchar'].includes(column.type) ||
      (column.length === 'max' ? !['varchar','nvarchar'].includes(column.type) :
        !Number.isInteger(column.length) || column.length < 1 || column.length > (column.type === 'nvarchar' || column.type === 'nchar' ? 4000 : 8000)))) throw new OlosError('CONTRACT_UNCONFIRMED');
    if (['decimal','numeric'].includes(column.type) && (!Number.isInteger(column.precision) || !Number.isInteger(column.scale) ||
      column.precision! < 1 || column.precision! > 38 || column.scale! < 0 || column.scale! > column.precision!)) throw new OlosError('CONTRACT_UNCONFIRMED');
    if (['datetime2','datetimeoffset'].includes(column.type) && (!Number.isInteger(column.scale) || column.scale! < 0 || column.scale! > 7)) throw new OlosError('CONTRACT_UNCONFIRMED');
  }
}
/** Fronteira anterior a clone/JSON/persistência: nenhuma reconstrução de number/Date perdido. */
export function adaptDataset(dataset: Dataset, contract: ValidatedTableContract, limits: PersistenceLimits = defaultLimits): readonly ExportRecord[] {
  validateTableContract(contract);
  if (canonical(dataset.columns) !== canonical(contract.columns)) throw new OlosError('INVALID_RESPONSE', 'unknown');
  if (dataset.representation !== 'mssql-native' && dataset.representation !== 'exact-text/v1') throw new OlosError('INVALID_RESPONSE', 'unknown');
  if (dataset.representation === 'exact-text/v1' && !dataset.exactEncodingEvidence) throw new OlosError('PRECISION_UNSAFE', 'unknown');
  if (dataset.rows.length > limits.maxRecords) throw new OlosError('LIMIT_EXCEEDED', 'unknown');
  const records: ExportRecord[] = [];
  for (const row of dataset.rows) {
    if (!row || Object.getPrototypeOf(row) !== Object.prototype || Object.keys(row).length !== contract.columns.length ||
      contract.columns.some(column => !Object.hasOwn(row, column.name))) throw new OlosError('INVALID_RESPONSE', 'unknown');
    const record: Record<string, string | boolean | null | Uint8Array> = {};
    for (const column of contract.columns) {
      let value = row[column.name];
      if (value instanceof Date || value === undefined) throw new OlosError('PRECISION_UNSAFE', 'unknown');
      if (value !== null && ['int','smallint','tinyint'].includes(column.type) && typeof value === 'number') {
        const range = column.type === 'tinyint' ? [0,255] : column.type === 'smallint' ? [-32768,32767] : [-2147483648,2147483647];
        if (!Number.isSafeInteger(value) || value < range[0]! || value > range[1]!) throw new OlosError('PRECISION_UNSAFE', 'unknown');
        value = String(value); // somente inteiros de largura comprovada e representáveis exatamente
      }
      if (value !== null && ['int','smallint','tinyint'].includes(column.type) && typeof value === 'string') {
        const [min,max] = column.type === 'tinyint' ? [0n,255n] : column.type === 'smallint' ? [-32768n,32767n] : [-2147483648n,2147483647n];
        if (!/^(0|-?[1-9]\d*)$/.test(value) || BigInt(value) < min! || BigInt(value) > max!) throw new OlosError('PRECISION_UNSAFE', 'unknown');
      }
      if (value !== null && typeof value === 'string' && column.length !== undefined && column.length !== 'max' && value.length > column.length) throw new OlosError('PRECISION_UNSAFE', 'unknown');
      if (typeof value === 'number' || typeof value === 'bigint') throw new OlosError('PRECISION_UNSAFE', 'unknown');
      if (value !== null && (['decimal','numeric','datetime2','datetimeoffset','datetime','smalldatetime'].includes(column.type))) {
        // Driver nativo não certifica DECIMAL/fração/offset; exigir aquisição textual anterior à perda.
        if (dataset.representation !== 'exact-text/v1' || typeof value !== 'string') throw new OlosError('PRECISION_UNSAFE', 'unknown');
        if (column.type === 'decimal' || column.type === 'numeric') {
          const match = /^-?(\d+)(?:\.(\d+))?$/.exec(value);
          if (!match || match[1]!.replace(/^0+/, '').length > column.precision! - column.scale! || (match[2]?.length ?? 0) > column.scale!) throw new OlosError('PRECISION_UNSAFE', 'unknown');
        } else if (column.scale !== undefined && (/\.(\d+)/.exec(value)?.[1]?.length ?? 0) > column.scale) throw new OlosError('PRECISION_UNSAFE', 'unknown');
        if (column.type === 'datetime') {
          const fraction = /\.(\d+)$/.exec(value)?.[1] ?? '';
          // DATETIME é 1/300s, não precisão decimal de 3 casas. .003 é display arredondado,
          // não uma fração exata do tick. Outras frações exigem o codec bruto ainda bloqueado.
          if (value.slice(0,10) < '1753-01-01' || fraction.length > 7 ||
            fraction && BigInt(fraction) * 300n % (10n ** BigInt(fraction.length)) !== 0n) throw new OlosError('PRECISION_UNSAFE', 'unknown');
        }
      }
      if (value !== null && typeof value !== 'string' && typeof value !== 'boolean' && !(value instanceof Uint8Array)) throw new OlosError('INVALID_RESPONSE', 'unknown');
      Object.defineProperty(record, column.name, { value: value instanceof Uint8Array ? new Uint8Array(value) : value, enumerable: true });
    }
    records.push(Object.freeze(record));
  }
  // Valida int64, calendário, offset, null, schema drift, bytes e duplicatas conflitantes com as regras da Fase 2A.
  deduplicate(prepareRecords(records, contract.source, limits).rows);
  return Object.freeze(records);
}
