import { legacyDatetimeFromTdsBody, decimalFromTdsBody } from '../olos/exact-value-prototypes.js';
import { sensitiveField, tableContracts } from '../olos/contracts.js';
import { assertContract, assertTable, prepareRecords } from '../persistence/validation.js';
import { defaultLimits } from '../persistence/types.js';
import { prepareCall } from '../olos/client.js';
import { SpoolError, type LargePlan, type SpoolRecord } from './types.js';

export function validateLargePlan(plan: LargePlan): void {
  const recovery = plan.signature.procedure === 'ReReadData' && plan.run.table === 'AttemptsRawData' && plan.run.readType === 'reread' &&
    plan.run.exportDate === plan.parameters.DateExport && plan.run.pkRange?.start === plan.parameters.PKIDStart && plan.run.pkRange?.end === plan.parameters.PKIDEnd &&
    typeof plan.parameters.PKIDStart === 'string' && typeof plan.parameters.PKIDEnd === 'string' &&
    /^-?(0|[1-9]\d*)$/.test(plan.parameters.PKIDStart) && /^-?(0|[1-9]\d*)$/.test(plan.parameters.PKIDEnd) &&
    BigInt(plan.parameters.PKIDStart) <= BigInt(plan.parameters.PKIDEnd);
  assertTable(plan.run.table);
  assertContract(plan.contract);
  if (!plan.acquisitionEvidence || !plan.responseEvidence || !plan.partitionEvidence ||
      plan.run.table !== plan.contract.source_table || plan.run.contractVersion !== plan.contract.version ||
      !plan.contract.key_fields.includes(tableContracts[plan.run.table].documentedKey) ||
      !plan.successReturnValues.length || plan.successReturnValues.some(v => !Number.isSafeInteger(v)) ||
      !['ReadData','ReadDataCurrentDay','ReadDataPreviousDays'].includes(plan.signature.procedure) && !recovery) throw new SpoolError('INVALID_VALUE');
  prepareCall(plan.signature, plan.parameters, 5000);
  const visible = plan.fields.filter(f => !sensitiveField(f.name));
  if (new Set(plan.fields.map(f => f.name.toLowerCase())).size !== plan.fields.length ||
      visible.length !== Object.keys(plan.contract.fields).length) throw new SpoolError('INVALID_VALUE');
  for (const f of visible) {
    if (!['datetime','int','bigint','decimal','varchar','nvarchar','char'].includes(f.type)) throw new SpoolError('INVALID_VALUE');
    const expected = f.type === 'datetime' ? 'sql_datetime_ticks' : f.type === 'int' || f.type === 'bigint' ? 'int64' : f.type === 'decimal' ? 'decimal' : 'text';
    if (plan.contract.fields[f.name]?.type !== expected || plan.contract.fields[f.name]?.nullable !== f.nullable ||
      (['varchar','nvarchar','char'].includes(f.type) && (!Number.isSafeInteger(f.maxBytes) || f.maxBytes! < 1)) ||
      (['varchar','char'].includes(f.type) && !['ascii','utf-8','windows-1252'].includes(f.encoding ?? ''))) throw new SpoolError('INVALID_VALUE');
    if (f.type === 'nvarchar' && f.maxBytes! % 2 !== 0) throw new SpoolError('INVALID_VALUE');
    if (f.type === 'decimal') decimalFromTdsBody(null,f.precision!,f.scale!);
  }
}
/** Recebe corpos binários ANTES de readValue/Tedious. Nenhuma conversão a partir de Date ou number recebido. */
export function decodeWireRow(plan: LargePlan, values: readonly (Uint8Array | null)[], maxBytes = 1024 ** 2): SpoolRecord {
  if (values.length !== plan.fields.length) throw new SpoolError('INVALID_VALUE');
  let total = 0;
  for (const body of values) {
    if (body !== null && !(body instanceof Uint8Array)) throw new SpoolError('INVALID_VALUE');
    total += body?.byteLength ?? 0;
    if (total > maxBytes) throw new SpoolError('QUOTA_EXCEEDED');
  }
  const result: Record<string,string | null> = {};
  for (let i = 0; i < values.length; i++) {
    const f = plan.fields[i]!, body = values[i];
    if (sensitiveField(f.name)) continue; // nem a senha nem sua forma binária entram no spool
    if (body === null) { if (!f.nullable) throw new SpoolError('INVALID_VALUE'); Object.defineProperty(result,f.name,{ value: null, enumerable: true }); continue; }
    if (!(body instanceof Uint8Array)) throw new SpoolError('INVALID_VALUE');
    const bytes = Buffer.from(body.buffer, body.byteOffset, body.byteLength);
    let value: string;
    switch (f.type) {
      case 'int': if (bytes.length !== 4) throw new SpoolError('INVALID_VALUE'); value = String(bytes.readInt32LE()); break;
      case 'bigint': if (bytes.length !== 8) throw new SpoolError('INVALID_VALUE'); value = bytes.readBigInt64LE().toString(); break;
      case 'datetime': {
        const exact = legacyDatetimeFromTdsBody(bytes)!;
        value = `sql-datetime/v1:${exact.rawHex}`; break;
      }
      case 'decimal': value = decimalFromTdsBody(bytes,f.precision!,f.scale!)!; break;
      case 'varchar': case 'nvarchar': case 'char': {
        if (bytes.length > f.maxBytes!) throw new SpoolError('INVALID_VALUE');
        if (f.type === 'nvarchar' && bytes.length % 2 !== 0) throw new SpoolError('INVALID_VALUE');
        const encoding = f.type === 'nvarchar' ? 'utf-16le' : f.encoding!;
        if (encoding === 'ascii' && bytes.some(b => b > 127)) throw new SpoolError('INVALID_VALUE');
        try { value = new TextDecoder(encoding === 'ascii' ? 'utf-8' : encoding,{ fatal: true, ignoreBOM: true }).decode(bytes); }
        catch { throw new SpoolError('INVALID_VALUE'); }
        break;
      }
      default: throw new SpoolError('INVALID_VALUE');
    }
    Object.defineProperty(result,f.name,{ value, enumerable: true });
  }
  prepareRecords([result],plan.contract,{ ...defaultLimits, maxRecords: 1, maxBytes: 16 * 1024 ** 2 });
  return result;
}
