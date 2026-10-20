import { createHash } from 'node:crypto';
import type { ExportTable } from '../sync/contracts.js';
import { exportTables } from '../sync/contracts.js';
import { PersistenceError, type SourceContract, type PersistenceBatch, type PersistenceLimits } from './types.js';
import { legacyDatetimeFromTdsBody } from '../olos/exact-value-prototypes.js';

export const targets: Record<ExportTable, string> = Object.freeze({
  AttemptsRawData: 'olos_export.attempts_raw_data', InboundRawData: 'olos_export.inbound_raw_data',
  PbxBillingData: 'olos_export.pbx_billing_data', Users: 'olos_export.users', Campaign: 'olos_export.campaign', Disposition: 'olos_export.disposition',
});
function fail(): never { throw new PersistenceError('VALIDATION_FAILED'); }
export function assertTable(value: unknown): asserts value is ExportTable { if (!exportTables.includes(value as ExportTable)) fail(); }
export function assertUuid(value: string): void { if (typeof value !== 'string' || !/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(value)) fail(); }
export function assertVersion(value: string): void { if (typeof value !== 'string' || !/^(0|[1-9]\d*)$/.test(value) || BigInt(value) > 9223372036854775807n) fail(); }
export function validDate(value: string): boolean {
  const match = /^(\d{4})-(\d{2})-(\d{2})$/.exec(value);
  if (!match) return false;
  const [, y, m, d] = match;
  const year = Number(y), month = Number(m), day = Number(d);
  const days = [31, year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0) ? 29 : 28, 31,30,31,30,31,31,30,31,30,31];
  return year >= 1 && month >= 1 && month <= 12 && day >= 1 && day <= (days[month - 1] ?? 0);
}
function timestamp(value: string, offset: boolean): boolean {
  const match = /^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.\d{1,7})?(Z|[+-]\d{2}:\d{2})?$/.exec(value);
  if (!match || !validDate(match[1]!) || Number(match[2]) > 23 || Number(match[3]) > 59 || Number(match[4]) > 59 || Boolean(match[5]) !== offset) return false;
  const zone = match[5];
  return !zone || zone === 'Z' || (Number(zone.slice(1,3)) <= 14 && Number(zone.slice(4,6)) <= 59 && (Number(zone.slice(1,3)) !== 14 || zone.endsWith(':00')));
}
export function canonical(value: unknown): string {
  if (Array.isArray(value)) return `[${value.map(canonical).join(',')}]`;
  if (value !== null && typeof value === 'object') return `{${Object.entries(value).sort(([a], [b]) => a < b ? -1 : a > b ? 1 : 0).map(([k,v]) => `${JSON.stringify(k)}:${canonical(v)}`).join(',')}}`;
  return JSON.stringify(value) ?? fail();
}
export const digest = (value: unknown): string => createHash('sha256').update(canonical(value)).digest('hex');
const sensitive = (key: string) => /password|passwd|senha/.test(key.toLowerCase().replace(/[^a-z]/g, ''));
export function assertContract(c: SourceContract): void {
  if (!c.approved || !c.layout_evidence || !c.key_evidence || c.key_fields.length === 0) throw new PersistenceError('CONTRACT_UNCONFIRMED');
  if (new Set(c.key_fields).size !== c.key_fields.length || c.key_fields.some(k => !Object.hasOwn(c.fields, k) || c.fields[k]?.nullable)) fail();
  for (const [key, field] of Object.entries(c.fields)) {
    if (sensitive(key) || !field || typeof field.nullable !== 'boolean' || !['text','int64','decimal','local_timestamp','offset_timestamp','sql_datetime_ticks','boolean','bytes'].includes(field.type)) fail();
  }
}
export interface PreparedRecord { readonly key: string; readonly payload: Readonly<Record<string, string | boolean | null>>; }
export function prepareRecords(records: PersistenceBatch['records'], c: SourceContract, limits: PersistenceLimits): { rows: PreparedRecord[]; bytes: number } {
  assertContract(c);
  if (records.length > limits.maxRecords) fail();
  const result: PreparedRecord[] = [];
  let bytes = 0;
  for (const input of records) {
    if (!input || typeof input !== 'object' || Array.isArray(input) || Object.getPrototypeOf(input) !== Object.prototype) fail();
    if (Object.keys(input).some(k => sensitive(k) || !Object.hasOwn(c.fields, k)) || Object.keys(c.fields).some(k => !Object.hasOwn(input, k))) fail();
    const payload: Record<string, string | boolean | null> = Object.create(null) as Record<string, string | boolean | null>;
    for (const [key, field] of Object.entries(c.fields)) {
      const value = input[key];
      if (value === null) { if (!field.nullable) fail(); payload[key] = null; continue; }
      if (field.type === 'boolean') { if (typeof value !== 'boolean') fail(); payload[key] = value; continue; }
      if (field.type === 'bytes') { if (!(value instanceof Uint8Array)) fail(); payload[key] = Buffer.from(value).toString('base64'); continue; }
      if (typeof value !== 'string') fail();
      if (field.type === 'int64' && (!/^-?(0|[1-9]\d*)$/.test(value) || BigInt(value) < -9223372036854775808n || BigInt(value) > 9223372036854775807n || BigInt(value).toString() !== value)) fail();
      if (field.type === 'decimal' && !/^-?\d+(\.\d+)?$/.test(value)) fail();
      if (field.type === 'local_timestamp' && !timestamp(value, false)) fail();
      if (field.type === 'offset_timestamp' && !timestamp(value, true)) fail();
      if (field.type === 'sql_datetime_ticks') {
        if (!/^sql-datetime\/v1:[0-9a-f]{16}$/.test(value)) fail();
        try { legacyDatetimeFromTdsBody(Buffer.from(value.slice('sql-datetime/v1:'.length),'hex')); } catch { fail(); }
      }
      payload[key] = value;
    }
    const row = { key: canonical(c.key_fields.map(k => [k, payload[k]])), payload };
    bytes += Buffer.byteLength(canonical(row));
    if (bytes > limits.maxBytes) fail();
    result.push(row);
  }
  return { rows: result, bytes };
}
export function batchDigest(batch: PersistenceBatch, rows: readonly PreparedRecord[], contractVersion: string): string {
  return digest({ id: batch.id, expected: batch.expected, nextCursor: batch.nextCursor, records: rows, contractVersion });
}
export function deduplicate(rows: readonly PreparedRecord[]): PreparedRecord[] {
  const seen = new Map<string, PreparedRecord>();
  for (const row of rows) {
    const prior = seen.get(row.key);
    if (prior && canonical(prior.payload) !== canonical(row.payload)) throw new PersistenceError('VALIDATION_FAILED');
    seen.set(row.key, row);
  }
  return [...seen.values()];
}
