import { legacyDatetimeFromTdsBody } from '../../olos/exact-value-prototypes.js';
import { canonical, digest, validDate, assertUuid } from '../../persistence/validation.js';
import type { LargePlan, SpoolManifest, SpoolRecord } from '../../large-batch/types.js';
import { signature } from './contracts.js';

export interface LogInterval { table: 'AttemptsRawData'; key: 'AttemptsRawDataID'; date: string; start: string; end: string; quantity: number; dateRead: string; }
export interface AttemptsRecoveryPlan {
  version: 'attempts-recovery/v1'; scope: 'isolated-fixture' | 'corporate-prepared-blocked'; caseId: string; originDigest: string; logDigest: string;
  interval: LogInterval; operator: string; approvalReference: string; ownershipEvidence: string; completenessEvidence: string;
}
export function decodeLogRecord(record: SpoolRecord): LogInterval {
  const names = ['TableName','PKColumnName','DateExport','PKIDStart','PKIDEnd','DateRead','Quantity'];
  if (Object.keys(record).length !== names.length || names.some(n => typeof record[n] !== 'string' || !/^(?:[a-f0-9]{2})*$/.test(record[n] as string))) throw new Error('LOG_LAYOUT_REJECTED');
  const bytes = (name: string, length?: number) => { const b = Buffer.from(record[name] as string,'hex'); if (length !== undefined && b.length !== length) throw new Error('LOG_LAYOUT_REJECTED'); return b; };
  if (!bytes('TableName').equals(Buffer.from('AttemptsRawData','ascii')) || !bytes('PKColumnName').equals(Buffer.from('AttemptsRawDataID','ascii'))) throw new Error('LOG_IDENTITY_REJECTED');
  const days = bytes('DateExport',3).readUIntLE(0,3);
  if (days > 3652058) throw new Error('LOG_DATE_REJECTED');
  // DATE has integral civil days, unlike DATETIME's fractional ticks; this arithmetic is exact.
  const date = new Date(-62135596800000+days*86400000).toISOString().slice(0,10);
  const start = bytes('PKIDStart',8).readBigInt64LE(), end = bytes('PKIDEnd',8).readBigInt64LE(), quantity = bytes('Quantity',4).readInt32LE();
  if (start > end || quantity < 0 || quantity > 200000 || BigInt(quantity) > end-start+1n) throw new Error('LOG_RANGE_REJECTED');
  return { table: 'AttemptsRawData',key: 'AttemptsRawDataID',date,start: start.toString(),end: end.toString(),quantity,
    dateRead: `sql-datetime/v1:${legacyDatetimeFromTdsBody(bytes('DateRead',8))!.rawHex}` };
}
function reviewAttemptsRecovery(scope: AttemptsRecoveryPlan['scope'], origin: SpoolManifest, logDigest: string, intervals: readonly LogInterval[], selection: { date: string; start: string; end: string },
  review: { operator: string; approvalReference: string; ownershipEvidence: string; completenessEvidence: string }): AttemptsRecoveryPlan {
  assertUuid(origin.identity.batchId);
  if (origin.sealed || origin.state !== 'recovery_required' || origin.identity.plan.run.table !== 'AttemptsRawData' || !validDate(selection.date) || selection.date !== origin.identity.plan.run.exportDate ||
    !/^[a-f0-9]{64}$/.test(logDigest) || !/^-?(0|[1-9]\d*)$/.test(selection.start) || !/^-?(0|[1-9]\d*)$/.test(selection.end) ||
    [review.operator,review.approvalReference,review.ownershipEvidence,review.completenessEvidence].some(v => typeof v !== 'string' || !v || v.length > 200) || intervals.length > 10000) throw new Error('RECOVERY_PLAN_REJECTED');
  const start = BigInt(selection.start), end = BigInt(selection.end);
  // The fixture implements the manual's INT extraction PK; Log/ReRead parameters remain exact BIGINT.
  if (start < -2147483648n || end > 2147483647n || start > end) throw new Error('RECOVERY_PK_CONTRACT_REJECTED');
  const overlap = intervals.filter(i => i.date === selection.date && BigInt(i.start) <= end && BigInt(i.end) >= start);
  const chosen = overlap[0];
  if (overlap.length !== 1 || !chosen || chosen.table !== 'AttemptsRawData' || chosen.key !== 'AttemptsRawDataID' || !Number.isSafeInteger(chosen.quantity) || chosen.start !== selection.start || chosen.end !== selection.end || chosen.quantity < 1 || chosen.quantity > 200000) throw new Error('RECOVERY_LOG_AMBIGUOUS');
  return { version: 'attempts-recovery/v1',scope,caseId: origin.identity.batchId,originDigest: digest(origin),logDigest,
    interval: structuredClone(chosen),operator: review.operator,approvalReference: review.approvalReference,
    ownershipEvidence: review.ownershipEvidence,completenessEvidence: review.completenessEvidence };
}
/** Existing fixture entrypoint retains its scope; never becomes a corporate permit. */
export function planAttemptsRecovery(...args: Parameters<typeof reviewAttemptsRecovery> extends [unknown,...infer Rest] ? Rest : never): AttemptsRecoveryPlan {
  return reviewAttemptsRecovery('isolated-fixture',...args);
}
/** Same range/digest/review checks, but explicitly non-executable corporate preparation. */
export function planCorporateAttemptsRecovery(...args: Parameters<typeof planAttemptsRecovery>): AttemptsRecoveryPlan {
  return reviewAttemptsRecovery('corporate-prepared-blocked',...args);
}
export function rereadLargePlan(original: SpoolManifest, recovery: AttemptsRecoveryPlan): LargePlan {
  if (recovery.scope !== 'isolated-fixture' || recovery.caseId !== original.identity.batchId || recovery.originDigest !== digest(original) || original.sealed || original.state !== 'recovery_required') throw new Error('RECOVERY_CASE_MISMATCH');
  const i = recovery.interval;
  return { ...structuredClone(original.identity.plan),signature: signature('ReReadData'),
    run: { ...original.identity.plan.run,readType: 'reread',pkRange: { start: i.start,end: i.end } },
    parameters: { TableName: 'AttemptsRawData',DateExport: i.date,PKIDStart: i.start,PKIDEnd: i.end },
    partitionEvidence: canonical({ caseId: recovery.caseId,logDigest: recovery.logDigest,reviewDigest: digest(recovery),date: i.date }),
    acquisitionEvidence: 'ISOLATED FIXTURE ONLY: approved ReReadData recovery; same exact TDS acquisition' };
}
