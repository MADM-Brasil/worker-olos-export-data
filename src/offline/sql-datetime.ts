import { legacyDatetimeFromTdsBody } from '../olos/exact-value-prototypes.js';

const TICKS_PER_DAY = 86_400 * 300;
const MICROS_PER_DAY = 86_400_000_000n;

export function sqlDatetimeToCivil(value: string | null): string | null {
  if (value === null) return null;

  if (!/^sql-datetime\/v1:[0-9a-f]{16}$/.test(value)) {
    throw new Error('INVALID_SQL_DATETIME');
  }

  const bytes = Buffer.from(value.slice('sql-datetime/v1:'.length), 'hex');
  const exact = legacyDatetimeFromTdsBody(bytes);

  if (!exact) throw new Error('INVALID_SQL_DATETIME');

  const days = Number(exact.daysSince1900);
  const ticks = Number(exact.ticks300);

  if (!Number.isSafeInteger(days) || !Number.isSafeInteger(ticks) ||
      ticks < 0 || ticks >= TICKS_PER_DAY) {
    throw new Error('INVALID_SQL_DATETIME');
  }

  // SQL DATETIME: 300 ticks por segundo.
  // Arredondamento para microssegundo mais proximo, half-up.
  const micros = (BigInt(ticks) * 1_000_000n + 150n) / 300n;

  const extraDay = micros / MICROS_PER_DAY;
  const microsInDay = micros % MICROS_PER_DAY;

  const civilDay = new Date(Date.UTC(1900, 0, 1));
  civilDay.setUTCDate(civilDay.getUTCDate() + days + Number(extraDay));

  const yyyy = civilDay.getUTCFullYear().toString().padStart(4, '0');
  const mm = (civilDay.getUTCMonth() + 1).toString().padStart(2, '0');
  const dd = civilDay.getUTCDate().toString().padStart(2, '0');

  const hours = microsInDay / 3_600_000_000n;
  const minutes = (microsInDay / 60_000_000n) % 60n;
  const seconds = (microsInDay / 1_000_000n) % 60n;
  const fraction = microsInDay % 1_000_000n;

  const two = (n: bigint) => n.toString().padStart(2, '0');

  return `${yyyy}-${mm}-${dd}T${two(hours)}:${two(minutes)}:${two(seconds)}.${fraction.toString().padStart(6, '0')}`;
}
