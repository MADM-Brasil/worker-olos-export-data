import { validDate } from '../persistence/validation.js';
import { OlosError } from './types.js';

/** DATA CIVIL exclusivamente; não aceita Date, offset, locale nem timestamp. */
export function dateParameter(value: unknown): string {
  if (typeof value !== 'string' || !validDate(value)) throw new OlosError('INVALID_PARAMETER');
  return value;
}
/** Adaptação somente de DATE de entrada para a API do driver; timestamps de resposta continuam proibidos como Date. */
export function utcDriverDate(value: string): Date {
  dateParameter(value);
  const [year,month,day] = value.split('-').map(Number);
  const date = new Date(0);
  date.setUTCFullYear(year!,month! - 1,day!);
  date.setUTCHours(0,0,0,0);
  return date;
}
