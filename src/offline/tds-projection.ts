import { sqlDatetimeToCivil } from './sql-datetime.js';
import {
  mapOffline,
  type OfflineRecord,
  type OfflineProfile,
  type Catalogs,
} from './model.js';
import type { SpoolRecord } from '../large-batch/types.js';

const TIME_FIELDS = [
  'StartDate',
  'EndCall',
  'EndWrap',
] as const;

export function projectTdsAttempts(
  source: SpoolRecord,
  catalogs: Catalogs,
  profile: OfflineProfile,
  loadedAt: string,
) {
  const record: Record<string, string | boolean | null> = { ...source };

  for (const field of TIME_FIELDS) {
    const value = record[field];

    if (value === undefined || value === null) continue;

    if (typeof value !== 'string') {
      throw new Error('INVALID_TDS_TIMESTAMP');
    }

    record[field] = sqlDatetimeToCivil(value);
  }

  // A origem original permanece imutável.
  return mapOffline(
    record as OfflineRecord,
    catalogs,
    profile,
    loadedAt,
  );
}
