import { z } from 'zod';
import { SpoolError, type SpoolLimits } from './types.js';
const schema = z.object({
  maxRows: z.number().int().min(1).max(200_000).default(200_000),
  maxDiskBytes: z.number().int().min(4096).max(16 * 1024 ** 3).default(512 * 1024 ** 2),
  minFreeBytes: z.number().int().min(0).max(16 * 1024 ** 3).default(64 * 1024 ** 2),
  bufferBytes: z.number().int().min(1024).max(1024 ** 2).default(64 * 1024),
  maxRecordBytes: z.number().int().min(64).max(8 * 1024 ** 2).default(1024 ** 2),
  syncEveryBytes: z.number().int().min(1024).max(64 * 1024 ** 2).default(1024 ** 2),
  chunkRows: z.number().int().min(1).max(2000).default(500),
  chunkBytes: z.number().int().min(1024).max(8 * 1024 ** 2).default(2 * 1024 ** 2),
  readTimeoutMs: z.number().int().min(1).max(3_600_000).default(300_000),
}).strict().refine(v => v.maxRecordBytes <= v.chunkBytes && v.bufferBytes <= v.maxDiskBytes);
export function spoolLimits(input: Partial<SpoolLimits> = {}): SpoolLimits {
  const r = schema.safeParse(input);
  if (!r.success) throw new SpoolError('INVALID_CONFIG');
  return Object.freeze(r.data);
}
