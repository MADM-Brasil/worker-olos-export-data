import {z} from 'zod';

/** The administrative Attempts authorization cannot select another source. */
export const expiredRunSelectionSchema=z.object({
  runId:z.string().uuid(),source:z.literal('AttemptsRawData'),
  expectedGeneration:z.string().regex(/^[1-9][0-9]{0,18}$/)
    .refine(value=>/^[1-9][0-9]{0,18}$/.test(value) && BigInt(value)<9223372036854775807n)
}).strict();
export type ExpiredRunSelection=z.infer<typeof expiredRunSelectionSchema>;
