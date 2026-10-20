import type { SpoolRecord } from '../../large-batch/types.js';

export interface ReadonlyCallBatch {
  records: readonly SpoolRecord[];
  nextCursor: string | null;
}

export interface ReadonlyCallSource {
  readonly sourceName: string;

  readAfter(
    cursor: string | null,
    limit: number,
    signal: AbortSignal,
  ): Promise<ReadonlyCallBatch>;
}
