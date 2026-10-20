import type { ReadonlyCallSource } from './types.js';

export interface PollResult {
  received: number;
  nextCursor: string | null;
}

export class ReadonlyCallPoller {
  constructor(
    private readonly source: ReadonlyCallSource,
    private readonly persist: (
      records: readonly Readonly<
        Record<string, string | boolean | null>
      >[],
      nextCursor: string | null,
    ) => Promise<void>,
  ) {}

  async run(
    cursor: string | null,
    limit: number,
    signal: AbortSignal,
  ): Promise<PollResult> {
    if (signal.aborted) {
      throw new Error('POLL_ABORTED');
    }

    if (!Number.isSafeInteger(limit) || limit < 1 || limit > 200000) {
      throw new Error('INVALID_POLL_LIMIT');
    }

    const batch = await this.source.readAfter(
      cursor,
      limit,
      signal,
    );

    if (signal.aborted) {
      throw new Error('POLL_ABORTED');
    }

    if (
      batch.records.length > limit ||
      (batch.records.length > 0 && batch.nextCursor === null)
    ) {
      throw new Error('INVALID_SOURCE_BATCH');
    }

    // A implementação de persistência deverá confirmar
    // dados e cursor na mesma transação.
    await this.persist(batch.records, batch.nextCursor);

    return {
      received: batch.records.length,
      nextCursor: batch.nextCursor,
    };
  }
}
