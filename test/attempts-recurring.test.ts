import { describe, expect, it, vi } from 'vitest';
import { createAttemptsRecurringRunner } from '../src/integration/attempts/recurring.js';
import type { LargeBatchCoordinator } from '../src/large-batch/coordinator.js';
import type { LargePlan, RawSource } from '../src/large-batch/types.js';

describe('Attempts recurring integration', () => {
  it('cria uma nova fonte em cada ciclo concluído', async () => {
    vi.useFakeTimers();

    try {
      const receive = vi.fn().mockResolvedValue({
        received: 10,
        persisted: 10,
        duplicates: 0,
        rejected: 0,
      });

      const plan = vi.fn(() => ({}) as LargePlan);
      const source = vi.fn(() => ({}) as RawSource);

      const runner = createAttemptsRecurringRunner({
        coordinator: { receive } as unknown as LargeBatchCoordinator,
        plan,
        source,
      }, 60_000);

      runner.start();

      await vi.advanceTimersByTimeAsync(120_000);

      expect(receive).toHaveBeenCalledTimes(2);
      expect(source).toHaveBeenCalledTimes(2);
      expect(plan).toHaveBeenCalledTimes(2);

      await runner.stop();
    } finally {
      vi.useRealTimers();
    }
  });

  it('interrompe aquisições após falha do coordenador', async () => {
    vi.useFakeTimers();

    try {
      const receive = vi.fn().mockRejectedValue(
        new Error('RECOVERY_REQUIRED'),
      );

      const runner = createAttemptsRecurringRunner({
        coordinator: { receive } as unknown as LargeBatchCoordinator,
        plan: () => ({}) as LargePlan,
        source: () => ({}) as RawSource,
      }, 60_000);

      runner.start();

      await vi.advanceTimersByTimeAsync(180_000);

      expect(receive).toHaveBeenCalledTimes(1);
      expect(runner.status).toBe('failed');

      await runner.stop();
    } finally {
      vi.useRealTimers();
    }
  });
});
