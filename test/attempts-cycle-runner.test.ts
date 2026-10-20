import { describe, expect, it, vi } from 'vitest';
import { AttemptsCycleRunner } from '../src/integration/attempts/cycle-runner.js';

describe('AttemptsCycleRunner', () => {
  it('executa ciclos sucessivos sem sobreposição', async () => {
    vi.useFakeTimers();

    try {
      let release!: () => void;
      let active = 0;
      let maximum = 0;
      let executions = 0;

      const runner = new AttemptsCycleRunner(async () => {
        executions++;
        active++;
        maximum = Math.max(maximum, active);

        if (executions === 1) {
          await new Promise<void>(resolve => {
            release = resolve;
          });
        }

        active--;
      }, 60_000);

      runner.start();

      await vi.advanceTimersByTimeAsync(60_000);
      expect(executions).toBe(1);

      await vi.advanceTimersByTimeAsync(180_000);
      expect(executions).toBe(1);

      release();
      await Promise.resolve();

      await vi.advanceTimersByTimeAsync(60_000);

      expect(executions).toBe(2);
      expect(maximum).toBe(1);

      await runner.stop();
    } finally {
      vi.useRealTimers();
    }
  });

  it('bloqueia novas aquisições após falha', async () => {
    vi.useFakeTimers();

    try {
      let executions = 0;

      const runner = new AttemptsCycleRunner(async () => {
        executions++;
        throw new Error('SOURCE_INTERRUPTED');
      }, 60_000);

      runner.start();

      await vi.advanceTimersByTimeAsync(60_000);

      expect(executions).toBe(1);
      expect(runner.status).toBe('failed');

      await vi.advanceTimersByTimeAsync(180_000);

      expect(executions).toBe(1);
      expect(() => runner.start()).toThrow('ATTEMPTS_CYCLE_STOPPED');

      await runner.stop();
    } finally {
      vi.useRealTimers();
    }
  });
});
