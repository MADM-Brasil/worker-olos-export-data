import { describe, expect, it, vi } from 'vitest';
import { AttemptsCycleRunner } from '../src/integration/attempts/cycle-runner.js';

describe('AttemptsCycleRunner', () => {
  it('legacy callback starts the 120-second interval after delivery, not after acquisition alone', async () => {
    vi.useFakeTimers();
    let finishDelivery!: () => void;
    let acquisitions = 0;
    const runner = new AttemptsCycleRunner(async () => {
      acquisitions++;
      if (acquisitions === 1) await new Promise<void>(resolve => { finishDelivery = resolve; });
    }, 120_000);
    try {
      runner.start();
      await vi.advanceTimersByTimeAsync(120_000);
      expect(acquisitions).toBe(1);
      await vi.advanceTimersByTimeAsync(600_000);
      expect(acquisitions).toBe(1);
      expect(runner.nextExecutionAt).toBeNull();
      finishDelivery();
      await vi.advanceTimersByTimeAsync(0);
      await vi.advanceTimersByTimeAsync(119_999);
      expect(acquisitions).toBe(1);
      await vi.advanceTimersByTimeAsync(1);
      expect(acquisitions).toBe(2);
    } finally { finishDelivery?.(); await runner.stop(); vi.useRealTimers(); }
  });

  it('acquisition-only callback schedules 120 seconds after durable handoff while the delivery remains pending',async()=>{
    vi.useFakeTimers();let finishCapture!:()=>void,calls=0,queued=0;
    const runner=new AttemptsCycleRunner(async()=>{calls++;await new Promise<void>(r=>{finishCapture=r;});queued++;},120000);
    try{runner.start();await vi.advanceTimersByTimeAsync(120000);expect(calls).toBe(1);
      await vi.advanceTimersByTimeAsync(500000);expect(calls).toBe(1);expect(queued).toBe(0);
      finishCapture();await vi.advanceTimersByTimeAsync(0);expect(queued).toBe(1);
      await vi.advanceTimersByTimeAsync(119999);expect(calls).toBe(1);
      await vi.advanceTimersByTimeAsync(1);expect(calls).toBe(2);expect(queued).toBe(1);
    }finally{finishCapture?.();await runner.stop();vi.useRealTimers();}
  });

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
