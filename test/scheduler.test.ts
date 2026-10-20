import { afterEach, describe, expect, it, vi } from 'vitest';
import { Scheduler } from '../src/sync/scheduler.js';

afterEach(() => { vi.useRealTimers(); });
describe('scheduler futuro', () => {
  it('aguarda 60s, impede sobreposição e drena execução no stop', async () => {
    vi.useFakeTimers();
    let finish!: () => void;
    let signal!: AbortSignal;
    const task = vi.fn((s: AbortSignal) => { signal = s; return new Promise<void>(resolve => { finish = resolve; }); });
    const scheduler = new Scheduler(task, vi.fn());
    scheduler.start(); scheduler.start();
    await vi.advanceTimersByTimeAsync(59_999);
    expect(task).not.toHaveBeenCalled();
    await vi.advanceTimersByTimeAsync(1);
    await vi.advanceTimersByTimeAsync(180_000);
    expect(task).toHaveBeenCalledTimes(1);
    const stopping = scheduler.stop();
    expect(signal.aborted).toBe(true);
    finish();
    await stopping;
    await vi.advanceTimersByTimeAsync(120_000);
    expect(task).toHaveBeenCalledTimes(1);
  });
  it('recupera falha sem rejeição não tratada e respeita próximo intervalo', async () => {
    vi.useFakeTimers();
    const failed = vi.fn();
    const task = vi.fn(async () => { throw new Error('failure'); });
    const scheduler = new Scheduler(task, failed);
    scheduler.start();
    await vi.advanceTimersByTimeAsync(120_000);
    expect(task).toHaveBeenCalledTimes(2);
    expect(failed).toHaveBeenCalledTimes(2);
    await scheduler.stop();
  });
});
