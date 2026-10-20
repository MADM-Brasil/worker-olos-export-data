import { afterEach, describe, expect, it, vi } from 'vitest';
import { DependencyMonitor } from '../src/health/monitor.js';
import { createLogger } from '../src/logging.js';
import { loadConfig } from '../src/config/env.js';

afterEach(() => vi.useRealTimers());
describe('monitor de dependências', () => {
  it('aplica backoff, recupera e cancela novas tentativas no encerramento', async () => {
    vi.useFakeTimers();
    const probe = vi.fn().mockRejectedValueOnce(new Error('offline')).mockResolvedValue(undefined);
    const close = vi.fn(async () => {});
    const monitor = new DependencyMonitor([{ name: 'olos', configured: true, probe, close }],
      createLogger(loadConfig({ LOG_LEVEL: 'silent' })), 1000, 8000);
    monitor.start(); monitor.start();
    await vi.advanceTimersByTimeAsync(0);
    expect(monitor.snapshot().olos).toMatchObject({ state: 'down', consecutiveFailures: 1 });
    await vi.advanceTimersByTimeAsync(1500);
    expect(probe).toHaveBeenCalledTimes(1);
    await vi.advanceTimersByTimeAsync(500);
    expect(probe).toHaveBeenCalledTimes(2);
    expect(monitor.snapshot().olos.state).toBe('up');
    await monitor.stop();
    await vi.advanceTimersByTimeAsync(10000);
    expect(probe).toHaveBeenCalledTimes(2);
    expect(close).toHaveBeenCalledTimes(1);
    expect(monitor.snapshot().olos.state).toBe('stopped');
  });
  it('não tenta conectar dependência sem credenciais', async () => {
    const probe = vi.fn();
    const monitor = new DependencyMonitor([{ name: 'postgres', configured: false, probe, close: async () => {} }],
      createLogger(loadConfig({ LOG_LEVEL: 'silent' })), 1000, 8000);
    monitor.start();
    expect(probe).not.toHaveBeenCalled();
    expect(monitor.snapshot().postgres.state).toBe('unconfigured');
    await monitor.stop();
  });
});
