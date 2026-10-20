import type { Logger } from 'pino';
import type { Dependency, DependencyName, DependencyStatus } from '../db/dependency.js';

// Um laço por dependência; probes nunca se sobrepõem. Backoff limitado com jitter.
export class DependencyMonitor {
  private stopped = false;
  private started = false;
  private timers = new Map<DependencyName, ReturnType<typeof setTimeout>>();
  private inFlight = new Set<Promise<void>>();
  private states: Record<DependencyName, DependencyStatus>;

  constructor(private readonly dependencies: Dependency[], private readonly logger: Logger,
    private readonly intervalMs: number, private readonly retryMaxMs: number) {
    this.states = {
      olos: { state: 'unconfigured', checkedAt: null, consecutiveFailures: 0 },
      postgres: { state: 'unconfigured', checkedAt: null, consecutiveFailures: 0 },
    };
    for (const d of dependencies) this.states[d.name].state = d.configured ? 'unknown' : 'unconfigured';
  }

  snapshot(): Record<DependencyName, DependencyStatus> { return structuredClone(this.states); }

  start(): void {
    if (this.started || this.stopped) return;
    this.started = true;
    for (const dependency of this.dependencies) if (dependency.configured) this.launch(dependency);
  }

  private launch(dependency: Dependency): void {
    const task = this.check(dependency);
    this.inFlight.add(task);
    void task.finally(() => this.inFlight.delete(task));
  }

  private async check(dependency: Dependency): Promise<void> {
    const status = this.states[dependency.name];
    const previous = status.state;
    try {
      await dependency.probe();
      status.state = 'up';
      status.consecutiveFailures = 0;
    } catch {
      status.state = 'down';
      status.consecutiveFailures += 1;
    }
    status.checkedAt = new Date().toISOString();
    if (previous !== status.state) this.logger.info({ dependency: dependency.name, state: status.state, event: 'dependency_state' }, 'Estado da dependência');
    if (this.stopped) return;
    const backoff = Math.min(this.retryMaxMs, this.intervalMs * 2 ** Math.min(status.consecutiveFailures, 16));
    const delay = status.consecutiveFailures ? Math.min(this.retryMaxMs, Math.round(backoff * (0.8 + Math.random() * 0.2))) : this.intervalMs;
    this.timers.set(dependency.name, setTimeout(() => this.launch(dependency), delay));
  }

  async stop(): Promise<void> {
    this.stopped = true;
    for (const timer of this.timers.values()) clearTimeout(timer);
    this.timers.clear();
    await Promise.allSettled(this.inFlight);
    const results = await Promise.allSettled(this.dependencies.map(d => d.close()));
    for (const status of Object.values(this.states)) status.state = 'stopped';
    if (results.some(r => r.status === 'rejected')) throw new Error('Dependency shutdown failed');
  }
}
