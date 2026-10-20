import { Scheduler } from '../../sync/scheduler.js';

export class AttemptsCycleRunner {
  private readonly scheduler: Scheduler;
  private stopped = false;
  private failed = false;

  constructor(
    private readonly cycle: (signal: AbortSignal) => Promise<void>,
    intervalMs = 60_000,
  ) {
    this.scheduler = new Scheduler(
      async signal => {
        if (this.stopped || this.failed) return;
        await this.cycle(signal);
      },
      () => {
        this.failed = true;
        void this.scheduler.stop();
      },
      intervalMs,
    );
  }

  start(): void {
    if (this.stopped || this.failed) {
      throw new Error('ATTEMPTS_CYCLE_STOPPED');
    }

    this.scheduler.start();
  }

  async stop(): Promise<void> {
    this.stopped = true;
    await this.scheduler.stop();
  }

  get status(): 'running' | 'failed' | 'stopped' {
    if (this.failed) return 'failed';
    return this.stopped ? 'stopped' : 'running';
  }
}
