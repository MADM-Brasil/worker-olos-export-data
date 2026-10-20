// Estrutura testável para a Fase 2. Não instanciada pelo aplicativo da Fase 1.
export class Scheduler {
  private timer: ReturnType<typeof setTimeout> | undefined;
  private inFlight: Promise<void> | undefined;
  private stopped = true;
  private readonly abort = new AbortController();

  constructor(private readonly task: (signal: AbortSignal) => Promise<void>,
    private readonly onFailure: () => void, private readonly intervalMs = 60_000) {
    if (!Number.isSafeInteger(intervalMs) || intervalMs < 1) throw new Error('Invalid interval');
  }
  start(): void {
    if (!this.stopped || this.abort.signal.aborted) return;
    this.stopped = false;
    this.schedule();
  }
  private schedule(): void {
    if (this.stopped) return;
    this.timer = setTimeout(() => {
      this.inFlight = this.run();
    }, this.intervalMs);
  }
  private async run(): Promise<void> {
    try { await this.task(this.abort.signal); }
    catch { this.onFailure(); }
    finally { this.inFlight = undefined; this.schedule(); }
  }
  async stop(): Promise<void> {
    this.stopped = true;
    this.abort.abort();
    clearTimeout(this.timer);
    await this.inFlight;
  }
}
