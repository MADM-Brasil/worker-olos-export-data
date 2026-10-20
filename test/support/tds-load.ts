import pg from 'pg';
import { getHeapStatistics } from 'node:v8';
import { LocalSpool } from '../../adapters/local-spool.js';
import { spoolLimits } from '../../src/large-batch/config.js';
import { LargeBatchCoordinator } from '../../src/large-batch/coordinator.js';
import { PostgresPersistence } from '../../src/persistence/store.js';
import { PostgresLargeBatchPersistence } from '../../src/persistence/large-batch.js';
import { IsolatedTdsSource, type IsolatedEndpoint } from '../../src/olos/tds/source.js';
import type { TdsField } from '../../src/olos/tds/raw-parser.js';
import type { LargePlan } from '../../src/large-batch/types.js';
let pool: pg.Pool | undefined, timer: ReturnType<typeof setInterval> | undefined;
try {
  const parts: Buffer[] = []; for await (const part of process.stdin) parts.push(Buffer.from(part));
  const input = JSON.parse(Buffer.concat(parts).toString()) as { root: string; connection: pg.PoolConfig; endpoint: IsolatedEndpoint; plan: LargePlan; fields: TdsField[] };
  if (input.connection.host !== '127.0.0.1' || input.connection.database !== 'phase2a_fixture' || input.connection.user !== 'postgres') throw new Error('INVALID_FIXTURE');
  pool = new pg.Pool({ ...input.connection,options: '-c role=olos_export_worker',max: 3 });
  const limits = spoolLimits({ minFreeBytes: 0 }), spool = await LocalSpool.open(input.root,limits);
  const coordinator = new LargeBatchCoordinator(new PostgresPersistence(pool),spool,new PostgresLargeBatchPersistence(pool,limits),limits);
  const source = new IsolatedTdsSource(input.endpoint,input.fields);
  global.gc?.(); const baseline = process.memoryUsage(); let peakHeap = baseline.heapUsed, peakRss = baseline.rss;
  const sample = () => { const m = process.memoryUsage(); peakHeap = Math.max(peakHeap,m.heapUsed); peakRss = Math.max(peakRss,m.rss); };
  timer = setInterval(sample,10); timer.unref(); const started = performance.now();
  const result = await coordinator.receive(input.plan,source,new AbortController().signal); sample();
  const saved = (await pool.query('SELECT count(*) AS n FROM olos_export.attempts_raw_data')).rows[0].n;
  const checkpoint = (await pool.query('SELECT version FROM olos_export.checkpoints')).rows[0].version;
  const payload = (await pool.query('SELECT payload FROM olos_export.attempts_raw_data LIMIT 1')).rows[0].payload;
  if (!String(payload.StartDate).endsWith('01000000')) throw new Error('TICK_NOT_PRESERVED');
  process.stdout.write(JSON.stringify({ result,saved,checkpoint,sourceMetrics: source.metrics,milliseconds: Math.round(performance.now()-started),
    baselineHeap: baseline.heapUsed,peakHeap,peakRss,heapLimit: getHeapStatistics().heap_size_limit,spoolBytes: spool.metrics.bytes,peakBufferedBytes: spool.metrics.peakBufferedBytes })+'\n');
} catch { process.stderr.write('ISOLATED_TDS_LOAD_FAILED\n'); process.exitCode = 1; }
finally { if (timer) clearInterval(timer); await pool?.end(); }
