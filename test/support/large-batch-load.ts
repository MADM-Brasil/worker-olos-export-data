import pg from 'pg';
import { getHeapStatistics } from 'node:v8';
import { LocalSpool } from '../../adapters/local-spool.js';
import { spoolLimits } from '../../src/large-batch/config.js';
import { LargeBatchCoordinator } from '../../src/large-batch/coordinator.js';
import { PostgresPersistence } from '../../src/persistence/store.js';
import { PostgresLargeBatchPersistence } from '../../src/persistence/large-batch.js';
import { largePlan, rawSource } from './large-batch-fixture.js';

// Executável SOMENTE de teste; recebe por stdin a conexão da fixture criada pelo pai.
// Não lê credenciais/configuração corporativa, não faz fallback e não exibe erros brutos.
let pool: pg.Pool | undefined;
let timer: ReturnType<typeof setInterval> | undefined;
try {
  const parts: Buffer[] = []; for await (const part of process.stdin) parts.push(Buffer.from(part));
  const input = JSON.parse(Buffer.concat(parts).toString()) as { root: string; connection: pg.PoolConfig };
  if (input.connection.host !== '127.0.0.1' || input.connection.database !== 'phase2a_fixture' || input.connection.user !== 'postgres' ||
    !Number.isSafeInteger(input.connection.port) || typeof input.connection.password !== 'string') throw new Error('INVALID_FIXTURE');
  pool = new pg.Pool({ ...input.connection, options: '-c role=olos_export_worker', max: 3 });
  const limits = spoolLimits({ minFreeBytes: 0 }), spool = await LocalSpool.open(input.root,limits);
  const coordinator = new LargeBatchCoordinator(new PostgresPersistence(pool),spool,new PostgresLargeBatchPersistence(pool,limits),limits);
  global.gc?.();
  const baseline = process.memoryUsage(); let peakHeap = baseline.heapUsed, peakRss = baseline.rss;
  const sample = () => { const m = process.memoryUsage(); peakHeap = Math.max(peakHeap,m.heapUsed); peakRss = Math.max(peakRss,m.rss); };
  timer = setInterval(sample,10); timer.unref();
  const source = rawSource(200_000), read = source.read.bind(source); let observed = 0;
  source.read = async function* (p,s) { for await (const event of read(p,s)) { if (event.kind === 'row' && ++observed % 1000 === 0) sample(); yield event; } };
  const started = performance.now();
  const result = await coordinator.receive(largePlan(),source,new AbortController().signal);
  sample();
  const saved = (await pool.query('SELECT count(*) AS n FROM olos_export.attempts_raw_data')).rows[0].n as string;
  const checkpoint = (await pool.query('SELECT version FROM olos_export.checkpoints')).rows[0].version as string;
  process.stdout.write(JSON.stringify({ result, saved, checkpoint, sourceCalls: source.calls, milliseconds: Math.round(performance.now()-started),
    baselineHeap: baseline.heapUsed, baselineRss: baseline.rss, peakHeap, peakRss, heapLimit: getHeapStatistics().heap_size_limit,
    spoolBytes: spool.metrics.bytes, peakBufferedBytes: spool.metrics.peakBufferedBytes, configuredBufferBytes: limits.bufferBytes,
    chunkRows: limits.chunkRows, chunkBytes: limits.chunkBytes, syncs: spool.metrics.syncs, writes: spool.metrics.writes })+'\n');
} catch { process.stderr.write('ISOLATED_LOAD_FAILED\n'); process.exitCode = 1; }
finally { if (timer) clearInterval(timer); await pool?.end(); }
