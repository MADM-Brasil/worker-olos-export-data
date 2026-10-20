import { spawn } from 'node:child_process';
import { mkdtemp, rm, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { expect, it } from 'vitest';
import { isolatedPostgres } from './support/postgres.js';
import { largePlan } from './support/large-batch-fixture.js';

it('recebe e persiste 200.000 registros com spool e heap limitado, sem checkpoint parcial', async () => {
  const fixture = await isolatedPostgres(), root = await mkdtemp(join(tmpdir(),'olos-load-200k-'));
  try {
    await fixture.admin.query(await readFile(new URL('../migrations/005_large_batch_control.sql',import.meta.url),'utf8'));
    await fixture.admin.query(await readFile(new URL('../permissions/large-batch.sql',import.meta.url),'utf8'));
    const c = largePlan().contract;
    await fixture.admin.query(`INSERT INTO olos_export.source_contracts(source_table,version,approved,fields,key_fields,layout_evidence,key_evidence)
      VALUES ($1,$2,true,$3::jsonb,$4,$5,$6)`,[c.source_table,c.version,JSON.stringify(c.fields),c.key_fields,c.layout_evidence,c.key_evidence]);
    const child = spawn(process.execPath,['--expose-gc','--max-old-space-size=128','--max-semi-space-size=4','--import','tsx',
      fileURLToPath(new URL('./support/large-batch-load.ts',import.meta.url))],{ cwd: fileURLToPath(new URL('../',import.meta.url)), stdio: ['pipe','pipe','pipe'] });
    const output: Buffer[] = [], errors: Buffer[] = []; let size = 0;
    child.stdout.on('data',(b: Buffer) => { size += b.length; if (size > 65536) child.kill(); else output.push(b); });
    child.stderr.on('data',(b: Buffer) => { if (errors.reduce((n,x) => n+x.length,0) < 65536) errors.push(b); });
    const timeout = setTimeout(() => child.kill('SIGKILL'),150_000);
    child.stdin.end(JSON.stringify({ root, connection: fixture.connection }));
    const exit = await new Promise<number | null>((resolve,reject) => { child.on('error',reject); child.on('exit',resolve); }).finally(() => clearTimeout(timeout));
    expect(exit,Buffer.concat(errors).toString()).toBe(0);
    const report = JSON.parse(Buffer.concat(output).toString());
    expect(report.result).toEqual({ received: 200_000, persisted: 200_000, duplicates: 0, rejected: 0 });
    expect(report.saved).toBe('200000'); expect(report.checkpoint).toBe('1'); expect(report.sourceCalls).toBe(1);
    expect(report.peakHeap).toBeLessThan(report.heapLimit); expect(report.heapLimit).toBeLessThanOrEqual(140 * 1024 ** 2);
    expect(report.peakBufferedBytes).toBeLessThanOrEqual(report.configuredBufferBytes);
    expect(report.chunkRows).toBe(500); expect(report.spoolBytes).toBeGreaterThan(50 * 1024 ** 2);
    // Artefato sem dados/credenciais para documentação da carga desta sessão.
    await writeFile('/tmp/olos-phase3c-load-report.json',JSON.stringify(report,null,2),{ mode: 0o600 });
  } finally { await fixture.stop(); await rm(root,{ recursive: true, force: true }); }
},180_000);
