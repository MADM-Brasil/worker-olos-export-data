import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { randomUUID } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import pg from 'pg';

const exec = promisify(execFile);
// Nenhuma URL/credencial PG da máquina é consumida. Somente container criado por este helper.
export async function isolatedPostgres() {
  const password = randomUUID();
  let id: string | undefined;
  let admin: pg.Pool | undefined;
  const stop = async () => {
    await admin?.end();
    if (id) await exec('docker', ['rm', '-f', id]);
  };
  try {
    const result = await exec('docker', ['run','--rm','-d','--label','worker-olos-export-data.test=phase2a',
      '-p','127.0.0.1::5432','--tmpfs','/var/lib/postgresql/data:rw',
      '-e','POSTGRES_PASSWORD','-e','POSTGRES_DB=phase2a_fixture','postgres:16-bookworm'],
      { env: { ...process.env, POSTGRES_PASSWORD: password }, timeout: 120_000 });
    id = result.stdout.trim();
    if (!/^[a-f0-9]{64}$/.test(id)) throw new Error('Invalid fixture container');
    const inspected = await exec('docker', ['inspect','--format','{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}',id]);
    const port = Number(inspected.stdout.trim());
    if (!Number.isSafeInteger(port) || port < 1 || port > 65535) throw new Error('Invalid fixture port');
    const connection = { host: '127.0.0.1', port, user: 'postgres', password, database: 'phase2a_fixture',
      ssl: false as const, connectionTimeoutMillis: 1000, query_timeout: 5000, max: 5 };
    admin = new pg.Pool(connection);
    let ready = false;
    for (let attempt = 0; attempt < 100; attempt++) {
      try { await admin.query('SELECT 1'); ready = true; break; } catch { await new Promise(resolve => setTimeout(resolve, 200)); }
    }
    if (!ready) throw new Error('Fixture unavailable');
    for (const filename of ['001_control.sql','002_operational_envelopes.sql']) {
      await admin.query(await readFile(new URL(`../../migrations/${filename}`, import.meta.url), 'utf8'));
    }
    await admin.query(await readFile(new URL('../../permissions/worker.sql', import.meta.url), 'utf8'));
    const worker = () => new pg.Pool({ ...connection, options: '-c role=olos_export_worker' });
    return { admin, worker, connection, stop };
  } catch {
    await stop();
    throw new Error('PostgreSQL de teste isolado indisponível; nenhum fallback para banco externo é permitido');
  }
}
