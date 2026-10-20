import type { Pool, PoolClient } from 'pg';
import { PersistenceError } from './types.js';

export async function transaction<T>(pool: Pick<Pool, 'connect'>, body: (client: PoolClient) => Promise<T>): Promise<T> {
  let client: PoolClient;
  try { client = await pool.connect(); } catch { throw new PersistenceError('PERSISTENCE_FAILED'); }
  let committing = false;
  let destroy = false;
  try {
    await client.query('BEGIN');
    await client.query("SET LOCAL lock_timeout = '2s'");
    await client.query("SET LOCAL idle_in_transaction_session_timeout = '15s'");
    const result = await body(client);
    committing = true;
    await client.query('COMMIT');
    return result;
  } catch (error) {
    try { await client.query('ROLLBACK'); } catch { destroy = true; }
    if (committing) { destroy = true; throw new PersistenceError('COMMIT_UNKNOWN', 'unknown'); }
    if (error instanceof PersistenceError) throw error;
    if (typeof error === 'object' && error !== null && 'code' in error && error.code === '55P03') throw new PersistenceError('BUSY');
    throw new PersistenceError('PERSISTENCE_FAILED');
  } finally { client.release(destroy); }
}
