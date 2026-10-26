import type { Pool, PoolClient } from 'pg';
import { PersistenceError } from './types.js';
import {preserveFailure} from '../diagnostics/worker-failure.js';

export async function transaction<T>(pool: Pick<Pool, 'connect'>, body: (client: PoolClient) => Promise<T>): Promise<T> {
  let client: PoolClient;
  try { client = await pool.connect(); } catch(error) { throw preserveFailure(new PersistenceError('PERSISTENCE_FAILED'),error); }
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
    if (committing) { destroy = true; throw preserveFailure(new PersistenceError('COMMIT_UNKNOWN', 'unknown'),error); }
    // No COMMIT was sent, but an unacknowledged rollback must never become recoverable BUSY.
    if (destroy) throw preserveFailure(new PersistenceError('PERSISTENCE_FAILED'),error);
    if (error instanceof PersistenceError) throw error;
    if (typeof error === 'object' && error !== null && 'code' in error && error.code === '55P03') throw preserveFailure(new PersistenceError('BUSY'),error);
    throw preserveFailure(new PersistenceError('PERSISTENCE_FAILED'),error);
  } finally { client.release(destroy); }
}
