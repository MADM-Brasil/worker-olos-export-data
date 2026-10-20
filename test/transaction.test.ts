import { describe, expect, it, vi } from 'vitest';
import type { Pool } from 'pg';
import { transaction } from '../src/persistence/transaction.js';

function fixture(failOn: string | null) {
  const release = vi.fn();
  const query = vi.fn(async (sql: string) => {
    if (sql === failOn) throw new Error('driver-message-with-sensitive-data');
    return { rows: [], rowCount: 0 };
  });
  const pool = { connect: vi.fn(async () => ({ query, release })) } as unknown as Pick<Pool,'connect'>;
  return { pool, query, release };
}
describe('falhas de transporte transacional', () => {
  it('falha de conexão não executa comandos e expõe apenas código sanitizado', async () => {
    const connect = vi.fn(async () => { throw new Error('secret-connection-string'); });
    const body = vi.fn();
    const pool = { connect } as unknown as Pick<Pool,'connect'>;
    await expect(transaction(pool, body)).rejects.toMatchObject({ code: 'PERSISTENCE_FAILED', outcome: 'not_committed', message: 'PERSISTENCE_FAILED' });
    expect(body).not.toHaveBeenCalled();
    expect(connect).toHaveBeenCalledTimes(1);
  });
  it('rollback após erro antes do commit, sem retry implícito', async () => {
    const f = fixture('WRITE');
    await expect(transaction(f.pool, c => c.query('WRITE'))).rejects.toMatchObject({ code: 'PERSISTENCE_FAILED', outcome: 'not_committed' });
    expect(f.query.mock.calls.map(c => c[0])).toContain('ROLLBACK');
    expect(f.query.mock.calls.map(c => c[0])).not.toContain('COMMIT');
    expect(f.release).toHaveBeenCalledWith(false);
  });
  it('falha de confirmação do COMMIT permanece UNKNOWN e destrói conexão', async () => {
    const f = fixture('COMMIT');
    await expect(transaction(f.pool, async () => 'result')).rejects.toMatchObject({ code: 'COMMIT_UNKNOWN', outcome: 'unknown' });
    expect(f.query.mock.calls.filter(c => c[0] === 'COMMIT')).toHaveLength(1);
    expect(f.release).toHaveBeenCalledWith(true);
  });
  it('falha no rollback destrói conexão sem mascarar ausência de COMMIT', async () => {
    const f = fixture('ROLLBACK');
    await expect(transaction(f.pool, async () => { throw new Error('failure'); })).rejects.toMatchObject({ outcome: 'not_committed' });
    expect(f.release).toHaveBeenCalledWith(true);
  });
});
