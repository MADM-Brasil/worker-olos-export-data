import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, it } from 'vitest';
import type pg from 'pg';
import { documentedTableContracts } from '../src/olos/documented-tables.js';
import { PostgresPersistence } from '../src/persistence/store.js';
import { targets } from '../src/persistence/validation.js';
import { OlosProcedureClient } from '../src/olos/client.js';
import { OlosExtraction } from '../src/olos/extraction.js';
import { documentedFixture, documentedPlan, documentedRecord } from './support/olos-documented-fixture.js';
import { isolatedPostgres } from './support/postgres.js';
import { transport } from './support/olos.js';

let fixture: Awaited<ReturnType<typeof isolatedPostgres>>, pool: pg.Pool;
const tables = Object.keys(documentedTableContracts) as (keyof typeof documentedTableContracts)[];
beforeAll(async () => {
  fixture = await isolatedPostgres();
  for (const table of tables) {
    const c = documentedFixture(table).source;
    await fixture.admin.query(`INSERT INTO olos_export.source_contracts(source_table,version,approved,fields,key_fields,layout_evidence,key_evidence)
      VALUES ($1,$2,true,$3::jsonb,$4,$5,$6)`,[table,c.version,JSON.stringify(c.fields),c.key_fields,c.layout_evidence,c.key_evidence]);
  }
},150_000);
afterAll(async () => { await fixture?.stop(); },30_000);
beforeEach(() => { pool = fixture.worker(); });
afterEach(async () => { await pool?.end(); });
describe('layouts documentados com nulabilidade/chaves exclusivamente sintéticas e PostgreSQL descartável', () => {
  it.each(tables)('persiste envelope completo de %s com a assinatura informada e checkpoint atômico', async table => {
    const contract = documentedFixture(table), row = documentedRecord(table);
    const fake = transport({ datasets: [{ columns: contract.columns, rows: [row,row], representation: 'exact-text/v1', exactEncodingEvidence: 'SYNTHETIC whole seconds; not a real transport' }],
      output: { Message: null, ErrorMessage: null }, returnValue: 0 });
    const store = new PostgresPersistence(pool), engine = new OlosExtraction(new OlosProcedureClient(fake),store);
    const receipt = await engine.run(documentedPlan(table),new AbortController().signal);
    expect(receipt.received).toBe(2); expect(fake.invoke).toHaveBeenCalledTimes(1);
    expect(fake.invoke.mock.calls[0]?.[0].values).toEqual({ TableName: table });
    const saved = (await fixture.admin.query(`SELECT payload FROM ${targets[table]}`)).rows;
    expect(saved).toEqual([{ payload: row }]);
    expect(JSON.stringify(saved)).not.toContain('Password');
    const counts = (await fixture.admin.query('SELECT received_count,persisted_count,duplicate_count FROM olos_export.batches WHERE run_id=$1',[receipt.runId])).rows[0];
    expect(counts).toEqual({ received_count: 2, persisted_count: 1, duplicate_count: 1 });
    const cp = (await fixture.admin.query(`SELECT c.version FROM olos_export.checkpoints c JOIN olos_export.runs r ON r.stream_id=c.stream_id WHERE r.id=$1`,[receipt.runId])).rows[0];
    expect(cp.version).toBe('1');
    expect((await fixture.admin.query("SELECT approved FROM olos_export.source_contracts WHERE source_table=$1 AND version='pending'",[table])).rows[0].approved).toBe(false);
  });
});
