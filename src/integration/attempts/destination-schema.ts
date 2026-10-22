import type {PoolClient} from 'pg';
import {APPROVED_COLUMNS} from '../../offline/tds-writer.js';

/** Read-only: the exact destination contract required before any source consumption. */
export async function assertAttemptsDestinationSchema(c:Pick<PoolClient,'query'>){
  const cols=(await c.query(`SELECT column_name,udt_name,datetime_precision,is_nullable,is_generated,is_identity FROM information_schema.columns WHERE table_schema='raw' AND table_name='olos_calls' ORDER BY ordinal_position`)).rows;
  if(cols.length!==16 || cols.some((r,i)=>r.column_name!==APPROVED_COLUMNS[i] || r.is_nullable!==(i===0?'NO':'YES') || r.is_generated!=='NEVER' || r.is_identity!=='NO' || r.udt_name!==(i===0 || i===15?'int8':i===2?'timestamp':i===13?'int4':'text')) || cols[2].datetime_precision!==6)throw Error('SCHEMA_REJECTED');
  const pk=(await c.query(`SELECT array_agg(a.attname::text ORDER BY k.ord) AS keys FROM pg_constraint p
    CROSS JOIN LATERAL unnest(p.conkey) WITH ORDINALITY k(attnum,ord) JOIN pg_attribute a ON a.attrelid=p.conrelid AND a.attnum=k.attnum
    WHERE p.conrelid='raw.olos_calls'::regclass AND p.contype='p'`)).rows[0].keys;
  if(JSON.stringify(pk)!==JSON.stringify(['internal_id_olos']))throw Error('SCHEMA_REJECTED');
  for(const privilege of ['SELECT','INSERT'])if(!(await c.query("SELECT has_table_privilege(current_user,'raw.olos_calls',$1) AS allowed",[privilege])).rows[0].allowed)throw Error('PRIVILEGE_REJECTED');
}
