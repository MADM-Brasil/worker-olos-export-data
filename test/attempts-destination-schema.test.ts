import {beforeAll,afterAll,it,expect} from 'vitest';
import {isolatedPostgres} from './support/postgres.js';
import {APPROVED_COLUMNS} from '../src/offline/tds-writer.js';
import {columnTypes} from '../src/offline/model.js';
import {assertAttemptsDestinationSchema} from '../src/integration/attempts/destination-schema.js';
let fixture:Awaited<ReturnType<typeof isolatedPostgres>>;
beforeAll(async()=>{fixture=await isolatedPostgres();await fixture.admin.query('CREATE SCHEMA raw');await fixture.admin.query(`CREATE TABLE raw.olos_calls(${APPROVED_COLUMNS.map(c=>`"${c}" ${columnTypes[c]} ${c==='internal_id_olos'?'PRIMARY KEY':''}`).join(',')})`);},120000);
afterAll(async()=>{await fixture?.stop();});
it('accepts exactly the nullable destination layout and its real PK',async()=>{await expect(assertAttemptsDestinationSchema(fixture.admin)).resolves.toBeUndefined();});
it('rejects NOT NULL on a field that every approved delivery explicitly sets to NULL',async()=>{
 await fixture.admin.query('ALTER TABLE raw.olos_calls ALTER COLUMN tma_origem SET NOT NULL');
 try{await expect(assertAttemptsDestinationSchema(fixture.admin)).rejects.toThrow('SCHEMA_REJECTED');}
 finally{await fixture.admin.query('ALTER TABLE raw.olos_calls ALTER COLUMN tma_origem DROP NOT NULL');}
});
it('rejects millisecond timestamp precision before acquisition',async()=>{
 await fixture.admin.query('ALTER TABLE raw.olos_calls ALTER COLUMN data_ligacao TYPE timestamp(3)');
 try{await expect(assertAttemptsDestinationSchema(fixture.admin)).rejects.toThrow('SCHEMA_REJECTED');}
 finally{await fixture.admin.query('ALTER TABLE raw.olos_calls ALTER COLUMN data_ligacao TYPE timestamp(6)');}
});
it('rejects a missing PK even when names and types still match',async()=>{
 await fixture.admin.query('ALTER TABLE raw.olos_calls DROP CONSTRAINT olos_calls_pkey');
 try{await expect(assertAttemptsDestinationSchema(fixture.admin)).rejects.toThrow('SCHEMA_REJECTED');}
 finally{await fixture.admin.query('ALTER TABLE raw.olos_calls ADD PRIMARY KEY(internal_id_olos)');}
});
