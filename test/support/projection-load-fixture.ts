import {readFile} from 'node:fs/promises';
import {randomUUID} from 'node:crypto';
import pg from 'pg';
import {isolatedPostgres} from './postgres.js';
import {largePlan,rawRow} from './large-batch-fixture.js';
import {decodeWireRow} from '../../src/large-batch/wire.js';
import {exampleProfile,columnTypes} from '../../src/offline/model.js';
import {APPROVED_COLUMNS} from '../../src/offline/tds-writer.js';
import {isolatedTdsDelivery} from '../../src/offline/tds-delivery.js';

/** Only a newly created disposable container. Never reads runtime credentials. */
export async function projectionLoadFixture(){
 const fixture=await isolatedPostgres();
 const project=new pg.Pool({...fixture.connection,max:2,query_timeout:15000,options:'-c role=olos_export_projector'});
 const receiver=new pg.Pool({...fixture.connection,max:2,query_timeout:15000,options:'-c role=olos_export_receiver'});
 const destinationId='synthetic-load-target';
 const profile={...exampleProfile,columns:[...APPROVED_COLUMNS],phoneMatchKey:'normalized' as const};
 const catalogs={agents:new Map<string,string>(),campaigns:new Map<string,string>(),dispositions:new Map<string,string>()};
 const delivery=()=>isolatedTdsDelivery(project,receiver,destinationId,profile,catalogs,'2026-01-01T00:00:00',true);
 const stop=async()=>{await Promise.all([project.end(),receiver.end()]);await fixture.stop();};
 try{
  for(const name of ['migrations/003_projection_control.sql','permissions/projection.sql','migrations/004_delivery_receipts.sql','permissions/receiver.sql'])
   await fixture.admin.query(await readFile(new URL('../../'+name,import.meta.url),'utf8'));
  await fixture.admin.query('CREATE SCHEMA raw');
  await fixture.admin.query(`CREATE TABLE raw.olos_calls(${APPROVED_COLUMNS.map(c=>`"${c}" ${columnTypes[c]} ${c==='internal_id_olos'?'PRIMARY KEY':''}`).join(',')})`);
  await fixture.admin.query('GRANT USAGE ON SCHEMA raw TO olos_export_receiver');
  await fixture.admin.query('GRANT SELECT,INSERT ON raw.olos_calls TO olos_export_receiver');
  const plan=largePlan(),contract=plan.contract;
  await fixture.admin.query('INSERT INTO olos_export.source_contracts(source_table,version,approved,fields,key_fields,layout_evidence,key_evidence) VALUES($1,$2,true,$3,$4,$5,$6)',
   [contract.source_table,contract.version,JSON.stringify(contract.fields),contract.key_fields,contract.layout_evidence,contract.key_evidence]);
  const seed=async(count:number,append=false)=>{
   if(!Number.isSafeInteger(count)||count<1||count>200000)throw Error('INVALID_SYNTHETIC_LOAD');
   const stream=append?(await fixture.admin.query('SELECT id FROM olos_export.streams LIMIT 1')).rows[0].id as string:randomUUID(),run=randomUUID(),batch=randomUUID();
   const offset=append?Number((await fixture.admin.query('SELECT coalesce(max(source_key::bigint),0)::text AS n FROM olos_export.projection_outbox')).rows[0].n):0;
   if(!append)await fixture.admin.query('TRUNCATE olos_export.streams,olos_export.delivery_receipts,raw.olos_calls CASCADE');
   if(!append)await fixture.admin.query("INSERT INTO olos_export.streams(id,source_table,read_type,export_date) VALUES($1,'AttemptsRawData','current_day','2026-01-01')",[stream]);
   await fixture.admin.query("INSERT INTO olos_export.runs(id,stream_id,source_table,contract_version,fencing_token,state) VALUES($1,$2,'AttemptsRawData',$3,1,'succeeded')",[run,stream,contract.version]);
   await fixture.admin.query("INSERT INTO olos_export.batches(id,run_id,stream_id,source_table,digest,expected_version,state,received_count,persisted_count,committed_at) VALUES($1,$2,$3,'AttemptsRawData',$4,0,'committed',$5,$5,clock_timestamp())",[batch,run,stream,'0'.repeat(64),count]);
   await fixture.admin.query("INSERT INTO olos_export.projection_batches(batch_id,destination_id,source_table,source_namespace,source_contract_version,field_definitions,destination_version,mapping_version,config_digest,snapshot_digest,row_count) VALUES($1,$2,'AttemptsRawData','synthetic',$3,$4,'raw.olos_calls/v1','synthetic-load/v1',$5,$5,$6)",[batch,destinationId,contract.version,JSON.stringify(contract.fields),'0'.repeat(64),count]);
   const payload=decodeWireRow(plan,rawRow(plan));
   const insert={query_timeout:120000,text:`INSERT INTO olos_export.projection_outbox(delivery_id,batch_id,destination_id,source_key,payload)
    SELECT gen_random_uuid(),$1,$2,i::text,$3::jsonb||jsonb_build_object('AttemptsRawDataID',i::text,'CallId',i::text)
    FROM generate_series($5::integer+1,$5::integer+$4::integer) AS i`,values:[batch,destinationId,JSON.stringify(payload),count,offset]};
   await fixture.admin.query(insert);
   await fixture.admin.query('ANALYZE olos_export.projection_outbox');return batch;
  };
  return {...fixture,project,receiver,destinationId,profile,catalogs,delivery,seed,stop};
 }catch(error){await stop();throw error;}
}
