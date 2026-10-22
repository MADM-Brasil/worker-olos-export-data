import {beforeAll,afterAll,beforeEach,afterEach,it,expect,vi} from 'vitest';
import {mkdtemp,readFile,rm,readdir} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import pg from 'pg';
import {isolatedPostgres} from './support/postgres.js';
import {historicalAttemptsReplay} from './support/historical-attempts-replay.js';
import {createApprovedCaptureReplayBinding} from '../src/integration/attempts/corporate.js';
import {createAttemptsPipeline} from '../src/integration/attempts/pipeline.js';
import {LocalSpool} from '../adapters/local-spool.js';
import {spoolLimits} from '../src/large-batch/config.js';
import {APPROVED_COLUMNS} from '../src/offline/tds-writer.js';
import {columnTypes,mapOffline} from '../src/offline/model.js';
import {projectTdsAttempts} from '../src/offline/tds-projection.js';
import {decodeWireRow} from '../src/large-batch/wire.js';
import {canonical} from '../src/persistence/validation.js';
import {PostgresLargeBatchPersistence} from '../src/persistence/large-batch.js';
import {IsolatedTdsSource} from '../src/olos/tds/source.js';
import {assertAttemptsDestinationSchema} from '../src/integration/attempts/destination-schema.js';
import type {SpoolManifest} from '../src/large-batch/types.js';
const directory=process.env.OLOS_HISTORICAL_AUDIT_DIR;
let fixture:Awaited<ReturnType<typeof isolatedPostgres>>,data:Awaited<ReturnType<typeof historicalAttemptsReplay>>,root:string,index=0;
let pools:{ingest:pg.Pool;project:pg.Pool;receiver:pg.Pool};
const signal=()=>new AbortController().signal;
beforeAll(async()=>{if(!directory)return;
 fixture=await isolatedPostgres({privateData:true});
 expect((await fixture.admin.query("SELECT current_setting('log_min_messages') AS messages,current_setting('log_min_error_statement') AS statements,current_setting('log_statement') AS all_statements,current_setting('log_parameter_max_length_on_error') AS parameters")).rows[0]).toEqual({messages:'panic',statements:'panic',all_statements:'none',parameters:'0'});
 for(const f of ['migrations/003_projection_control.sql','permissions/projection.sql','migrations/005_large_batch_control.sql','permissions/large-batch.sql','migrations/004_delivery_receipts.sql','permissions/receiver.sql'])await fixture.admin.query(await readFile(new URL('../'+f,import.meta.url),'utf8'));
 await fixture.admin.query('CREATE SCHEMA raw');await fixture.admin.query(`CREATE TABLE raw.olos_calls(${APPROVED_COLUMNS.map(c=>`"${c}" ${columnTypes[c]} ${c==='internal_id_olos'?'PRIMARY KEY':''}`).join(',')})`);
 await fixture.admin.query('GRANT USAGE ON SCHEMA raw TO olos_export_receiver');await fixture.admin.query('GRANT SELECT,INSERT ON raw.olos_calls TO olos_export_receiver');
},120000);
afterAll(async()=>{await fixture?.stop();});
beforeEach(async()=>{if(!directory)return;
 data=await historicalAttemptsReplay(directory,`2026-10-${String(22+index++).padStart(2,'0')}`);
 root=await mkdtemp(join(tmpdir(),'olos-historical-audit-'));pools={ingest:fixture.worker(),project:new pg.Pool({...fixture.connection,options:'-c role=olos_export_projector'}),receiver:new pg.Pool({...fixture.connection,options:'-c role=olos_export_receiver'})};
 const c=data.plan.contract;await fixture.admin.query('INSERT INTO olos_export.source_contracts(source_table,version,approved,fields,key_fields,layout_evidence,key_evidence) VALUES($1,$2,true,$3::jsonb,$4,$5,$6) ON CONFLICT DO NOTHING',[c.source_table,c.version,JSON.stringify(c.fields),c.key_fields,c.layout_evidence,c.key_evidence]);
 vi.spyOn(IsolatedTdsSource.prototype,'call').mockImplementation(()=>{throw Error('REMOTE_RPC_FORBIDDEN');});
});
afterEach(async()=>{if(!directory)return;expect(IsolatedTdsSource.prototype.call).not.toHaveBeenCalled();vi.restoreAllMocks();await Promise.all(Object.values(pools).map(p=>p.end()));await rm(root,{recursive:true,force:true});});
async function prepared(name:string){let verified=0;for await(const event of data.source().read(data.plan,signal())){if(event.kind==='row'){decodeWireRow(data.plan,event.values);verified++;}}expect(verified).toBe(570);const spool=await LocalSpool.open(join(root,name),spoolLimits());const binding=createApprovedCaptureReplayBinding(data.plan,data.fields,data.profile,data.catalogs,data.source());
 return {spool,binding,path:join(root,name),pipeline:createAttemptsPipeline(pools.ingest,pools.project,pools.receiver,spool,spoolLimits(),binding)};}
async function manifest(spool:LocalSpool){const files=await readdir(join(spool.root,'batches'));return JSON.parse(await readFile(join(spool.root,'batches',files[0]!,'manifest.json'),'utf8')) as SpoolManifest;}
async function checkpoint(){return (await fixture.admin.query('SELECT c.version::text FROM olos_export.checkpoints c JOIN olos_export.streams s ON s.id=c.stream_id WHERE s.export_date=$1 AND s.read_type=$2',[data.plan.run.exportDate,'reread'])).rows[0]?.version;}
async function finish(spool:LocalSpool,binding:Awaited<ReturnType<typeof prepared>>['binding']){await new PostgresLargeBatchPersistence(pools.ingest,spoolLimits(),300000,{sourceNamespace:binding.sourceNamespace,destinationId:binding.destinationId,mappingVersion:binding.mappingVersion},true).confirmDeliveryCheckpoint(await manifest(spool));}
async function rowCount(){return Number((await fixture.admin.query('SELECT count(*)::text AS n FROM raw.olos_calls')).rows[0].n);}
const run=(name:string,test:()=>Promise<void>,timeout:number)=>it.skipIf(!directory)(name,async()=>{try{await test();}catch(error){if(error instanceof Error && error.name==='AssertionError')throw error;const code=String((error as {code?:string})?.code??'UNCLASSIFIED');throw Error('HISTORICAL_AUDIT_FAILED:'+(['23502','23505','23514','42501','INVALID_VALUE','SOURCE_INTERRUPTED','RECOVERY_REQUIRED','DELIVERY_REQUIRES_RECONCILIATION','CORRUPT'].includes(code)?code:'UNCLASSIFIED'));}},timeout);
run('570 historical rows traverse the V2 replay factory, exact parser, pipeline and 16-column target; repeated input deduplicates',async()=>{
 await assertAttemptsDestinationSchema(fixture.admin);const p=await prepared('first');
 const counts=await p.pipeline.cycle(signal());expect(counts).toMatchObject({received:570,persisted:570,duplicates:0});expect(await rowCount()).toBe(570);expect(await checkpoint()).toBe('0');await finish(p.spool,p.binding);expect(await checkpoint()).toBe('1');await finish(p.spool,p.binding);expect(await checkpoint()).toBe('1');
 // Never let an assertion render private historical rows or personal values.
 const actual=(await fixture.admin.query(`SELECT *,to_char(data_ligacao,'YYYY-MM-DD"T"HH24:MI:SS.US') AS data_ligacao FROM raw.olos_calls ORDER BY internal_id_olos`)).rows;
 const expected=data.bodies.map(body=>projectTdsAttempts(decodeWireRow(data.plan,body),data.catalogs,data.profile,'unused').row).sort((a,b)=>Number(a.internal_id_olos)-Number(b.internal_id_olos));
 expect(canonical(actual)===canonical(expected)).toBe(true);
 const envelopes=(await fixture.admin.query("SELECT payload FROM olos_export.attempts_raw_data ORDER BY (payload->>'AttemptsRawDataID')::bigint")).rows.map(r=>r.payload);
 const decoded=data.bodies.map(body=>decodeWireRow(data.plan,body)).sort((a,b)=>Number(a.AttemptsRawDataID)-Number(b.AttemptsRawDataID));
 expect(canonical(envelopes)===canonical(decoded)).toBe(true);
 expect(data.rows.every(r=>Object.keys(r).length===18)).toBe(true);expect(data.temporalValues).toBe(1710);
 const originalMetrics=data.rows.reduce((a,r)=>{const m=mapOffline(r,data.catalogs,data.profile,'unused').metrics;return {cpf:a.cpf+m.cpf,email:a.email+m.email,invalidXml:a.invalidXml+m.invalidXml,missingCampaign:a.missingCampaign+m.missingCampaign,missingDisposition:a.missingDisposition+m.missingDisposition};},{cpf:0,email:0,invalidXml:0,missingCampaign:0,missingDisposition:0});
 expect(originalMetrics).toEqual({cpf:218,email:264,invalidXml:0,missingCampaign:0,missingDisposition:0});
 const copy=await prepared('duplicate'),duplicate=await copy.pipeline.cycle(signal());expect(duplicate).toMatchObject({received:570,persisted:0,duplicates:570});expect(await rowCount()).toBe(570);await finish(copy.spool,copy.binding);expect(await checkpoint()).toBe('2');
},30000);
run('persistence failure leaves a sealed batch; restarting the pipeline recovers locally without reacquisition',async()=>{
 const p=await prepared('persist-failed');
 const before=(await fixture.admin.query('SELECT count(*)::text AS n FROM olos_export.attempts_raw_data')).rows[0].n;
 await fixture.admin.query('ALTER TABLE olos_export.attempts_raw_data ADD CONSTRAINT audit_reject CHECK(false) NOT VALID');
 try{await expect(p.pipeline.cycle(signal())).rejects.toThrow();}finally{await fixture.admin.query('ALTER TABLE olos_export.attempts_raw_data DROP CONSTRAINT audit_reject');}
 expect((await fixture.admin.query('SELECT count(*)::text AS n FROM olos_export.attempts_raw_data')).rows[0].n).toBe(before);expect((await p.spool.active())?.sealed).toBe(true);expect(await checkpoint()).toBe('0');
 const source=vi.spyOn(p.binding,'source');const restarted=createAttemptsPipeline(pools.ingest,pools.project,pools.receiver,p.spool,spoolLimits(),p.binding);
 expect((await restarted.recoverLocal(signal())).received).toBe(570);expect(source).not.toHaveBeenCalled();await finish(p.spool,p.binding);expect(await checkpoint()).toBe('1');expect(await rowCount()).toBe(570);
},30000);
run('receiver COMMIT without ACK preserves pending deliveries and checkpoint; local receipt reconciliation finishes 570 rows',async()=>{
 const p=await prepared('ack-loss'),original=pools.receiver.connect.bind(pools.receiver);let lost=false;
 const failure=vi.spyOn(pools.receiver,'connect').mockImplementation((async()=>{
  const c=await original();return new Proxy(c,{get(target,key){if(key==='query')return async(sql:string,...args:unknown[])=>{const result=await (target.query as (...a:unknown[])=>Promise<unknown>).call(target,sql,...args);if(sql==='COMMIT' && !lost){lost=true;throw Error('TEST_COMMIT_ACK_LOST');}return result;};const v=Reflect.get(target,key);return typeof v==='function'?v.bind(target):v;}});
 }) as typeof pools.receiver.connect);
 await expect(p.pipeline.cycle(signal())).rejects.toThrow();failure.mockRestore();expect(lost).toBe(true);expect(await checkpoint()).toBe('0');
 expect(Number((await fixture.admin.query("SELECT count(*)::text AS n FROM olos_export.projection_outbox WHERE state<>'confirmed'")).rows[0].n)).toBeGreaterThan(0);
 const source=vi.spyOn(p.binding,'source');await p.pipeline.recoverLocal(signal());expect(source).not.toHaveBeenCalled();await finish(p.spool,p.binding);expect(await checkpoint()).toBe('1');expect(await rowCount()).toBe(570);
 expect(Number((await fixture.admin.query("SELECT count(*)::text AS n FROM olos_export.projection_outbox WHERE state<>'confirmed'")).rows[0].n)).toBe(0);
},30000);
