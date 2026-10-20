import { afterAll, afterEach, beforeAll, describe, expect, it, vi } from 'vitest';
import { mkdtemp, writeFile, readFile, rm, chmod, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import type pg from 'pg';
import { exampleProfile, profileSchema, parseLossless, recordsFromJson, originalId, xmlContacts, validCpf, validEmail, businessTimestamp, normalizePhone, mapOffline, catalog, type OfflineRecord } from '../src/offline/model.js';
import { prepareOffline, offlinePool, offlineCycle, privateText, runOfflineScheduler, type OfflineState, type OfflineInputs } from '../adapters/offline-homologation.js';
const exec=promisify(execFile);
// Generated checksum-valid fixtures, never real personal records.
function cpf(seed: number) { let s=String(seed).padStart(9,'0'); for (const n of [9,10]) { let sum=0; for(let i=0;i<n;i++) sum+=Number(s[i])*(n+1-i); s+=sum%11<2 ? '0' : String(11-sum%11); } return s; }
function fixture(i: number): OfflineRecord { return { AttemptsRawDataID: String(9007199254740993n+BigInt(i)),CallId: `call-${Math.floor(i/2)}`,StartDate: '2026-10-01T12:00:00.003000',EndCall: i<21 ? '2026-10-01T12:00:30.003000' : null,
  OriginalPhoneNumber: '(11) 90000-0000',CampaignId: '7',DispositionId: '9',AgentId: null,CampaignData: `<data>${i<218 ? `<CPF>${cpf(i+1)}</CPF>` : ''}${i<264 ? `<Email>fixture${i}@example.invalid</Email>` : ''}</data>`,ExtraNullable: null }; }
const catalogs={ campaigns: new Map([['7','Fixture campaign']]),dispositions: new Map([['9','Fixture disposition']]),agents: new Map<string,string>() };

describe('offline mapping: original evidence and conservative enrichment',() => {
  it('parses BIGINT and decimal JSON tokens without JS number loss and rejects duplicate keys',() => {
    const row=recordsFromJson(parseLossless('[{"AttemptsRawDataID":9223372036854775807,"Amount":123456789012345678.123400,"Empty":null}]'))[0]!;
    expect(originalId(row)).toBe('9223372036854775807'); expect(row.Amount).toBe('123456789012345678.123400'); expect(row.Empty).toBeNull(); expect(Object.hasOwn(row,'Missing')).toBe(false);
    expect(() => parseLossless('{"a":1,"a":2}')).toThrow('INVALID_PRIVATE_JSON');
  });
  it('rejects ambiguous datasets, passwords, numeric JS values, overflow and empty source',() => {
    expect(() => recordsFromJson({recordsets:[[],[]]})).toThrow(); expect(() => recordsFromJson([{AttemptsRawDataID:1}])).toThrow();
    expect(() => recordsFromJson([{Password:'secret'}])).toThrow(); expect(() => recordsFromJson([])).toThrow();
    expect(() => originalId({AttemptsRawDataID:'9223372036854775808'})).toThrow();
    expect(() => originalId({AttemptsRawDataID:'-0'})).toThrow();
    expect(recordsFromJson([],true)).toEqual([]);
  });
  it('validates XML checksums and email syntax; does not infer contact data',() => {
    expect(validCpf(cpf(1))).toBe(cpf(1)); expect(validCpf('11111111111')).toBeNull(); expect(validCpf(cpf(1).slice(0,-1)+'9')).toBeNull();
    expect(validEmail('a..b@example.invalid')).toBeNull(); expect(validEmail(' a@example.invalid ')).toBe('a@example.invalid');
    expect(xmlContacts(`<r><CPF>${cpf(1)}</CPF><Email><![CDATA[a@example.invalid]]></Email></r>`,exampleProfile)).toMatchObject({cpf:cpf(1),email:'a@example.invalid',invalidXml:false});
    expect(xmlContacts('<!DOCTYPE r [<!ENTITY x SYSTEM "file:///etc/passwd">]><r>&x;</r>',exampleProfile)).toMatchObject({cpf:null,email:null,invalidXml:true});
    expect(xmlContacts(`<r><CPF>${cpf(1)}</CPF><CPF>${cpf(2)}</CPF></r>`,exampleProfile)).toMatchObject({cpf:null,ambiguous:true});
    expect(xmlContacts('<r><unknown>a@example.invalid</unknown></r>',exampleProfile).email).toBeNull();
  });
  it('supports only explicitly selected XML field attribute rules',() => {
    const xml=`<r><field name="CPF" value="${cpf(1)}"/><field name="Email">a@example.invalid</field></r>`;
    expect(xmlContacts(xml,exampleProfile).cpf).toBeNull();
    expect(xmlContacts(xml,{...exampleProfile,xmlFieldAttributes:{name:'name',value:'value'}}).cpf).toBe(cpf(1));
    expect(xmlContacts(xml,{...exampleProfile,xmlFieldAttributes:{name:'name',value:'$text'}}).email).toBe('a@example.invalid');
  });
  it('preserves timestamp microseconds and refuses destructive precision/timezone conversion',() => {
    expect(businessTimestamp('2026-10-01 12:00:00.003333','reject-offset')).toBe('2026-10-01T12:00:00.003333');
    expect(businessTimestamp('2026-10-01T12:00:00.0033333','utc-civil')).toBeNull();
    expect(businessTimestamp('2026-10-01T12:00:00.003333-03:00','reject-offset')).toBeNull();
    expect(businessTimestamp('2026-10-01T12:00:00.003333-03:00','utc-civil')).toBe('2026-10-01T15:00:00.003333');
    expect(businessTimestamp('2026-02-30 12:00:00','reject-offset')).toBeNull();
    expect(businessTimestamp(new Date(),'utc-civil')).toBeNull();
  });
  it('does not conflate IDs, infer agent names, lead/reason or enable provisional TMA by default',() => {
    const mapped=mapOffline(fixture(0),catalogs,exampleProfile,'2026-10-16T12:00:00');
    expect(Object.keys(mapped.row)).toHaveLength(16); expect(mapped.row).toMatchObject({internal_id_olos:'9007199254740993',olos_call_id:'call-0',nome_agente:null,lead_id_origem:null,razao_desligamento:null,tma_origem:null,nome_campanha_olos:'Fixture campaign'});
    expect(normalizePhone('0011-1234','digits-only')).toBe('00111234'); expect(normalizePhone('UNKNOWN','digits-only')).toBeNull();
    expect(() => profileSchema.parse({...exampleProfile,columns:[...exampleProfile.columns.slice(1),'kommo_lead_id']})).toThrow();
    expect(() => catalog([{CampaignId:'7',Description:'a'},{CampaignId:'7',Description:'b'}],'CampaignId','Description')).toThrow('CATALOG_CONFLICT');
    expect(() => mapOffline({...fixture(0),CallId:'\ud800'},catalogs,exampleProfile,'2026-10-16T12:00:00')).toThrow('OFFLINE_TEXT_REQUIRES_REVIEW');
    expect(() => mapOffline({...fixture(0),CampaignId:true},catalogs,exampleProfile,'2026-10-16T12:00:00')).toThrow('OFFLINE_SOURCE_TYPE_REQUIRES_REVIEW');
    expect(() => mapOffline({...fixture(0),StartDate:'2026-10-01T12:00:00.0033333'},catalogs,exampleProfile,'2026-10-16T12:00:00')).toThrow('OFFLINE_TIMESTAMP_REQUIRES_REVIEW');
  });
});

describe('offline scheduler uses the existing non-overlapping scheduler',() => {
  afterEach(() => vi.useRealTimers());
  it('runs immediately, waits 60 seconds after completion, drains on stop and never overlaps',async() => {
    vi.useFakeTimers(); let stop!:()=>void,release!:()=>void; const stopped=new Promise<void>(r=>{stop=r;});
    let calls=0,active=0,max=0; const run=runOfflineScheduler(async()=>{calls++; active++; max=Math.max(max,active); if(calls===2) await new Promise<void>(r=>{release=r;}); active--;},stopped);
    await vi.advanceTimersByTimeAsync(0); expect(calls).toBe(1); await vi.advanceTimersByTimeAsync(59999); expect(calls).toBe(1);
    await vi.advanceTimersByTimeAsync(1); expect(calls).toBe(2); await vi.advanceTimersByTimeAsync(180000); expect(calls).toBe(2); expect(max).toBe(1);
    release(); await vi.advanceTimersByTimeAsync(0); await vi.advanceTimersByTimeAsync(60000); expect(calls).toBe(3); stop(); await run; await vi.advanceTimersByTimeAsync(120000); expect(calls).toBe(3);
  });
  it('stops after a failed local cycle',async()=>{ vi.useFakeTimers(); let calls=0; const run=expect(runOfflineScheduler(async()=>{if(++calls===2) throw Error('local');},new Promise(()=>{}))).rejects.toThrow('OFFLINE_LOCAL_CYCLE_FAILED'); await vi.advanceTimersByTimeAsync(60000); await run; await vi.advanceTimersByTimeAsync(120000); expect(calls).toBe(2); });
});

describe('isolated PostgreSQL offline pipeline (570 SYNTHETIC records)',() => {
  let dir:string,root:string,state:OfflineState,pool:pg.Pool,admin:pg.Pool,inputs:OfflineInputs;
  beforeAll(async()=>{
    dir=await mkdtemp(join(tmpdir(),'olos-offline-test-')); await chmod(dir,0o700); root=join(dir,'run');
    const profile={...exampleProfile,tmaPolicy:'EndCall-minus-StartDate-provisional' as const};
    state=await prepareOffline(root,profile); pool=await offlinePool(state); admin=await offlinePool(state,false);
    const write=async(name:string,rows:unknown)=>{const path=join(dir,name);await writeFile(path,JSON.stringify(rows),{mode:0o600});return path;};
    inputs={input:await write('attempts.json',Array.from({length:570},(_,i)=>fixture(i))),campaigns:await write('campaigns.json',[{CampaignId:'7',Description:'Fixture campaign'}]),dispositions:await write('dispositions.json',[{DispositionId:'9',Description:'Fixture disposition'}]),exportDate:'2026-10-01',expectedCount:570,expectedCpfs:218,expectedEmails:264};
  },120000);
  afterAll(async()=>{await pool?.end();await admin?.end();if(state)await exec('docker',['rm','-f','-v',state.container]);if(dir)await rm(dir,{recursive:true,force:true});},30000);
  it('imports, commits local checkpoint and delivers all 570; repeats without duplicates or new checkpoint',async()=>{
    const one=await offlineCycle(root,state,pool,inputs);
    expect(one).toMatchObject({rows:'570',unique_rows:'570',expected:570,confirmed:570,complete:true,sqlServerQueries:0,sourceObservedNow:false,metrics:{cpf:218,email:264,tma:21}});
    const counts=(await pool.query('SELECT count(cpf)::int cpf,count(email)::int email,count(tma_origem)::int tma,count(lead_id_origem)::int leads,count(razao_desligamento)::int reasons FROM raw.olos_calls')).rows[0];
    expect(counts).toEqual({cpf:218,email:264,tma:21,leads:0,reasons:0});
    expect((await pool.query('SELECT count(DISTINCT olos_call_id)::int n FROM raw.olos_calls')).rows[0].n).toBe(285);
    const source=(await pool.query("SELECT payload->>'source_record' AS body FROM olos_export.attempts_raw_data WHERE payload->>'AttemptsRawDataID'=$1",['9007199254740993'])).rows[0];
    expect(source).toBeDefined(); const original=parseLossless(source.body) as OfflineRecord; expect(original.ExtraNullable).toBeNull(); expect(original.AttemptsRawDataID).toBe('9007199254740993');
    const before=(await pool.query('SELECT internal_id_olos::text,xmin::text,data_carga::text FROM raw.olos_calls ORDER BY internal_id_olos')).rows;
    const two=await offlineCycle(root,state,pool,inputs); expect(two).toMatchObject({rows:'570',confirmed:570,complete:true,ingestionCheckpoint:one.ingestionCheckpoint});
    expect((await pool.query('SELECT internal_id_olos::text,xmin::text,data_carga::text FROM raw.olos_calls ORDER BY internal_id_olos')).rows).toEqual(before);
    expect((await pool.query('SELECT count(*)::int n FROM olos_export.batches')).rows[0].n).toBe(1);
    expect((await pool.query('SELECT count(*)::int n FROM olos_export.delivery_receipts')).rows[0].n).toBe(570);
    const archived=(await pool.query('SELECT input_digest FROM olos_export.offline_imports')).rows[0].input_digest;
    expect(await readFile(join(root,'archive',`${archived}.json`))).toEqual(await readFile(inputs.input));
    const log=JSON.stringify(two); expect(log).not.toContain('example.invalid');expect(log).not.toContain(cpf(1));expect(log).not.toContain('90000-0000');
  },120000);
  it('rejects concurrent cycles while the PostgreSQL advisory lock is held',async()=>{const lock=await pool.connect();try{await lock.query('SELECT pg_advisory_lock(740210,1)');await expect(offlineCycle(root,state,pool,inputs)).rejects.toThrow('OFFLINE_WORKER_BUSY');}finally{await lock.query('SELECT pg_advisory_unlock(740210,1)');lock.release();}});
  it('blocks mismatched expectations and conflicting duplicate IDs without altering checkpoint',async()=>{
    await expect(offlineCycle(root,state,pool,{...inputs,expectedCount:571})).rejects.toThrow('EXPECTED_COUNT_MISMATCH');
    const bad=join(dir,'conflict.json');await writeFile(bad,JSON.stringify([fixture(0),{...fixture(0),CallId:'different'}]),{mode:0o600});
    await expect(offlineCycle(root,state,pool,{...inputs,input:bad})).rejects.toThrow('CONFLICTING_DUPLICATE_ID');
    expect((await pool.query('SELECT count(*)::int n FROM olos_export.batches')).rows[0].n).toBe(1);
  });
  it('keeps a sealed spool across destination failure/restart; replays locally and preserves valid fields against NULL',async()=>{
    const input=join(dir,'replay.json');await writeFile(input,JSON.stringify([{...fixture(0),CampaignData:null,OriginalPhoneNumber:null,EndCall:null},{...fixture(0),CampaignData:null,OriginalPhoneNumber:null,EndCall:null}]),{mode:0o600});
    const replay={...inputs,input,expectedCount:1,expectedCpfs:0,expectedEmails:0};
    await admin.query("CREATE FUNCTION raw.fail_offline() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic destination unavailable'; END $$");
    await admin.query('CREATE TRIGGER offline_failure BEFORE INSERT OR UPDATE ON raw.olos_calls FOR EACH ROW EXECUTE FUNCTION raw.fail_offline()');
    const failed=await offlineCycle(root,state,pool,replay);expect(failed).toMatchObject({expected:1,confirmed:0,complete:false,duplicatesInFile:1,rows:'570'});
    const job=(await pool.query('SELECT spool_manifest,batch_id FROM olos_export.offline_imports WHERE completed_at IS NULL')).rows[0];expect(job.spool_manifest.sealed).not.toBe(false);
    expect((await pool.query('SELECT count(*)::int n FROM olos_export.delivery_receipts')).rows[0].n).toBe(570);
    await expect(offlineCycle(root,state,pool,{...inputs,exportDate:'2026-10-02'})).rejects.toThrow('RECOVER_PREVIOUS_OFFLINE_IMPORT_FIRST');
    await admin.query('DROP TRIGGER offline_failure ON raw.olos_calls');await admin.query('DROP FUNCTION raw.fail_offline()');
    await admin.query("UPDATE olos_export.projection_outbox SET lease_until=CASE WHEN lease_owner IS NULL THEN NULL ELSE clock_timestamp()-interval '1 second' END,available_at=clock_timestamp() WHERE state IN ('pending','uncertain')");
    await pool.end();pool=await offlinePool(state); // Restart, no in-memory delivery handle.
    const recovered=await offlineCycle(root,state,pool,replay);expect(recovered).toMatchObject({expected:1,confirmed:1,complete:true,rows:'570',sqlServerQueries:0,ingestionCheckpoint:failed.ingestionCheckpoint});
    const row=(await pool.query('SELECT cpf,email,telefone,tma_origem FROM raw.olos_calls WHERE internal_id_olos=$1',['9007199254740993'])).rows[0];expect(row).toMatchObject({cpf:cpf(1),email:'fixture0@example.invalid',telefone:'(11) 90000-0000',tma_origem:30});
    expect((await pool.query('SELECT count(*)::int n FROM olos_export.delivery_receipts')).rows[0].n).toBe(571);
  },30000);
  it('recovers a sealed spool after a crash before ingestion and never advances a partial checkpoint',async()=>{
    const input=join(dir,'before-ingestion.json');await writeFile(input,JSON.stringify([fixture(600)]),{mode:0o600});
    const local={...inputs,input,expectedCount:1,expectedCpfs:0,expectedEmails:0};
    const stopped=new Proxy(pool,{get(target,key){if(key==='query')return async(sql:unknown,...params:unknown[])=>{
      if(String(sql)==='SELECT state FROM olos_export.batches WHERE id=$1')throw Error('synthetic stop after seal before ingestion');
      return (target.query as Function).call(target,sql,...params);
    };const v=Reflect.get(target,key);return typeof v==='function'?v.bind(target):v;}});
    await expect(offlineCycle(root,state,stopped,local)).rejects.toThrow('synthetic stop after seal before ingestion');
    const job=(await pool.query('SELECT spool_manifest,stream_id FROM olos_export.offline_imports WHERE completed_at IS NULL')).rows[0];expect(job.spool_manifest).toBeTruthy();
    const cp=(await pool.query('SELECT version FROM olos_export.checkpoints WHERE stream_id=$1',[job.stream_id])).rows[0].version;
    expect((await pool.query('SELECT count(*)::int n FROM raw.olos_calls')).rows[0].n).toBe(570);
    const replay=await offlineCycle(root,state,pool,local);expect(replay).toMatchObject({expected:1,confirmed:1,complete:true,rows:'571',sqlServerQueries:0});expect(BigInt(replay.ingestionCheckpoint)).toBe(BigInt(cp)+1n);
  },30000);
  it('reconciles COMMIT without ACK by its receipt after restart instead of writing again',async()=>{
    const input=join(dir,'lost-ack.json');await writeFile(input,JSON.stringify([fixture(601)]),{mode:0o600});
    const local={...inputs,input,expectedCount:1,expectedCpfs:0,expectedEmails:0};let lost=false;
    // Instance-scoped transport fault AFTER the actual target COMMIT; no global patch.
    const faulty=new Proxy(pool,{get(target,key){if(key==='connect')return async()=>{const c=await target.connect();let business=false;return new Proxy(c,{get(client,k){if(k==='query')return async(sql:unknown,...params:unknown[])=>{
      const q=String(sql);if(q.includes('INSERT INTO raw.olos_calls'))business=true;
      const result=await (client.query as Function).call(client,sql,...params);
      if(q==='COMMIT'&&business&&!lost){lost=true;throw Error('synthetic lost acknowledgement');}return result;
    };const v=Reflect.get(client,k);return typeof v==='function'?v.bind(client):v;}});};const v=Reflect.get(target,key);return typeof v==='function'?v.bind(target):v;}});
    const first=await offlineCycle(root,state,faulty,local);expect(lost).toBe(true);expect(first).toMatchObject({confirmed:1,complete:true,rows:'572'});
    expect((await pool.query("SELECT count(*)::int n FROM olos_export.projection_outbox WHERE state='uncertain'")).rows[0].n).toBe(1);
    const before=(await pool.query('SELECT xmin::text FROM raw.olos_calls WHERE internal_id_olos=$1',[fixture(601).AttemptsRawDataID])).rows[0];
    await pool.end();pool=await offlinePool(state);const second=await offlineCycle(root,state,pool,local);expect(second).toMatchObject({confirmed:1,complete:true,rows:'572',ingestionCheckpoint:first.ingestionCheckpoint});
    expect((await pool.query("SELECT count(*)::int n FROM olos_export.projection_outbox WHERE state='uncertain'")).rows[0].n).toBe(0);
    expect((await pool.query('SELECT xmin::text FROM raw.olos_calls WHERE internal_id_olos=$1',[fixture(601).AttemptsRawDataID])).rows[0]).toEqual(before);
  },30000);
  it('rejects public files, symlinks and production-style destination identities',async()=>{
    await chmod(inputs.input,0o644);await expect(privateText(inputs.input)).rejects.toThrow('PRIVATE_FILE_REQUIRED');await chmod(inputs.input,0o600);
    const link=join(dir,'linked.json');await symlink(inputs.input,link);await expect(privateText(link)).rejects.toThrow('PRIVATE_FILE_REQUIRED');
    await expect(offlineCycle(root,{...state,id:'00000000-0000-4000-8000-000000000000'},pool,inputs)).rejects.toThrow('ISOLATED_DATABASE_REQUIRED');
  });
  it('CLI produces a clean explicit profile and rejects SYNC_ENABLED=true',async()=>{
    const yes=await exec('node',['--import','tsx','adapters/offline-homologation.ts','profile-example'],{env:{...process.env,SYNC_ENABLED:'false'}});expect(profileSchema.parse(JSON.parse(yes.stdout))).toEqual(exampleProfile);
    await expect(exec('node',['--import','tsx','adapters/offline-homologation.ts','profile-example'],{env:{...process.env,SYNC_ENABLED:'true'}})).rejects.toMatchObject({code:1,stderr:expect.stringContaining('OFFLINE_HOMOLOGATION_STOPPED')});
  });
});
