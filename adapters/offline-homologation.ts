import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import * as fs from 'node:fs/promises';
import { constants } from 'node:fs';
import { resolve, join, dirname, isAbsolute } from 'node:path';
import { pathToFileURL, fileURLToPath } from 'node:url';
import { randomUUID, createHash } from 'node:crypto';
import pg from 'pg';
import { z } from 'zod';
import { LocalSpool } from './local-spool.js';
import { profileSchema, exampleProfile, columnTypes, parseLossless, recordsFromJson, originalId, catalog, mapOffline, type OfflineProfile, type Catalogs } from '../src/offline/model.js';
import { offlineDelivery, type OfflineJob } from '../src/offline/store.js';
import { PostgresPersistence } from '../src/persistence/store.js';
import { canonical, digest, validDate } from '../src/persistence/validation.js';
import { spoolLimits } from '../src/large-batch/config.js';
import { Scheduler } from '../src/sync/scheduler.js';
import type { SourceContract } from '../src/persistence/types.js';
import type { SpoolManifest, LargePlan } from '../src/large-batch/types.js';
import { transaction } from '../src/persistence/transaction.js';
import { parseAgentCatalog,agentMap } from '../src/offline/agents.js';
import { agentCatalogInstalled,loadAgentMap,updateAgentCatalog } from '../src/offline/agent-store.js';

const exec = promisify(execFile), repo = resolve(dirname(fileURLToPath(import.meta.url)),'..');
export const offlineContract: SourceContract = { source_table: 'AttemptsRawData',version: 'offline-json-archive/v1',approved: true,
  fields: { AttemptsRawDataID: { type: 'int64',nullable: false },source_record: { type: 'text',nullable: false },source_file_digest: { type: 'text',nullable: false } },
  key_fields: ['AttemptsRawDataID'],layout_evidence: 'OFFLINE ONLY: exact JSON archived separately; no assertion of TDS temporal fidelity',key_evidence: 'explicit AttemptsRawDataID from historical JSON; isolated namespace only' };
const limits = spoolLimits({ maxRecordBytes: 4*1024**2,chunkBytes: 8*1024**2 });
const stateSchema = z.object({ id: z.string().uuid(),container: z.string().regex(/^[a-f0-9]{64}$/),port: z.number().int().min(1024).max(65535),password: z.string().min(1),profile: profileSchema }).strict();
export type OfflineState = z.infer<typeof stateSchema>;
function fail(code = 'OFFLINE_HOMOLOGATION_BLOCKED'): never { throw new Error(code); }
export async function privateRoot(root: string, create = false) {
  if (!isAbsolute(root) || resolve(root)!==root || root===repo || root.startsWith(`${repo}/`)) fail();
  if (create) await fs.mkdir(root,{ mode: 0o700 });
  const stat = await fs.lstat(root);
  if (!stat.isDirectory() || stat.isSymbolicLink() || stat.mode&0o077 || stat.uid!==process.getuid?.() || await fs.realpath(root)!==root) fail();
}
export async function privateText(path: string, max = 32*1024**2): Promise<string> {
  if (!isAbsolute(path) || resolve(path)!==path || path===repo || path.startsWith(`${repo}/`) || await fs.realpath(path)!==path) fail('PRIVATE_FILE_REQUIRED');
  const f = await fs.open(path,constants.O_RDONLY|constants.O_NOFOLLOW);
  try {
    const s = await f.stat(); if (!s.isFile() || s.mode&0o077 || s.uid!==process.getuid?.() || s.size>max) fail('PRIVATE_FILE_REQUIRED');
    const buffer = Buffer.alloc(Math.min(max+1,s.size+1)), read = await f.read(buffer,0,buffer.length,0);
    if (read.bytesRead!==s.size || (await f.stat()).size!==s.size) fail('INPUT_CHANGED_DURING_READ');
    return new TextDecoder('utf-8',{ fatal: true,ignoreBOM: true }).decode(buffer.subarray(0,read.bytesRead));
  } finally { await f.close(); }
}
export async function exclusive(path: string, data: string) {
  const f = await fs.open(path,constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|constants.O_NOFOLLOW,0o600);
  try { await f.writeFile(data); await f.sync(); } finally { await f.close(); }
  const d = await fs.open(dirname(path),constants.O_RDONLY|constants.O_DIRECTORY); try { await d.sync(); } finally { await d.close(); }
}
export async function assertOfflineDestination(s: OfflineState) {
  const d = JSON.parse((await exec('docker',['inspect',s.container],{ timeout: 10000 })).stdout)[0], ports = d?.NetworkSettings?.Ports?.['5432/tcp'];
  if (d?.Id!==s.container || d.Config?.Labels?.['worker-olos-export-data.offline']!==s.id || d.Config?.Image!=='postgres:16-bookworm' || !d.State?.Running ||
    ports?.length!==1 || ports[0].HostIp!=='127.0.0.1' || Number(ports[0].HostPort)!==s.port ||
    !d.Config.Env.includes('POSTGRES_DB=olos_offline_homolog') || !d.Config.Env.includes(`POSTGRES_PASSWORD=${s.password}`)) fail('ISOLATED_DATABASE_REQUIRED');
}
export async function offlinePool(s: OfflineState, worker = true) {
  await assertOfflineDestination(s);
  const pool=new pg.Pool({ host: '127.0.0.1',port: s.port,user: 'postgres',password: s.password,database: 'olos_offline_homolog',ssl: false,max: 5,
    connectionTimeoutMillis: 2000,query_timeout: 15000,options: worker ? '-c role=olos_offline_homolog' : '' });
  pool.on('error',() => { process.stderr.write('{"event":"offline_postgres_dependency_error","retrySource":false}\n'); });
  return pool;
}
export async function prepareOffline(root: string, input: unknown): Promise<OfflineState> {
  const profile = profileSchema.parse(input); await privateRoot(root,true);
  const id = randomUUID(), password = randomUUID(); await exclusive(join(root,'preparing.json'),JSON.stringify({ id }));
  const container = (await exec('docker',['run','-d','--label',`worker-olos-export-data.offline=${id}`,'--memory','768m','--cpus','2','-p','127.0.0.1::5432',
    '-e','POSTGRES_DB=olos_offline_homolog','-e','POSTGRES_PASSWORD','postgres:16-bookworm'],{ env: { ...process.env,POSTGRES_PASSWORD: password },timeout: 120000 })).stdout.trim();
  if (!/^[a-f0-9]{64}$/.test(container)) fail();
  const port = Number((await exec('docker',['inspect','--format','{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}',container])).stdout.trim());
  const s = stateSchema.parse({ id,password,container,port,profile }); await exclusive(join(root,'state.json'),JSON.stringify(s));
  const p = await offlinePool(s,false);
  try {
    let ready = false; for (let i=0;i<50;i++) { try { await p.query('SELECT 1'); ready=true; break; } catch { await new Promise(r => setTimeout(r,200)); } } if (!ready) fail();
    for (const file of ['migrations/001_control.sql','migrations/002_operational_envelopes.sql','migrations/003_projection_control.sql','migrations/004_delivery_receipts.sql',
      'permissions/worker.sql','permissions/projection.sql','permissions/receiver.sql','homologation/offline-control.sql','homologation/001_global_agents.sql']) await p.query(await fs.readFile(join(repo,file),'utf8'));
    await p.query(`CREATE TABLE raw.olos_calls(${profile.columns.map(c => `"${c}" ${columnTypes[c]}${c==='internal_id_olos' ? ' PRIMARY KEY' : ''}`).join(',')})`);
    await p.query('GRANT SELECT,INSERT,UPDATE ON raw.olos_calls TO olos_offline_homolog');
    const c = offlineContract;
    await p.query('INSERT INTO olos_export.source_contracts(source_table,version,approved,fields,key_fields,layout_evidence,key_evidence) VALUES($1,$2,true,$3::jsonb,$4,$5,$6)',
      [c.source_table,c.version,JSON.stringify(c.fields),c.key_fields,c.layout_evidence,c.key_evidence]);
    await exclusive(join(root,'ready.json'),JSON.stringify({ id,digest: digest(profile) }));
  } finally { await p.end(); }
  return s;
}
async function loadState(root: string) {
  await privateRoot(root); const s = stateSchema.parse(JSON.parse(await privateText(join(root,'state.json')))), ready = JSON.parse(await privateText(join(root,'ready.json')));
  if (ready.id!==s.id || ready.digest!==digest(s.profile)) fail(); return s;
}
export interface OfflineInputs { input: string; campaigns: string; dispositions: string; users?: string; exportDate: string; expectedCount?: number; expectedCpfs?: number; expectedEmails?: number; }
async function archive(root: string, text: string) {
  const hash = createHash('sha256').update(text).digest('hex'), dir = join(root,'archive');
  await fs.mkdir(dir,{ mode: 0o700 }).catch(e => { if (e.code!=='EEXIST') throw e; });
  const dirStat=await fs.lstat(dir); if (!dirStat.isDirectory() || dirStat.isSymbolicLink() || dirStat.mode&0o077 || await fs.realpath(dir)!==dir) fail();
  const path = join(dir,`${hash}.json`);
  try { await exclusive(path,text); } catch (e) { if ((e as NodeJS.ErrnoException).code!=='EEXIST' || await privateText(path)!==text) throw e; }
  return hash;
}
const jobSelect = 'id,input_digest,mapping_digest,run_id,stream_id,fencing_token,batch_id,spool_manifest,expected,confirmed,metrics,loaded_at::text AS loaded_at';
export async function offlineCycle(root: string, s: OfflineState, p: pg.Pool, inputs: OfflineInputs, signal = new AbortController().signal) {
  await assertOfflineDestination(s);
  const lock = await p.connect(); let acquired = false, lost = false; const lostLock = () => { lost=true; }; lock.on('error',lostLock);
  try {
    acquired = (await lock.query("SELECT pg_try_advisory_lock(740210,1) AS acquired")).rows[0].acquired;
    if (!acquired) fail('OFFLINE_WORKER_BUSY');
    if (!validDate(inputs.exportDate)) fail('EXPORT_DATE_REQUIRED');
    const [text,campaignText,dispositionText,userText] = await Promise.all([privateText(inputs.input),privateText(inputs.campaigns),privateText(inputs.dispositions),inputs.users ? privateText(inputs.users) : Promise.resolve(null)]);
    const rows = recordsFromJson(parseLossless(text)), cp = catalog(recordsFromJson(parseLossless(campaignText),true),'CampaignId','Description'), dp = catalog(recordsFromJson(parseLossless(dispositionText),true),'DispositionId','Description');
    const snapshot=userText ? parseAgentCatalog(userText) : null;
    const installed=await agentCatalogInstalled(lock);
    if (snapshot && installed) {
      const active=(await lock.query('SELECT snapshot_digest FROM olos_export.offline_agent_state WHERE singleton')).rows[0].snapshot_digest;
      if (active!==null && active!==snapshot.digest) fail('AGENT_CATALOG_UPDATE_REQUIRES_APPROVAL');
      // Borrow the session holding ingestion's lock; transaction release is deferred to this scope.
      await updateAgentCatalog({connect:async()=>({query:lock.query.bind(lock),release:(destroy?:boolean)=>{if(destroy)lost=true;}} as pg.PoolClient)},snapshot);
    }
    const catalogs: Catalogs = { campaigns: cp,dispositions: dp,agents: await loadAgentMap(lock) ?? (snapshot ? agentMap(snapshot) : new Map()) };
    const unique = new Map<string,string>();
    for (const row of rows) { const id=originalId(row), body=canonical(row); if (unique.has(id) && unique.get(id)!==body) fail('CONFLICTING_DUPLICATE_ID'); unique.set(id,body); }
    if (inputs.expectedCount!==undefined && unique.size!==inputs.expectedCount) fail('EXPECTED_COUNT_MISMATCH');
    const loadedAt = new Date().toISOString().slice(0,-1), metrics = { cpf: 0,email: 0,tma: 0,invalidXml: 0,ambiguousXml: 0,missingTimestamp: 0,missingCampaign: 0,missingDisposition: 0 };
    for (const body of unique.values()) { const mapped=mapOffline(parseLossless(body) as import('../src/offline/model.js').OfflineRecord,catalogs,s.profile,loadedAt);
      for (const key of Object.keys(metrics) as (keyof typeof metrics)[]) metrics[key]+=mapped.metrics[key]; }
    if (inputs.expectedCpfs!==undefined && inputs.expectedCpfs!==metrics.cpf || inputs.expectedEmails!==undefined && inputs.expectedEmails!==metrics.email) fail('EXPECTED_ENRICHMENT_COUNT_MISMATCH');
    const inputDigest=await archive(root,text), campaignDigest=await archive(root,campaignText), dispositionDigest=await archive(root,dispositionText);
    // Only the sanitized two-column representation can ever be archived. Global updates do not create source checkpoints.
    const userDigest=snapshot && !installed ? await archive(root,canonical(snapshot.entries)) : null;
    const mappingDigest = digest({ profile: s.profile,campaignDigest,dispositionDigest,userDigest,exportDate: inputs.exportDate });
    let job=(await p.query<OfflineJob>(`SELECT ${jobSelect} FROM olos_export.offline_imports WHERE input_digest=$1 AND mapping_digest=$2`,[inputDigest,mappingDigest])).rows[0];
    let core: PostgresPersistence;
    if (!job) {
      if ((await p.query('SELECT 1 FROM olos_export.offline_imports WHERE completed_at IS NULL LIMIT 1')).rowCount) fail('RECOVER_PREVIOUS_OFFLINE_IMPORT_FIRST');
      if (signal.aborted || lost) fail('OFFLINE_STOPPED');
      const id=randomUUID(), destination=`offline:${s.id}:${id}`, mappingVersion=`offline:${mappingDigest}`;
      core = new PostgresPersistence(p,{ maxRecords: 10000,maxBytes: 64*1024**2,chunkSize: 500,leaseMs: 300000 },{ sourceNamespace: `ExportDataV3/offline/${s.id}`,destinationId: destination,mappingVersion });
      const handle=await core.startRun({ table: 'AttemptsRawData',contractVersion: offlineContract.version,readType: 'investigation',exportDate: inputs.exportDate,pkRange: null },signal), expected=await core.checkpoint(handle);
      const batchId=randomUUID();
      await p.query('INSERT INTO olos_export.offline_imports(id,input_digest,mapping_digest,run_id,stream_id,fencing_token,batch_id,expected,metrics,loaded_at) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9::jsonb,$10)',
        [id,inputDigest,mappingDigest,handle.runId,handle.streamId,handle.fencingToken,batchId,unique.size,JSON.stringify(metrics),loadedAt]);
      // Evidence-only plan: no executable signature, parameters or TDS fidelity assertion.
      const plan: LargePlan = { run: { table: 'AttemptsRawData',contractVersion: offlineContract.version,readType: 'investigation',exportDate: inputs.exportDate,pkRange: null },
        contract: offlineContract,fields: [],signature: { procedure: 'ReReadData',version: 'offline-json/non-executable',evidence: 'operator-declared historical JSON; no SQL execution',parameters: [],messageOutput: 'Message',errorOutput: 'ErrorMessage' },
        parameters: {},successReturnValues: [],acquisitionEvidence: 'private original JSON archive',responseEvidence: 'offline file, not a SQL response',partitionEvidence: 'operator-supplied artifact label, not source consumption cursor' };
      const spool=await LocalSpool.open(join(root,'spools',id),limits), writer=await spool.create({ format: 'olos-spool/v1',batchId,runId: handle.runId,streamId: handle.streamId,fencingToken: handle.fencingToken,expected,plan });
      try { for (const [key,body] of unique) await writer.append({ AttemptsRawDataID: key,source_record: body,source_file_digest: inputDigest });
        const sealed=await writer.seal(); await p.query('UPDATE olos_export.offline_imports SET spool_manifest=$2::jsonb WHERE id=$1',[id,JSON.stringify(sealed)]);
      } catch (e) { await writer.interrupt(); throw e; }
      job=(await p.query<OfflineJob>(`SELECT ${jobSelect} FROM olos_export.offline_imports WHERE id=$1`,[id])).rows[0]!;
    }
    const destination=`offline:${s.id}:${job.id}`, mappingVersion=`offline:${mappingDigest}`;
    core = new PostgresPersistence(p,{ maxRecords: 10000,maxBytes: 64*1024**2,chunkSize: 500,leaseMs: 300000 },{ sourceNamespace: `ExportDataV3/offline/${s.id}`,destinationId: destination,mappingVersion });
    if (!job.spool_manifest) fail('OFFLINE_IMPORT_REQUIRES_REVIEW');
    const m=job.spool_manifest as SpoolManifest, spool=await LocalSpool.open(join(root,'spools',job.id),limits), records=[];
    for await (const record of spool.records(m)) records.push(record); // Bounded by private input/10k rows, validated to EOF before receipt.
    if (signal.aborted || lost) fail('OFFLINE_STOPPED');
    const handle={ runId: job.run_id,streamId: job.stream_id,fencingToken: job.fencing_token,signal }, batch={ id: job.batch_id,records,expected: m.identity.expected,nextCursor: inputDigest };
    const prior=(await p.query('SELECT state FROM olos_export.batches WHERE id=$1',[job.batch_id])).rows[0];
    if (!prior) await core.observeBatch(handle,batch);
    await core.persistBatch(handle,batch);
    const run=(await p.query('SELECT state FROM olos_export.runs WHERE id=$1',[job.run_id])).rows[0];
    if (run?.state!=='succeeded') await core.finishRun(handle);
    const d=offlineDelivery(p,destination,s.profile,catalogs,job.loaded_at);
    const uncertain=(await p.query<{ delivery_id: string }>('SELECT delivery_id FROM olos_export.projection_outbox WHERE destination_id=$1 AND state=\'uncertain\' AND (lease_until IS NULL OR lease_until<=clock_timestamp())',[destination])).rows;
    for (const row of uncertain) {
      await d.dispatcher.reconcile(row.delivery_id);
      // Same-database, same-UUID retry is safe ONLY for this transactional receipt writer.
      await transaction(p,async client => { await client.query(`UPDATE olos_export.projection_outbox o SET state='pending',available_at=clock_timestamp(),last_code='PROJECTION_PENDING'
        WHERE o.delivery_id=$1 AND o.destination_id=$2 AND o.state='uncertain' AND o.last_code='RECEIPT_ABSENT'
        AND NOT EXISTS(SELECT 1 FROM olos_export.delivery_receipts r WHERE r.delivery_id=o.delivery_id AND r.destination_id=o.destination_id)`,[row.delivery_id,destination]); });
    }
    for (let i=0;i<unique.size;i++) { if (signal.aborted || lost) fail('OFFLINE_STOPPED'); const result=await d.dispatcher.runOnce(); if (result==='empty' || result==='pending' || result==='uncertain') break; }
    const progress=(await p.query('SELECT expected,confirmed,completed_at IS NOT NULL AS complete,metrics FROM olos_export.offline_imports WHERE id=$1',[job.id])).rows[0];
    const checkpoint=(await p.query('SELECT version FROM olos_export.checkpoints WHERE stream_id=$1',[job.stream_id])).rows[0];
    const count=(await p.query('SELECT count(*)::text AS rows,count(DISTINCT internal_id_olos)::text AS unique_rows FROM raw.olos_calls')).rows[0];
    return { event: 'offline_cycle_complete',freshness: 'historical-offline',sqlServerQueries: 0,inputRows: rows.length,uniqueInputRows: unique.size,duplicatesInFile: rows.length-unique.size,
      ...progress,...count,ingestionCheckpoint: checkpoint.version,sourceObservedNow: false };
  } finally { if (acquired && !lost) await lock.query('SELECT pg_advisory_unlock(740210,1)').catch(() => {}); lock.removeListener('error',lostLock); lock.release(lost); }
}
export async function runOfflineScheduler(task: (signal: AbortSignal) => Promise<void>, stop: Promise<void>) {
  let signalFailure!: () => void, didFail=false; const failed=new Promise<void>(resolve => { signalFailure=resolve; });
  const scheduler=new Scheduler(task,() => { didFail=true; signalFailure(); },60000);
  await task(new AbortController().signal); scheduler.start();
  try { await Promise.race([stop,failed]); } finally { await scheduler.stop(); }
  if (didFail) fail('OFFLINE_LOCAL_CYCLE_FAILED');
}
async function command(args: string[]) {
  const action=args.shift(), flags: Record<string,string>={};
  while (args.length) { const k=args.shift()!,v=args.shift(); if (!k.startsWith('--') || !v || Object.hasOwn(flags,k)) fail(); flags[k]=v; }
  const allowed: Record<string,string[]>={ 'profile-example': [],'agents-check':['--users'],'agents-update':['--work-dir','--users','--confirm'],prepare: ['--work-dir','--profile','--confirm'],import: ['--work-dir','--input','--campaigns','--dispositions','--users','--export-date','--expect-count','--expect-cpfs','--expect-emails','--confirm'],
    start: ['--work-dir','--input','--campaigns','--dispositions','--users','--export-date','--expect-count','--expect-cpfs','--expect-emails','--confirm'],stop: ['--work-dir'],status: ['--work-dir'] };
  if (!action || !allowed[action] || Object.keys(flags).some(k => !allowed[action]!.includes(k)) || process.env.SYNC_ENABLED!=='false') fail();
  if (action==='profile-example') return exampleProfile;
  if (action==='agents-check') { const a=parseAgentCatalog(await privateText(flags['--users'] ?? ''));return {agents:a.entries.length,received:a.received,skipped:a.skipped,sqlServerQueries:0}; }
  const root=flags['--work-dir']; if (!root) fail();
  if (action==='prepare') { if (flags['--confirm']!=='CREATE_ISOLATED_OFFLINE_DATABASE') fail(); const s=await prepareOffline(root,JSON.parse(await privateText(flags['--profile'] ?? ''))); return { prepared: true,id: s.id }; }
  const s=await loadState(root);
  if (action==='stop') { const r=JSON.parse(await privateText(join(root,'runner.json'))); await exclusive(join(root,'stop-request.json'),JSON.stringify({ nonce: r.nonce })); return { stopRequested: true }; }
  const p=await offlinePool(s);
  try {
    if (action==='agents-update') { if(flags['--confirm']!=='APPLY_SANITIZED_GLOBAL_AGENTS')fail();const snapshot=parseAgentCatalog(await privateText(flags['--users'] ?? ''));
      if(!await agentCatalogInstalled(p))fail('AGENT_CATALOG_SCHEMA_NOT_READY');return await updateAgentCatalog(p,snapshot); }
    if (action==='status') return { freshness: 'historical-offline',sqlServerQueries: 0,jobs: (await p.query('SELECT expected,confirmed,completed_at IS NOT NULL AS complete,metrics FROM olos_export.offline_imports ORDER BY loaded_at')).rows };
    if (flags['--confirm']!==(action==='start' ? 'PROCESS_HISTORICAL_EVERY_60_SECONDS' : 'IMPORT_HISTORICAL_DATA')) fail();
    const integer=(k: string) => { if (flags[k]===undefined) return undefined; if (!/^\d{1,5}$/.test(flags[k]!)) fail(); return Number(flags[k]); };
    const inputs: OfflineInputs={ input: flags['--input'] ?? '',campaigns: flags['--campaigns'] ?? '',dispositions: flags['--dispositions'] ?? '',exportDate: flags['--export-date'] ?? '',
      ...(flags['--users'] ? { users: flags['--users'] } : {}),...(flags['--expect-count'] ? { expectedCount: integer('--expect-count')! } : {}),
      ...(flags['--expect-cpfs'] ? { expectedCpfs: integer('--expect-cpfs')! } : {}),...(flags['--expect-emails'] ? { expectedEmails: integer('--expect-emails')! } : {}) };
    if (action==='import') { const result=await offlineCycle(root,s,p,inputs); if (!result.complete) { process.stdout.write(`${JSON.stringify(result)}\n`); fail('OFFLINE_DELIVERY_PENDING'); } return result; }
    const nonce=randomUUID(); await exclusive(join(root,'runner.json'),JSON.stringify({ nonce,pid: process.pid,id: s.id }));
    let stop!: () => void; const stopped=new Promise<void>(resolve => { stop=resolve; }); const cancel=() => stop();
    process.once('SIGINT',cancel); process.once('SIGTERM',cancel);
    const timer=setInterval(() => { privateText(join(root,'stop-request.json')).then(text => { if (JSON.parse(text).nonce===nonce) stop(); }).catch(() => {}); },1000);
    try { await runOfflineScheduler(async signal => { process.stdout.write(`${JSON.stringify(await offlineCycle(root,s,p,inputs,signal))}\n`); },stopped); }
    finally { clearInterval(timer); process.removeListener('SIGINT',cancel); process.removeListener('SIGTERM',cancel);
      await fs.unlink(join(root,'runner.json')); await fs.unlink(join(root,'stop-request.json')).catch(() => {}); }
    return { stopped: true,freshness: 'historical-offline',sqlServerQueries: 0 };
  } finally { await p.end(); }
}
if (process.argv[1] && import.meta.url===pathToFileURL(resolve(process.argv[1])).href) {
  try { const result=await command(process.argv.slice(2)); process.stdout.write(`${JSON.stringify(process.argv[2]==='profile-example' ? result : { ok: true,...result })}\n`); }
  catch (error) { const codes=['OFFLINE_WORKER_BUSY','RECOVER_PREVIOUS_OFFLINE_IMPORT_FIRST','OFFLINE_TIMESTAMP_REQUIRES_REVIEW','OFFLINE_TEXT_REQUIRES_REVIEW','OFFLINE_SOURCE_TYPE_REQUIRES_REVIEW','OFFLINE_IMPORT_REQUIRES_REVIEW','PRIVATE_FILE_REQUIRED',
    'EXPECTED_COUNT_MISMATCH','EXPECTED_ENRICHMENT_COUNT_MISMATCH','CONFLICTING_DUPLICATE_ID','ISOLATED_DATABASE_REQUIRED','EXPORT_DATE_REQUIRED','OFFLINE_DELIVERY_PENDING','OFFLINE_LOCAL_CYCLE_FAILED',
    'AGENT_CATALOG_SCHEMA_NOT_READY','AGENT_CATALOG_UPDATE_REQUIRES_APPROVAL','AGENT_CATALOG_CONFLICT','UNSAFE_AGENT_CATALOG','USERS_SANITIZATION_REJECTED','AGENT_CATALOG_LIMIT'];
    const code=error instanceof Error && codes.includes(error.message) ? error.message : 'OFFLINE_HOMOLOGATION_STOPPED';
    process.stderr.write(`${JSON.stringify({ ok: false,code,retrySource: false })}\n`); process.exitCode=1; }
}
