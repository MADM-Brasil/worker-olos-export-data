import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { randomUUID, createHash } from 'node:crypto';
import * as fs from 'node:fs/promises';
import { constants } from 'node:fs';
import { resolve, join, isAbsolute, dirname } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import pg from 'pg';
import { z } from 'zod';
import { LocalSpool } from './local-spool.js';
import { dispositionPlan, ControlledDispositionSource, replayDisposition } from '../src/integration/disposition.js';
import { approveDispositionEndpoint, approvalSchema, sourceConfigSchema, type DispositionApproval } from '../src/olos/tds/disposition-access.js';
import { authorizeExceptionalDispositionEndpoint, exceptionalSourceSchema, exceptionalTlsMode, validateExceptionalApproval,
  type ExceptionalApproval } from '../src/olos/tds/disposition-exception.js';
import { dispositionTlsMode, exceptionalConfigSchema, exceptionalConfigurationDigest } from './disposition-exception-policy.js';
import { LargeBatchCoordinator } from '../src/large-batch/coordinator.js';
import { spoolLimits } from '../src/large-batch/config.js';
import { PostgresPersistence } from '../src/persistence/store.js';
import { PostgresLargeBatchPersistence } from '../src/persistence/large-batch.js';
import { canonical, digest, validDate, prepareRecords } from '../src/persistence/validation.js';
import { PersistenceError, defaultLimits } from '../src/persistence/types.js';
import type { SpoolManifest, WholeBatchWriter } from '../src/large-batch/types.js';
const exec = promisify(execFile), repo = resolve(dirname(fileURLToPath(import.meta.url)),'..');
const limits = spoolLimits({ maxRows: 10000, maxDiskBytes: 64*1024**2, readTimeoutMs: 30000 });
const stateSchema = z.object({ testId: z.string().uuid(), container: z.string().regex(/^[a-f0-9]{64}$/),
  port: z.number().int().min(1024).max(65535), password: z.string().min(1), snapshotDate: z.string() }).strict();
type State = z.infer<typeof stateSchema>;
const configSchema = z.object({ source: sourceConfigSchema.omit({ password: true }), collationHex: z.string().regex(/^[a-f0-9]{10}$/).optional() }).strict();
function fail(): never { throw new Error('CONTROLLED_INTEGRATION_BLOCKED'); }
export async function privateDirectory(root: string, create = false): Promise<string> {
  if (!isAbsolute(root) || resolve(root) !== root || root === repo || root.startsWith(`${repo}/`)) fail();
  if (create) await fs.mkdir(root,{ mode: 0o700 });
  const stat = await fs.lstat(root);
  if (!stat.isDirectory() || stat.isSymbolicLink() || stat.mode & 0o077 || await fs.realpath(root) !== root || stat.uid !== process.getuid?.()) fail();
  return root;
}
export async function privateJson(path: string): Promise<unknown> {
  if (!isAbsolute(path) || path === repo || path.startsWith(`${repo}/`) || await fs.realpath(path) !== path) fail();
  const file = await fs.open(path,constants.O_RDONLY|constants.O_NOFOLLOW);
  try { const stat = await file.stat(); if (!stat.isFile() || stat.mode & 0o077 || stat.uid !== process.getuid?.() || stat.size > 131072) fail(); return JSON.parse(await file.readFile('utf8')); }
  finally { await file.close(); }
}
export async function durableExclusive(path: string, data: unknown): Promise<void> {
  const f = await fs.open(path,constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|constants.O_NOFOLLOW,0o600);
  try { await f.writeFile(JSON.stringify(data)); await f.sync(); } finally { await f.close(); }
  const d = await fs.open(dirname(path),constants.O_RDONLY|constants.O_DIRECTORY); try { await d.sync(); } finally { await d.close(); }
}
function approvalFor(s: State, reference = 'fixture'): DispositionApproval {
  return { testId: s.testId,action: "ReadData('Disposition')",reference,expiresAt: new Date(Date.now()+60*60*1000).toISOString(),snapshotDate: s.snapshotDate };
}
async function inspect(s: State): Promise<void> {
  const data = JSON.parse((await exec('docker',['inspect',s.container])).stdout)[0];
  const ports = data?.NetworkSettings?.Ports?.['5432/tcp'];
  if (data?.Id !== s.container || data?.Config?.Labels?.['worker-olos-export-data.integration'] !== s.testId || data?.Config?.Image !== 'postgres:16-bookworm' ||
      !data?.State?.Running || ports?.length !== 1 || ports[0].HostIp !== '127.0.0.1' || Number(ports[0].HostPort) !== s.port ||
      !data.Config.Env.includes('POSTGRES_DB=phase4_worker_fixture') || !data.Config.Env.includes(`POSTGRES_PASSWORD=${s.password}`)) fail();
}
async function pool(s: State, worker: boolean): Promise<pg.Pool> {
  await inspect(s);
  return new pg.Pool({ host: '127.0.0.1',port: s.port,user: 'postgres',password: s.password,database: 'phase4_worker_fixture',ssl: false,
    max: 2, connectionTimeoutMillis: 2000,query_timeout: 10000,options: worker ? '-c role=olos_export_worker' : '' });
}
export async function prepare(root: string, snapshotDate: string): Promise<State> {
  if (!validDate(snapshotDate)) fail(); await privateDirectory(root,true);
  const testId = randomUUID(), password = randomUUID();
  await durableExclusive(join(root,'preparing.json'),{ testId });
  const container = (await exec('docker',['run','-d','--label',`worker-olos-export-data.integration=${testId}`,'--memory','512m','--cpus','1',
    '-p','127.0.0.1::5432','-e','POSTGRES_DB=phase4_worker_fixture','-e','POSTGRES_PASSWORD','postgres:16-bookworm'],
    { env: { ...process.env,POSTGRES_PASSWORD: password },timeout: 120000 })).stdout.trim();
  if (!/^[a-f0-9]{64}$/.test(container)) fail();
  const port = Number((await exec('docker',['inspect','--format','{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}',container])).stdout.trim());
  const s = stateSchema.parse({ testId,password,container,port,snapshotDate });
  await durableExclusive(join(root,'state.json'),s); // preserva evidência se migração/preparo falhar
  const admin = await pool(s,false);
  try {
    let ready = false;
    for (let i=0;i<50;i++) { try { await admin.query('SELECT 1'); ready = true; break; } catch { await new Promise(r => setTimeout(r,200)); } }
    if (!ready) fail();
    for (const file of ['migrations/001_control.sql','migrations/002_operational_envelopes.sql','migrations/005_large_batch_control.sql','permissions/worker.sql','permissions/large-batch.sql'])
      await admin.query(await fs.readFile(join(repo,file),'utf8'));
    const c = dispositionPlan(approvalFor(s)).contract;
    await admin.query(`INSERT INTO olos_export.source_contracts(source_table,version,approved,fields,key_fields,layout_evidence,key_evidence) VALUES ($1,$2,true,$3::jsonb,$4,$5,$6)`,[c.source_table,c.version,JSON.stringify(c.fields),c.key_fields,c.layout_evidence,c.key_evidence]);
    await durableExclusive(join(root,'ready.json'),{ testId });
  } finally { await admin.end(); }
  return s;
}
async function state(root: string): Promise<State> {
  await privateDirectory(root); const s = stateSchema.parse(await privateJson(join(root,'state.json')));
  const ready = await privateJson(join(root,'ready.json')) as { testId?: string }; if (ready.testId !== s.testId) fail(); return s;
}
export async function assertNoSourceAttempt(root: string): Promise<void> {
  try { await fs.lstat(join(root,'source-attempt.json')); }
  catch (error) { if ((error as NodeJS.ErrnoException).code === 'ENOENT') return; throw error; }
  fail();
}
export async function once(root: string, approval: DispositionApproval, operation: () => Promise<unknown>): Promise<unknown> {
  const a = approvalSchema.parse(approval);
  if (Date.parse(a.expiresAt) <= Date.now() || Date.parse(a.expiresAt) > Date.now()+12*3600000) fail();
  await durableExclusive(join(root,'source-attempt.json'),{ testId: a.testId,reference: a.reference,action: a.action,claimedAt: new Date().toISOString() });
  return operation(); // o claim nunca é apagado, nem após timeout/conexão falha
}
export async function onceExceptional<T>(root: string, approval: ExceptionalApproval, operation: () => Promise<T>): Promise<T> {
  const a = validateExceptionalApproval(approval,approval.testId);
  // The same exclusive filename fences both verified and exceptional attempts.
  await durableExclusive(join(root,'source-attempt.json'),{ ...a,claimedAt: new Date().toISOString(),serverIdentityValidated: false });
  return operation();
}
export async function assertFreshTest(p: pg.Pool): Promise<void> {
  const row = (await p.query('SELECT EXISTS(SELECT 1 FROM olos_export.runs) OR EXISTS(SELECT 1 FROM olos_export.disposition) AS used')).rows[0];
  if (row?.used !== false) fail();
}
export async function components(root: string, p: pg.Pool, fault = false) {
  const spool = await LocalSpool.open(join(root,'spool'),limits);
  const target = new PostgresLargeBatchPersistence(p,limits,300000);
  const writer: WholeBatchWriter = fault ? { preflight: plan => target.preflight(plan),persist: async () => { throw new PersistenceError('PERSISTENCE_FAILED'); } } : target;
  const coordinator = new LargeBatchCoordinator(new PostgresPersistence(p,{ leaseMs: 300000 }),spool,writer,limits);
  return { spool,target,coordinator };
}
export async function retained(root: string): Promise<SpoolManifest> {
  const paths = await fs.readdir(join(root,'spool','batches'));
  if (paths.length !== 1 || !/^[a-f0-9-]{36}$/.test(paths[0]!)) fail();
  const m = await privateJson(join(root,'spool','batches',paths[0]!,'manifest.json')) as SpoolManifest;
  if (m.identity?.batchId !== paths[0] || !m.sealed || m.identity.plan.run.table !== 'Disposition') fail(); return m;
}
export async function reconcile(p: pg.Pool, m: SpoolManifest, spool: LocalSpool) {
  const original = []; for await (const row of spool.records(m)) original.push(row);
  const envelopes = (await p.query('SELECT source_key,payload FROM olos_export.disposition')).rows;
  for (const envelope of envelopes) if (prepareRecords([envelope.payload],m.identity.plan.contract,defaultLimits).rows[0]?.key !== envelope.source_key) fail();
  const rows = envelopes.map(r => r.payload);
  const sort = (a: Record<string,unknown>[]) => a.map(row => canonical(row)).sort();
  if (canonical(sort(original)) !== canonical(sort(rows))) fail();
  const cp = (await p.query('SELECT version,last_batch_id FROM olos_export.checkpoints WHERE stream_id=$1',[m.identity.streamId])).rows[0];
  if (cp?.version !== (BigInt(m.identity.expected.version)+1n).toString() || cp.last_batch_id !== m.identity.batchId) fail();
  const batch = (await p.query('SELECT state,received_count,persisted_count,duplicate_count FROM olos_export.batches WHERE id=$1',[m.identity.batchId])).rows[0];
  if (batch?.state !== 'committed' || batch.received_count !== original.length || batch.persisted_count !== original.length || batch.duplicate_count !== 0) fail();
  return { records: original.length,checkpoint: cp.version,contentDigest: digest(sort(original)),batchId: m.identity.batchId };
}
function fingerprint(c: z.infer<typeof configSchema>) { const publicConfig = c.source; return createHash('sha256').update(canonical({ source: publicConfig,collationHex: c.collationHex ?? null })).digest('hex'); }
async function command(args: string[]) {
  const action = args.shift(); const flags: Record<string,string> = {};
  while (args.length) { const key = args.shift()!,value = args.shift(); if (!key.startsWith('--') || !value || Object.hasOwn(flags,key)) fail(); flags[key] = value; }
  const allowed: Record<string,string[]> = { prepare: ['--work-dir','--snapshot-date'],approve: ['--work-dir','--source-config','--reference','--expires-at','--confirm','--tls-mode','--tls-risk-confirmation','--operator'],
    read: ['--work-dir','--source-config','--confirm','--simulate-destination-failure','--tls-mode','--tls-risk-confirmation'],replay: ['--work-dir'],recover: ['--work-dir'],status: ['--work-dir'] };
  if (!action || !allowed[action] || Object.keys(flags).some(k => !allowed[action]!.includes(k)) || !flags['--work-dir'] || process.env.SYNC_ENABLED !== 'false' || process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') fail();
  const root = flags['--work-dir'];
  if (action === 'prepare') { const s = await prepare(root,flags['--snapshot-date'] ?? ''); return { prepared: true,testId: s.testId }; }
  const s = await state(root);
  if (action === 'approve' || action === 'read') {
    const tlsMode = dispositionTlsMode(flags);
    if (tlsMode === exceptionalTlsMode) {
      await assertNoSourceAttempt(root);
      await inspect(s); // Match the existing container and loopback-only destination before any source capability.
      const c = exceptionalConfigSchema.parse(await privateJson(flags['--source-config'] ?? ''));
      const source = exceptionalSourceSchema.parse({ ...c.source,password: process.env.PHASE4_OLOS_PASSWORD });
      const configurationDigest = exceptionalConfigurationDigest(s,c,source.password);
      if (action === 'approve') {
        if (flags['--confirm'] !== 'AUTHORIZE_READ_DISPOSITION_ONCE' || !flags['--reference'] || !flags['--operator']) fail();
        const a = validateExceptionalApproval({ ...approvalFor(s,flags['--reference']),operator: flags['--operator'],issuedAt: new Date().toISOString(),
          expiresAt: flags['--expires-at'],tlsMode,riskConfirmation: flags['--tls-risk-confirmation'],configurationDigest },s.testId);
        await durableExclusive(join(root,'exceptional-tls-approval.json'),a);
        return { approved: true,testId: s.testId,reference: a.reference,tlsMode,serverIdentityValidated: false };
      }
      if (flags['--confirm'] !== 'READ_DISPOSITION_ONCE' || flags['--simulate-destination-failure'] && flags['--simulate-destination-failure'] !== 'true') fail();
      const a = validateExceptionalApproval(await privateJson(join(root,'exceptional-tls-approval.json')),s.testId);
      if (a.configurationDigest !== configurationDigest || a.snapshotDate !== s.snapshotDate) fail();
      const plan = dispositionPlan(a), p = await pool(s,true), abort = new AbortController(), cancel = () => abort.abort();
      process.once('SIGINT',cancel); process.once('SIGTERM',cancel);
      try {
        const b = await components(root,p,flags['--simulate-destination-failure'] === 'true');
        await b.target.preflight(plan); await assertFreshTest(p);
        return await onceExceptional(root,a,async () => {
          // Recheck expiry, credential/configuration and destination after claiming, immediately before the source.
          await inspect(s);
          const latest = exceptionalConfigSchema.parse(await privateJson(flags['--source-config']!));
          if (exceptionalConfigurationDigest(s,latest,process.env.PHASE4_OLOS_PASSWORD) !== a.configurationDigest) fail();
          await durableExclusive(join(root,'source-transport-policy.json'),{ testId: s.testId,reference: a.reference,operator: a.operator,
            tlsMode,encrypt: true,trustServerCertificate: true,minVersion: 'TLSv1.2',serverIdentityValidated: false,configurationDigest });
          const endpoint = authorizeExceptionalDispositionEndpoint(source,a,a);
          const reader = new ControlledDispositionSource(endpoint,plan,c.collationHex);
          try { await b.coordinator.receive(plan,reader,abort.signal); }
          finally {
            await durableExclusive(join(root,'source-transport-result.json'),{ testId: s.testId,tlsMode,serverIdentityValidated: false,
              secureHandshakeObserved: reader.transportSecurity.length === 1,observations: reader.transportSecurity });
            if (reader.observedMetadata.length) await durableExclusive(join(root,'source-metadata.json'),{ testId: s.testId,columns: reader.observedMetadata });
          }
          return { committed: true,tlsMode,serverIdentityValidated: false,...await reconcile(p,await retained(root),b.spool) };
        });
      } finally { process.removeListener('SIGINT',cancel); process.removeListener('SIGTERM',cancel); await p.end(); }
    }
    const c = configSchema.parse(await privateJson(flags['--source-config'] ?? ''));
    if (action === 'approve') {
      if (flags['--confirm'] !== 'AUTHORIZE_READ_DISPOSITION_ONCE' || !flags['--reference']) fail();
      const a = approvalSchema.parse({ ...approvalFor(s,flags['--reference']),expiresAt: flags['--expires-at'] });
      approveDispositionEndpoint({ ...c.source,password: process.env.PHASE4_OLOS_PASSWORD },a,s.testId); // valida somente, zero rede
      await durableExclusive(join(root,'approval.json'),{ approval: a,sourceDigest: fingerprint(c) }); return { approved: true,testId: s.testId,reference: a.reference };
    }
    if (flags['--confirm'] !== 'READ_DISPOSITION_ONCE' || flags['--simulate-destination-failure'] && flags['--simulate-destination-failure'] !== 'true') fail();
    const record = await privateJson(join(root,'approval.json')) as { approval: DispositionApproval; sourceDigest: string };
    if (record.sourceDigest !== fingerprint(c) || record.approval.snapshotDate !== s.snapshotDate) fail();
    const endpoint = approveDispositionEndpoint({ ...c.source,password: process.env.PHASE4_OLOS_PASSWORD },record.approval,s.testId), plan = dispositionPlan(record.approval);
    const p = await pool(s,true), abort = new AbortController(); const cancel = () => abort.abort();
    process.once('SIGINT',cancel); process.once('SIGTERM',cancel);
    try {
      const b = await components(root,p,flags['--simulate-destination-failure'] === 'true'); await b.target.preflight(plan); await assertFreshTest(p);
      const source = new ControlledDispositionSource(endpoint,plan,c.collationHex);
      try { await once(root,record.approval,() => b.coordinator.receive(plan,source,abort.signal)); }
      finally { if (source.observedMetadata.length) await durableExclusive(join(root,'source-metadata.json'),{ testId: s.testId,columns: source.observedMetadata }); }
      return { committed: true,...await reconcile(p,await retained(root),b.spool) };
    } finally { process.removeListener('SIGINT',cancel); process.removeListener('SIGTERM',cancel); await p.end(); }
  }
  // Não carrega configuração/credenciais da Olos nestes comandos.
  const p = await pool(s,true);
  try {
    const b = await components(root,p), signal = new AbortController().signal;
    if (action === 'status') { const m = await b.spool.active(); return { testId: s.testId,pending: m !== null,state: m?.state ?? null }; }
    const m = await retained(root);
    if (action === 'recover') { const result = await b.coordinator.recover(signal); if (!result.result) fail(); }
    else {
      const before = await reconcile(p,m,b.spool);
      await replayDisposition(m,b.spool,b.target,signal);
      const after = await reconcile(p,m,b.spool); if (canonical(before) !== canonical(after)) fail();
    }
    return { verified: true,...await reconcile(p,m,b.spool) };
  } finally { await p.end(); }
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try { process.stdout.write(JSON.stringify({ ok: true,...await command(process.argv.slice(2)) })+'\n'); }
  catch { process.stderr.write('{"ok":false,"code":"CONTROLLED_INTEGRATION_STOPPED","retrySource":false}\n'); process.exitCode = 1; }
}
