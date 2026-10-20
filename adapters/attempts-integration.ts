import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { randomUUID, createHmac } from 'node:crypto';
import * as fs from 'node:fs/promises';
import { join, resolve, dirname } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import pg from 'pg';
import { z } from 'zod';
import { privateDirectory, privateJson, durableExclusive } from './disposition-integration.js';
import { LocalSpool } from './local-spool.js';
import { attemptsBlockers, firstAttemptsAcquisitionRequirements, FirstAttemptsAcquisitionBlocked, requireAttemptsFixture, validateApproval, attemptsFixtureContract, attemptsFixturePlan,
  attemptsFixtureFields, logFixtureFields, signature, type AttemptsApproval } from '../src/integration/attempts/contracts.js';
import { AttemptsFixtureSource, fixtureEndpointSchema } from '../src/integration/attempts/source.js';
import { decodeLogRecord, planAttemptsRecovery, rereadLargePlan, type AttemptsRecoveryPlan, type LogInterval } from '../src/integration/attempts/recovery.js';
import { spoolLimits } from '../src/large-batch/config.js';
import { LargeBatchCoordinator } from '../src/large-batch/coordinator.js';
import { PostgresPersistence } from '../src/persistence/store.js';
import { PostgresLargeBatchPersistence } from '../src/persistence/large-batch.js';
import { canonical, digest, validDate, prepareRecords } from '../src/persistence/validation.js';
import type { LargePlan, SpoolManifest, WholeBatchWriter, SpoolRepository } from '../src/large-batch/types.js';
import { PersistenceError, defaultLimits } from '../src/persistence/types.js';
import { transaction } from '../src/persistence/transaction.js';

const exec = promisify(execFile), repo = resolve(dirname(fileURLToPath(import.meta.url)),'..');
export const attemptsLimits = spoolLimits();
const stateSchema = z.object({ testId: z.string().uuid(),container: z.string().regex(/^[a-f0-9]{64}$/),
  port: z.number().int().min(1024).max(65535),password: z.string().min(1),exportDate: z.string().refine(validDate) }).strict();
export type AttemptsState = z.infer<typeof stateSchema>;
export const attemptsSourceSchema = z.object({ sqlContainer: z.string().regex(/^[a-f0-9]{64}$/),endpoint: fixtureEndpointSchema,
  collationHex: z.string().regex(/^[a-f0-9]{10}$/) }).strict();
type SourceConfig = z.infer<typeof attemptsSourceSchema>;
const sqlImage = 'mcr.microsoft.com/mssql/server@sha256:cf8f0ab2ebe61feec2eed2629061a07d247f791c2a8ddda74a4ac9f06a291718';
function fail(): never { throw new Error('ATTEMPTS_CONTROLLED_TEST_BLOCKED'); }
export async function assertAttemptsDestination(s: AttemptsState): Promise<void> {
  const d = JSON.parse((await exec('docker',['inspect',s.container])).stdout)[0], ports = d?.NetworkSettings?.Ports?.['5432/tcp'];
  if (d?.Id !== s.container || d.Config?.Labels?.['worker-olos-export-data.attempts-test'] !== s.testId || d.Config?.Image !== 'postgres:16-bookworm' || !d.State?.Running ||
    ports?.length !== 1 || ports[0].HostIp !== '127.0.0.1' || Number(ports[0].HostPort) !== s.port ||
    !d.Config.Env.includes('POSTGRES_DB=phase5_attempts_fixture') || !d.Config.Env.includes(`POSTGRES_PASSWORD=${s.password}`)) fail();
}
export async function assertAttemptsSource(c: SourceConfig): Promise<void> {
  const d = JSON.parse((await exec('docker',['inspect',c.sqlContainer])).stdout)[0], ports = d?.NetworkSettings?.Ports?.['1433/tcp'];
  if (d?.Id !== c.sqlContainer || d.Config?.Image !== sqlImage || d.Config?.Labels?.['worker-olos-export-data.test'] !== 'phase3d' || !d.State?.Running ||
    ports?.length !== 1 || ports[0].HostIp !== '127.0.0.1' || Number(ports[0].HostPort) !== c.endpoint.port ||
    !d.Config.Env.includes('MSSQL_PID=Developer') || !d.Config.Env.includes(`MSSQL_SA_PASSWORD=${c.endpoint.password}`)) fail();
}
export async function attemptsPool(s: AttemptsState, worker = true): Promise<pg.Pool> {
  await assertAttemptsDestination(s);
  return new pg.Pool({ host: '127.0.0.1',port: s.port,user: 'postgres',password: s.password,database: 'phase5_attempts_fixture',ssl: false,
    max: 2,connectionTimeoutMillis: 2000,query_timeout: 10000,options: worker ? '-c role=olos_export_worker' : '' });
}
export async function prepareAttempts(root: string, exportDate: string): Promise<AttemptsState> {
  if (!validDate(exportDate)) fail(); await privateDirectory(root,true);
  const testId = randomUUID(), password = randomUUID(); await durableExclusive(join(root,'preparing.json'),{ testId,scope: 'isolated-fixture' });
  const container = (await exec('docker',['run','-d','--label',`worker-olos-export-data.attempts-test=${testId}`,'--memory','512m','--cpus','1',
    '-p','127.0.0.1::5432','-e','POSTGRES_DB=phase5_attempts_fixture','-e','POSTGRES_PASSWORD','postgres:16-bookworm'],
    { env: { ...process.env,POSTGRES_PASSWORD: password },timeout: 120000 })).stdout.trim();
  if (!/^[a-f0-9]{64}$/.test(container)) fail();
  const port = Number((await exec('docker',['inspect','--format','{{(index (index .NetworkSettings.Ports "5432/tcp") 0).HostPort}}',container])).stdout.trim());
  const s = stateSchema.parse({ testId,password,container,port,exportDate }); await durableExclusive(join(root,'state.json'),s);
  const p = await attemptsPool(s,false);
  try {
    let ready = false;
    for (let i=0;i<50;i++) { try { await p.query('SELECT 1'); ready = true; break; } catch { await new Promise(r => setTimeout(r,200)); } }
    if (!ready) fail();
    for (const file of ['migrations/001_control.sql','migrations/002_operational_envelopes.sql','migrations/005_large_batch_control.sql','migrations/006_attempts_recovery_control.sql','permissions/worker.sql','permissions/large-batch.sql','permissions/attempts-recovery.sql']) await p.query(await fs.readFile(join(repo,file),'utf8'));
    const c = attemptsFixtureContract();
    await p.query('INSERT INTO olos_export.source_contracts(source_table,version,approved,fields,key_fields,layout_evidence,key_evidence) VALUES ($1,$2,true,$3::jsonb,$4,$5,$6)',
      [c.source_table,c.version,JSON.stringify(c.fields),c.key_fields,c.layout_evidence,c.key_evidence]);
    await durableExclusive(join(root,'ready.json'),{ testId });
  } finally { await p.end(); }
  return s;
}
async function state(root: string): Promise<AttemptsState> {
  await privateDirectory(root); const s = stateSchema.parse(await privateJson(join(root,'state.json')));
  if ((await privateJson(join(root,'ready.json')) as { testId?: string }).testId !== s.testId) fail(); return s;
}
export async function attemptsManifest(root: string, name = 'spool'): Promise<SpoolManifest> {
  const names = await fs.readdir(join(root,name,'batches'));
  if (names.length !== 1 || !/^[a-f0-9-]{36}$/.test(names[0]!)) fail();
  const m = await privateJson(join(root,name,'batches',names[0]!,'manifest.json')) as SpoolManifest;
  if (m.identity?.plan.run.table !== 'AttemptsRawData' || m.identity.batchId !== names[0]) fail(); return m;
}
export async function onceAttempts<T>(root: string, a: AttemptsApproval, operation: () => Promise<T>): Promise<T> {
  validateApproval(a,a.testId,a.operation,a.configurationDigest);
  await durableExclusive(join(root,`attempt-${a.operation}.json`),{ ...a,claimedAt: new Date().toISOString(),remoteOutcome: 'unknown_until_complete' });
  return operation(); // Never removed after an error, timeout, crash or TLS failure.
}
export async function claimAttemptsRecoveryOperation(p: pg.Pool, origin: SpoolManifest, a: AttemptsApproval): Promise<void> {
  validateApproval(a,a.testId,a.operation,a.configurationDigest);
  if (origin.sealed || origin.state !== 'recovery_required' || !['log','reread'].includes(a.operation)) fail();
  await transaction(p,async client => {
    const versions = (await client.query('SELECT version FROM olos_export.attempts_recovery_schema_versions')).rows;
    if (canonical(versions) !== canonical([{ version: 1 }])) fail();
    const r = (await client.query(`SELECT r.source_table,r.contract_version,r.state,l.owner_run_id,l.generation,
      (l.lease_until>clock_timestamp()) AS live,c.version,c.cursor_value FROM olos_export.runs r
      JOIN olos_export.source_locks l ON l.source_table=r.source_table JOIN olos_export.checkpoints c ON c.stream_id=r.stream_id
      WHERE r.id=$1 AND r.stream_id=$2 FOR UPDATE OF l NOWAIT`,[origin.identity.runId,origin.identity.streamId])).rows[0];
    if (!r || r.source_table !== 'AttemptsRawData' || r.contract_version !== origin.identity.plan.contract.version || !['read_intent','persisting'].includes(r.state) ||
      r.owner_run_id !== origin.identity.runId || r.generation !== origin.identity.fencingToken || !r.live ||
      r.version !== origin.identity.expected.version || r.cursor_value !== origin.identity.expected.cursor) fail();
    await client.query('INSERT INTO olos_export.attempts_recovery_operations(run_id,operation,claim_id,configuration_digest) VALUES ($1,$2,$3,$4)',
      [origin.identity.runId,a.operation,randomUUID(),a.configurationDigest]);
  });
}
export async function attemptsComponents(root: string, p: pg.Pool, name = 'spool', fault = false) {
  const spool = await LocalSpool.open(join(root,name),attemptsLimits), control = new PostgresPersistence(p,{ leaseMs: 300000 });
  const target = new PostgresLargeBatchPersistence(p,attemptsLimits,300000);
  const writer: WholeBatchWriter = fault ? { preflight: plan => target.preflight(plan),persist: async () => { throw new PersistenceError('PERSISTENCE_FAILED'); } } : target;
  return { spool,control,target,coordinator: new LargeBatchCoordinator(control,spool,writer,attemptsLimits) };
}
export async function replayAttempts(m: SpoolManifest, spool: SpoolRepository, target: WholeBatchWriter, signal: AbortSignal) {
  await validateAttemptsSpool(m,spool);
  return target.persist(m,{ runId: m.identity.runId,streamId: m.identity.streamId,fencingToken: m.identity.fencingToken,signal },() => spool.records(m));
}
async function validateAttemptsSpool(m: SpoolManifest, spool: SpoolRepository): Promise<void> {
  if (!m.sealed || m.identity.plan.run.table !== 'AttemptsRawData') fail();
  let rows = 0;
  for await (const record of spool.records(m)) {
    if (++rows > attemptsLimits.maxRows) fail();
    prepareRecords([record],m.identity.plan.contract,{ ...defaultLimits,maxRecords: 1,maxBytes: attemptsLimits.maxRecordBytes });
  }
  if (rows !== m.rows) fail(); // Read/hash to EOF even when PostgreSQL already has the receipt.
}
/** Same run/checkpoint/fence, only while its original lease remains valid. Never adopts an expired run. */
export function resumedAttemptsCoordinator(origin: SpoolManifest, b: Awaited<ReturnType<typeof attemptsComponents>>, signal: AbortSignal) {
  if (origin.sealed || origin.state !== 'recovery_required' || origin.identity.plan.run.table !== 'AttemptsRawData') fail();
  const handle = { runId: origin.identity.runId,streamId: origin.identity.streamId,fencingToken: origin.identity.fencingToken,signal };
  return new LargeBatchCoordinator({
    startRun: async input => {
      if (input.table !== 'AttemptsRawData' || input.readType !== 'reread' || input.contractVersion !== origin.identity.plan.contract.version || input.exportDate !== origin.identity.plan.run.exportDate) fail();
      await b.control.renew(handle); return handle;
    },
    checkpoint: async h => { const cp = await b.control.checkpoint(h); if (canonical(cp) !== canonical(origin.identity.expected)) fail(); return cp; },
    renew: h => b.control.renew(h),recoverExpired: () => b.control.recoverExpired(),
  },b.spool,b.target,attemptsLimits);
}
export async function receiveAttemptsLog(root: string, origin: SpoolManifest, source: AttemptsFixtureSource, signal: AbortSignal) {
  if (origin.sealed || origin.state !== 'recovery_required') fail();
  const spool = await LocalSpool.open(join(root,'log-spool'),attemptsLimits), fields = logFixtureFields('0000000000');
  const plan: LargePlan = { ...origin.identity.plan,fields: [],contract: { ...origin.identity.plan.contract,approved: false,version: 'attempts-log-evidence/v1',
    fields: Object.fromEntries(fields.map(f => [f.name,{ type: 'bytes' as const,nullable: false }])),key_fields: [] },
    signature: signature('LogReadData'),parameters: { tablename: 'AttemptsRawData',DateExport: origin.identity.plan.run.exportDate },
    run: { ...origin.identity.plan.run,readType: 'investigation' } };
  const writer = await spool.create({ ...origin.identity,batchId: randomUUID(),plan }); let complete = false, count = 0;
  try {
    for await (const event of source.call(plan.signature,plan.parameters,signal)) {
      if (event.kind === 'row') {
        if (++count > 10000 || event.values.length !== fields.length || event.values.some(v => v === null)) fail();
        const record = Object.fromEntries(fields.map((f,i) => [f.name,Buffer.from(event.values[i]!).toString('hex')]));
        decodeLogRecord(record); await writer.append(record);
      } else { if (complete) fail(); complete = true; }
    }
    if (!complete) fail();
    return await writer.seal(); // Evidence only: does not advance an ingestion checkpoint.
  } catch (error) { await writer.interrupt(); throw error; }
}
async function logIntervals(root: string) {
  const spool = await LocalSpool.open(join(root,'log-spool'),attemptsLimits), m = await attemptsManifest(root,'log-spool'), intervals: LogInterval[] = [];
  if (!m.sealed) fail();
  for await (const record of spool.records(m)) { if (intervals.length >= 10000) fail(); intervals.push(decodeLogRecord(record)); }
  return { m,intervals };
}
async function binding(root: string, s: AttemptsState, c: SourceConfig, operation: AttemptsApproval['operation']) {
  const context = operation === 'read' ? null : { origin: await attemptsManifest(root),
    ...(operation === 'reread' ? { recovery: await privateJson(join(root,'recovery-plan.json')),log: (await logIntervals(root)).m } : {}) };
  if (context && (context.origin.sealed || context.origin.state !== 'recovery_required')) fail();
  return createHmac('sha256',s.password).update(canonical({ state: s,source: c,operation,context,tls: 'verified-only',contract: attemptsFixtureContract() })).digest('hex');
}
export async function attemptsCommand(args: string[]) {
  const action = args.shift(), flags: Record<string,string> = {};
  while (args.length) { const key = args.shift()!, value = args.shift(); if (!key.startsWith('--') || !value || Object.hasOwn(flags,key)) fail(); flags[key] = value; }
  const allowed: Record<string,string[]> = {
    requirements: ['--export-date'],prepare: ['--export-date'],approve: ['--operation','--operator','--reference','--expires-at','--confirm','--source-config'],
    read: ['--confirm','--source-config','--simulate-destination-failure','--export-date'],log: ['--confirm','--source-config'],reread: ['--confirm','--source-config'],
    'plan-reread': ['--start','--end','--operator','--reference','--ownership-evidence','--completeness-evidence'],
    recover: ['--spool'],replay: ['--spool'],status: [],
  };
  if (!action || !allowed[action] || Object.keys(flags).some(k => !['--mode','--work-dir',...allowed[action]!].includes(k)) || process.env.SYNC_ENABLED !== 'false' || process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') fail();
  if (action === 'requirements') return { corporateReadEnabled: false,blockers: attemptsBlockers,firstAcquisition: firstAttemptsAcquisitionRequirements(flags['--export-date'] ?? '2026-10-19') };
  if (action === 'read' && flags['--mode'] === 'corporate-local') {
    if (flags['--confirm'] !== 'EXECUTE_ATTEMPTS_READ_ONCE' || !flags['--work-dir'] || !flags['--source-config'] || !flags['--export-date'] || flags['--simulate-destination-failure']) fail();
    // Do NOT create a remote-attempt claim, open a pool, read secrets or touch historical directories while preconditions fail.
    throw new FirstAttemptsAcquisitionBlocked(flags['--export-date']);
  }
  if (action === 'read' && flags['--export-date']) fail(); // Never silently ignore a target date on the fixture/legacy routes.
  requireAttemptsFixture(flags['--mode'] ?? 'corporate'); // Unconditional BEFORE filesystem, approval, network or Docker.
  const root = flags['--work-dir']; if (!root) fail();
  if (action === 'prepare') { const s = await prepareAttempts(root,flags['--export-date'] ?? ''); return { prepared: true,testId: s.testId }; }
  const s = await state(root); await assertAttemptsDestination(s);
  if (action === 'approve' || ['read','log','reread'].includes(action)) {
    const operation = (action === 'approve' ? flags['--operation'] : action) as AttemptsApproval['operation'];
    if (!['read','log','reread'].includes(operation)) fail();
    const c = attemptsSourceSchema.parse(await privateJson(flags['--source-config'] ?? '')); await assertAttemptsSource(c);
    const configurationDigest = await binding(root,s,c,operation);
    if (action === 'approve') {
      if (flags['--confirm'] !== 'AUTHORIZE_ATTEMPTS_OPERATION_ONCE') fail();
      try { await fs.lstat(join(root,`attempt-${operation}.json`)); fail(); } catch (e) { if ((e as NodeJS.ErrnoException).code !== 'ENOENT') throw e; }
      const a = validateApproval({ testId: s.testId,operation,operator: flags['--operator'],reference: flags['--reference'],issuedAt: new Date().toISOString(),expiresAt: flags['--expires-at'],configurationDigest },s.testId,operation,configurationDigest);
      await durableExclusive(join(root,`approval-${operation}.json`),a); return { approved: true,testId: s.testId,operation };
    }
    if (flags['--confirm'] !== `EXECUTE_ATTEMPTS_${operation.toUpperCase()}_ONCE` || flags['--simulate-destination-failure'] && flags['--simulate-destination-failure'] !== 'true') fail();
    const a = validateApproval(await privateJson(join(root,`approval-${operation}.json`)),s.testId,operation,configurationDigest);
    const p = await attemptsPool(s), abort = new AbortController(), cancel = () => abort.abort(); process.once('SIGINT',cancel); process.once('SIGTERM',cancel);
    try {
      return await onceAttempts(root,a,async () => {
        await assertAttemptsSource(c); await assertAttemptsDestination(s);
        const latest = attemptsSourceSchema.parse(await privateJson(flags['--source-config']!));
        if (await binding(root,s,latest,operation) !== a.configurationDigest) fail();
        if (operation === 'read') {
          if ((await p.query('SELECT EXISTS(SELECT 1 FROM olos_export.runs) AS used')).rows[0].used) fail();
          const plan = attemptsFixturePlan(s.exportDate,c.collationHex), b = await attemptsComponents(root,p,'spool',flags['--simulate-destination-failure'] === 'true');
          const source = new AttemptsFixtureSource(c.endpoint,attemptsFixtureFields(c.collationHex),plan.signature,plan.parameters);
          const result = await b.coordinator.receive(plan,source,abort.signal);
          await durableExclusive(join(root,'read-result.json'),{ result,transport: source.transportSecurity,columns: source.observedMetadata });
          return { committed: true,...result };
        }
        const origin = await attemptsManifest(root); if (origin.sealed || origin.state !== 'recovery_required') fail();
        const b = await attemptsComponents(root,p,'recovery-spool');
        // Do not renew: remote recovery must not keep an unresolved lease alive indefinitely.
        await b.control.checkpoint({ runId: origin.identity.runId,streamId: origin.identity.streamId,fencingToken: origin.identity.fencingToken,signal: abort.signal });
        await claimAttemptsRecoveryOperation(p,origin,a);
        if (operation === 'log') {
          const sig = signature('LogReadData'), source = new AttemptsFixtureSource(c.endpoint,logFixtureFields(c.collationHex),sig,{ tablename: 'AttemptsRawData',DateExport: s.exportDate });
          const m = await receiveAttemptsLog(root,origin,source,abort.signal); return { evidenceSealed: true,intervals: m.rows,caseId: origin.identity.batchId,attribution: 'not-established-by-log' };
        }
        const recovery = await privateJson(join(root,'recovery-plan.json')) as AttemptsRecoveryPlan, evidence = await logIntervals(root);
        const checked = planAttemptsRecovery(origin,digest(evidence.m),evidence.intervals,recovery.interval,recovery);
        if (canonical(checked) !== canonical(recovery)) fail();
        const plan = rereadLargePlan(origin,recovery), source = new AttemptsFixtureSource(c.endpoint,attemptsFixtureFields(c.collationHex),plan.signature,plan.parameters,recovery.interval.quantity);
        const result = await resumedAttemptsCoordinator(origin,b,abort.signal).receive(plan,source,abort.signal);
        await durableExclusive(join(root,'recovery-delivered.json'),{ caseId: recovery.caseId,reviewDigest: digest(recovery),result,remoteOwnershipProvenByLog: false });
        return { recovered: true,...result };
      });
    } finally { process.removeListener('SIGINT',cancel); process.removeListener('SIGTERM',cancel); await p.end(); }
  }
  if (action === 'plan-reread') {
    const origin = await attemptsManifest(root), log = await logIntervals(root);
    const plan = planAttemptsRecovery(origin,digest(log.m),log.intervals,{ date: s.exportDate,start: flags['--start'] ?? '',end: flags['--end'] ?? '' },
      { operator: flags['--operator'] ?? '',approvalReference: flags['--reference'] ?? '',ownershipEvidence: flags['--ownership-evidence'] ?? '',completenessEvidence: flags['--completeness-evidence'] ?? '' });
    await durableExclusive(join(root,'recovery-plan.json'),plan); return { planned: true,caseId: plan.caseId,remoteReadExecuted: false };
  }
  const name = flags['--spool'] ?? 'spool'; if (!['spool','recovery-spool'].includes(name)) fail();
  const p = await attemptsPool(s);
  try {
    const b = await attemptsComponents(root,p,name), signal = new AbortController().signal;
    if (action === 'status') { const m = await b.spool.active(); return { testId: s.testId,state: m?.state ?? null,corporateReadEnabled: false }; }
    const m = await attemptsManifest(root,name); await validateAttemptsSpool(m,b.spool);
    if (action === 'recover') { const r = await b.coordinator.recover(signal); if (!r.result) fail(); return { recovered: true,...r.result }; }
    return { replayed: true,...await replayAttempts(m,b.spool,b.target,signal) };
  } finally { await p.end(); }
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try { process.stdout.write(`${JSON.stringify({ ok: true,...await attemptsCommand(process.argv.slice(2)) })}\n`); }
  catch (error) { const code = error instanceof Error && error.message === 'ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED' ? error.message : 'ATTEMPTS_CONTROLLED_TEST_STOPPED';
    process.stderr.write(`${JSON.stringify({ ok: false,code,retrySource: false,...(error instanceof FirstAttemptsAcquisitionBlocked ? { firstAcquisition: error.report } : {}) })}\n`); process.exitCode = 1; }
}
