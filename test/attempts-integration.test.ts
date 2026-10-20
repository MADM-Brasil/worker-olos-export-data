import { beforeAll, afterAll, expect, it } from 'vitest';
import { mkdtemp, writeFile, readFile, appendFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { fileURLToPath } from 'node:url';
import { spawn, execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { Connection, Request } from 'tedious';
import { isolatedSqlServer } from './support/sqlserver.js';
import { prepareAttempts, attemptsPool, attemptsManifest, attemptsComponents, onceAttempts, assertAttemptsDestination,
  attemptsLimits, resumedAttemptsCoordinator, claimAttemptsRecoveryOperation, type AttemptsState } from '../adapters/attempts-integration.js';
import { attemptsFixturePlan, attemptsFixtureFields, logFixtureFields, signature, validateApproval, requireAttemptsFixture, type AttemptsApproval } from '../src/integration/attempts/contracts.js';
import { AttemptsFixtureSource } from '../src/integration/attempts/source.js';
import { createAttemptsRecurringRunner } from '../src/integration/attempts/recurring.js';
import { AttemptsCycleRunner } from '../src/integration/attempts/cycle-runner.js';
import { decodeLogRecord, planAttemptsRecovery, rereadLargePlan, type LogInterval } from '../src/integration/attempts/recovery.js';
import { validateLargePlan } from '../src/large-batch/wire.js';
import { digest } from '../src/persistence/validation.js';
import { LocalSpool } from '../adapters/local-spool.js';
import type { SpoolManifest } from '../src/large-batch/types.js';

const exec = promisify(execFile), signal = () => new AbortController().signal, date = '2026-10-01';
let sql: Awaited<ReturnType<typeof isolatedSqlServer>>, directory: string, sourceFile: string, collation: string;
const instances: { root: string; state: AttemptsState }[] = [];
beforeAll(async () => {
  sql = await isolatedSqlServer(); directory = await mkdtemp(join(tmpdir(),'olos-attempts-test-'));
  const definitions = attemptsFixtureFields('0000000000').map(f => `[${f.name}] ${f.type === 'varchar' ? `varchar(${f.sqlLength}) COLLATE Latin1_General_100_CI_AS` : f.type} ${f.nullable ? 'NULL' : 'NOT NULL'}`).join(',');
  const fields = attemptsFixturePlan(date,'0000000000').fields, columns = fields.map(f => `[${f.name}]`).join(',');
  await sql.admin.request().batch(`CREATE TABLE dbo.AttemptsFixture(${definitions});
    CREATE TABLE dbo.AttemptsMode(failRead bit NOT NULL,shortReRead bit NOT NULL,outputError bit NOT NULL); INSERT dbo.AttemptsMode VALUES(0,0,0);
    CREATE TABLE dbo.AttemptsCounters(reads int NOT NULL,logs int NOT NULL,rereads int NOT NULL); INSERT dbo.AttemptsCounters VALUES(0,0,0);
    CREATE TABLE dbo.LogFixture(TableName varchar(128) COLLATE Latin1_General_100_CI_AS NOT NULL,PKColumnName varchar(128) COLLATE Latin1_General_100_CI_AS NOT NULL,
      DateExport date NOT NULL,PKIDStart bigint NOT NULL,PKIDEnd bigint NOT NULL,DateRead datetime NOT NULL,Quantity int NOT NULL);
    INSERT dbo.LogFixture VALUES('AttemptsRawData','AttemptsRawDataID','20261001',1,3,DATEADD(millisecond,3,CONVERT(datetime,'20261001',112)),3);`);
  for (let id=1;id<=3;id++) {
    const values = fields.map(f => f.name === 'AttemptsRawDataID' ? String(id) : f.name === 'CallId' ? '101' : f.type === 'int' ? 'NULL' : f.name === 'StartDate' ? "DATEADD(millisecond,3,CONVERT(datetime,'20261001',112))" : f.type === 'datetime' ? 'NULL' : f.name === 'CampaignData' ? "CAST('synthetic' AS varchar(max))" : "'X'").join(',');
    await sql.admin.request().batch(`INSERT dbo.AttemptsFixture VALUES(${values})`);
  }
  await sql.admin.request().batch(`CREATE PROCEDURE dbo.usp_ExportData_ReadDataCurrentDay @TableName varchar(128),@Message varchar(4000) OUTPUT,@ErrorMessage varchar(4000) OUTPUT AS
    BEGIN SET NOCOUNT ON; UPDATE dbo.AttemptsCounters SET reads=reads+1; IF (SELECT failRead FROM dbo.AttemptsMode)=1 BEGIN SELECT TOP(1) ${columns} FROM dbo.AttemptsFixture; RAISERROR('synthetic read interrupted',16,1); RETURN 1; END
    SELECT ${columns} FROM dbo.AttemptsFixture; SET @Message=NULL; SET @ErrorMessage=CASE WHEN (SELECT outputError FROM dbo.AttemptsMode)=1 THEN 'synthetic output error' ELSE NULL END; RETURN 0; END`);
  await sql.admin.request().batch(`CREATE PROCEDURE dbo.usp_ExportData_LogReadData @tablename nvarchar(128),@DateExport date,@Message varchar(4000) OUTPUT,@ErrorMessage varchar(4000) OUTPUT AS
    BEGIN SET NOCOUNT ON; UPDATE dbo.AttemptsCounters SET logs=logs+1; SELECT * FROM dbo.LogFixture WHERE DateExport=@DateExport; SET @Message=NULL; SET @ErrorMessage=NULL; RETURN 0; END`);
  await sql.admin.request().batch(`CREATE PROCEDURE dbo.usp_ExportData_ReReadData @TableName varchar(128),@DateExport date,@PKIDStart bigint,@PKIDEnd bigint,@Message varchar(4000) OUTPUT,@ErrorMessage varchar(4000) OUTPUT AS
    BEGIN SET NOCOUNT ON; UPDATE dbo.AttemptsCounters SET rereads=rereads+1; IF (SELECT shortReRead FROM dbo.AttemptsMode)=1 BEGIN
    SELECT TOP(2) ${columns} FROM dbo.AttemptsFixture WHERE AttemptsRawDataID BETWEEN @PKIDStart AND @PKIDEnd; END ELSE BEGIN
    SELECT ${columns} FROM dbo.AttemptsFixture WHERE AttemptsRawDataID BETWEEN @PKIDStart AND @PKIDEnd; END SET @Message=NULL; SET @ErrorMessage=NULL; RETURN 0; END`);
  const c = new Connection({ server: '127.0.0.1',authentication: { type: 'default',options: { userName: 'sa',password: sql.endpoint.password } },options: {
    port: sql.endpoint.port,database: 'phase3d_fixture',serverName: 'localhost',encrypt: true,trustServerCertificate: false,cryptoCredentialsDetails: { ca: sql.endpoint.ca,minVersion: 'TLSv1.2' } } });
  await new Promise<void>((resolve,reject) => c.connect(e => e ? reject(e) : resolve()));
  const request = new Request('SELECT TOP(0) TableName FROM dbo.LogFixture',() => {});
  request.on('columnMetadata',columns => { if (!Array.isArray(columns)) throw new Error('fixture'); collation = columns[0]!.collation!.toBuffer().toString('hex'); });
  await new Promise<void>((resolve,reject) => { request.on('requestCompleted',resolve); request.on('error',reject); c.execSql(request); }); c.close();
  const ids = (await exec('docker',['ps','--filter','label=worker-olos-export-data.test=phase3d','--no-trunc','--format','{{.ID}}'])).stdout.trim().split('\n');
  const own = JSON.parse((await exec('docker',['inspect',...ids])).stdout).find((d: { NetworkSettings: { Ports: Record<string,{ HostPort: string }[]> } }) => Number(d.NetworkSettings.Ports['1433/tcp']?.[0]?.HostPort) === sql.endpoint.port);
  if (!own) throw new Error('FIXTURE_IDENTITY_NOT_FOUND');
  sourceFile = join(directory,'sql-fixture.json'); await writeFile(sourceFile,JSON.stringify({ sqlContainer: own.Id,endpoint: sql.endpoint,collationHex: collation }),{ mode: 0o600 });
},120000);
afterAll(async () => { for (const i of instances) await exec('docker',['rm','-f','-v',i.state.container]); await sql?.stop(); if (directory) await rm(directory,{ recursive: true,force: true }); },30000);
async function fresh() { const root = join(directory,randomUUID()), state = await prepareAttempts(root,date); instances.push({ root,state }); return { root,state }; }
function approval(testId: string = randomUUID(), operation: AttemptsApproval['operation'] = 'read'): AttemptsApproval {
  return { testId,operation,operator: 'fixture-operator',reference: 'FIXTURE-ONLY',issuedAt: new Date().toISOString(),expiresAt: new Date(Date.now()+10*60000).toISOString(),configurationDigest: 'a'.repeat(64) };
}
async function cli(args: string[]) {
  const child = spawn(process.execPath,['--import','tsx',fileURLToPath(new URL('../adapters/attempts-integration.ts',import.meta.url)),...args],{
    env: { ...process.env,SYNC_ENABLED: 'false' },stdio: ['ignore','pipe','pipe'] });
  let output = ''; child.stdout.on('data',d => { output += d; }); child.stderr.on('data',d => { output += d; });
  const code = await new Promise<number | null>((resolve,reject) => { child.on('exit',resolve); child.on('error',reject); });
  expect(output).not.toContain(sql.endpoint.password); return { code,output };
}
const flags = (root: string) => ['--mode','isolated-fixture','--work-dir',root];
async function approve(root: string, operation: 'read' | 'log' | 'reread') {
  const r = await cli(['approve',...flags(root),'--source-config',sourceFile,'--operation',operation,'--operator','fixture-operator','--reference','FIXTURE-ONLY',
    '--expires-at',new Date(Date.now()+10*60000).toISOString(),'--confirm','AUTHORIZE_ATTEMPTS_OPERATION_ONCE']); expect(r.code,r.output).toBe(0);
}
const invoke = (root: string, op: 'read' | 'log' | 'reread', extra: string[] = []) => cli([op,...flags(root),'--source-config',sourceFile,'--confirm',`EXECUTE_ATTEMPTS_${op.toUpperCase()}_ONCE`,...extra]);
async function counters() { return (await sql.admin.request().query('SELECT * FROM dbo.AttemptsCounters')).recordset[0] as { reads: number; logs: number; rereads: number }; }

it('CLI padrão e corporate bloqueiam antes de ler arquivos ou abrir sockets; sem aprovação por env/TLS Disposition',async () => {
  for (const args of [['read','--work-dir','/does-not-exist'],['read','--mode','corporate','--confirm','EXECUTE_ATTEMPTS_READ_ONCE'],
    ['read','--tls-mode','exceptional-unverified-test']]) {
    const result = await cli(args); expect(result.code).toBe(1); expect(result.output).toContain('retrySource');
  }
  expect(() => requireAttemptsFixture('corporate')).toThrow('ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED');
  expect((await cli(['requirements'])).output).toContain('CORPORATE_ATTEMPTS_CAPABILITY_NOT_ISSUABLE');
},30000); // Four separate Node/tsx processes; harness budget, not a network/RPC timeout.
it('aprovação é específica de operação/configuração/testId e de até 15 minutos',() => {
  const a = approval(); expect(validateApproval(a,a.testId,'read',a.configurationDigest)).toEqual(a);
  for (const b of [undefined,{ ...a,expiresAt: '2020-01-01T00:00:00.000Z' },{ ...a,expiresAt: new Date(Date.now()+16*60000).toISOString() },{ ...a,operator: '' },{ ...a,operation: 'reread' }])
    expect(() => validateApproval(b,a.testId,'read',a.configurationDigest)).toThrow();
  expect(() => validateApproval(a,randomUUID(),'read',a.configurationDigest)).toThrow();
  expect(() => validateApproval(a,a.testId,'read','b'.repeat(64))).toThrow();
});
it('claim durável/concorrente sobrevive ao crash e impede novo RPC',async () => {
  const root = await mkdtemp(join(directory,'claim-')), a = approval(); let calls = 0;
  const r = await Promise.allSettled([onceAttempts(root,a,async () => { calls++; throw new Error('synthetic'); }),onceAttempts(root,a,async () => { calls++; })]);
  expect(r.every(x => x.status === 'rejected')).toBe(true); expect(calls).toBe(1);
  await expect(onceAttempts(root,a,async () => { calls++; })).rejects.toBeDefined(); expect(calls).toBe(1);
});
it('18 campos documentados, PK distinta de CallId, DATETIME exato, null e TLS validado no pipeline/CLI',async () => {
  const { root,state } = await fresh(), before = await counters();
  expect((await invoke(root,'read')).code).toBe(1); expect(await counters()).toEqual(before);
  await approve(root,'read');
  const changed = join(root,'changed-source.json'), config = JSON.parse(await readFile(sourceFile,'utf8'));
  await writeFile(changed,JSON.stringify({ ...config,collationHex: '0000000000' }),{ mode: 0o600 });
  expect((await cli(['read',...flags(root),'--source-config',changed,'--confirm','EXECUTE_ATTEMPTS_READ_ONCE'])).code).toBe(1);
  expect(await counters()).toEqual(before);
  const r = await invoke(root,'read'); expect(r.code,r.output).toBe(0);
  const p = await attemptsPool(state);
  try {
    const rows = (await p.query('SELECT payload FROM olos_export.attempts_raw_data ORDER BY source_key')).rows;
    expect(rows).toHaveLength(3); expect(Object.keys(rows[0].payload)).toHaveLength(18);
    expect(rows.map(r => r.payload.AttemptsRawDataID)).toEqual(['1','2','3']); expect(rows[0].payload.CallId).toBe('101');
    expect(rows[0].payload.AgentId).toBeNull(); expect(rows[0].payload.StartDate).toMatch(/^sql-datetime\/v1:[a-f0-9]{8}01000000$/);
    const cp = (await p.query('SELECT version FROM olos_export.checkpoints')).rows;
    const replay = await cli(['replay',...flags(root)]); expect(replay.code,replay.output).toBe(0);
    expect((await p.query('SELECT version FROM olos_export.checkpoints')).rows).toEqual(cp);
    expect((await invoke(root,'read')).code).toBe(1); expect((await counters()).reads).toBe(before.reads+1);
    const result = JSON.parse(await readFile(join(root,'read-result.json'),'utf8'));
    expect(result.transport).toEqual([{ encrypted: true,identityValidated: true,protocol: 'TLSv1.2',tlsMode: 'verified' }]);
    const m = await attemptsManifest(root); await appendFile(join(root,'spool','batches',m.identity.batchId,'records.ready'),'\n');
    expect((await cli(['replay',...flags(root)])).code).toBe(1); // Receipt cannot hide a corrupted spool.
    expect((await p.query('SELECT version FROM olos_export.checkpoints')).rows).toEqual(cp);
  } finally { await p.end(); }
},45000);
it('falha PostgreSQL após selo preserva spool; recuperação e replay não voltam ao SQL Server',async () => {
  const { root,state } = await fresh(), before = await counters(); await approve(root,'read');
  expect((await invoke(root,'read',['--simulate-destination-failure','true'])).code).toBe(1);
  expect((await attemptsManifest(root)).sealed).toBe(true);
  const p = await attemptsPool(state);
  try {
    expect((await p.query('SELECT count(*) AS n FROM olos_export.attempts_raw_data')).rows[0].n).toBe('0');
    const recover = await cli(['recover',...flags(root)]); expect(recover.code,recover.output).toBe(0);
    expect((await cli(['replay',...flags(root)])).code).toBe(0);
    expect((await counters()).reads).toBe(before.reads+1); expect((await counters()).rereads).toBe(before.rereads);
  } finally { await p.end(); }
},45000);
it('falha parcial → LogReadData selado → plano manual → ReReadData completo → mesma transação/checkpoint e replay',async () => {
  const { root,state } = await fresh(), before = await counters();
  await sql.admin.request().batch('UPDATE dbo.AttemptsMode SET failRead=1');
  try {
    await approve(root,'read'); expect((await invoke(root,'read')).code).toBe(1);
    const origin = await attemptsManifest(root); expect(origin.sealed).toBe(false);
    const p = await attemptsPool(state);
    try {
      expect((await p.query('SELECT version FROM olos_export.checkpoints')).rows[0].version).toBe('0');
      await approve(root,'log'); const logged = await invoke(root,'log'); expect(logged.code,logged.output).toBe(0);
      expect((await p.query('SELECT version FROM olos_export.checkpoints')).rows[0].version).toBe('0');
      const planned = await cli(['plan-reread',...flags(root),'--start','1','--end','3','--operator','fixture-operator','--reference','FIXTURE-ONLY',
        '--ownership-evidence','exclusive-generated-fixture','--completeness-evidence','fixture-range-inclusive-and-count-3']);
      expect(planned.code,planned.output).toBe(0); expect((await counters()).rereads).toBe(before.rereads);
      await approve(root,'reread'); const rr = await invoke(root,'reread'); expect(rr.code,rr.output).toBe(0);
      const recovered = await attemptsManifest(root,'recovery-spool'); expect(recovered.identity.runId).toBe(origin.identity.runId);
      expect(recovered.identity.plan.signature.procedure).toBe('ReReadData'); expect(recovered.rows).toBe(3);
      expect((await p.query('SELECT version FROM olos_export.checkpoints')).rows[0].version).toBe('1');
      expect((await p.query('SELECT count(*) AS n FROM olos_export.attempts_raw_data')).rows[0].n).toBe('3');
      expect((await cli(['replay',...flags(root),'--spool','recovery-spool'])).code).toBe(0);
      expect((await invoke(root,'reread')).code).toBe(1); expect((await invoke(root,'read')).code).toBe(1);
      expect(await counters()).toEqual({ reads: before.reads+1,logs: before.logs+1,rereads: before.rereads+1 });
      expect((await attemptsManifest(root)).sealed).toBe(false); // Partial original retained, never promoted.
    } finally { await p.end(); }
  } finally { await sql.admin.request().batch('UPDATE dbo.AttemptsMode SET failRead=0'); }
},60000);
it('ReReadData truncado mesmo com retorno 0 não sela, não confirma checkpoint e não repete origem',async () => {
  const { root,state } = await fresh(), plan = attemptsFixturePlan(date,collation), p = await attemptsPool(state), b = await attemptsComponents(root,p);
  await sql.admin.request().batch('UPDATE dbo.AttemptsMode SET shortReRead=1');
  try {
    const rangePlan = { ...plan,signature: signature('ReReadData'),run: { ...plan.run,readType: 'reread' as const,pkRange: { start: '1',end: '3' } },parameters: { TableName: 'AttemptsRawData',DateExport: date,PKIDStart: '1',PKIDEnd: '3' } };
    const source = new AttemptsFixtureSource(sql.endpoint,attemptsFixtureFields(collation),rangePlan.signature,rangePlan.parameters,3);
    await expect(b.coordinator.receive(rangePlan,source,signal())).rejects.toBeDefined();
    expect((await attemptsManifest(root)).sealed).toBe(false); expect((await p.query('SELECT version FROM olos_export.checkpoints')).rows[0].version).toBe('0');
    await expect(source.read(rangePlan,signal()).next()).rejects.toMatchObject({ code: 'REAL_EXECUTION_BLOCKED' });
  } finally { await sql.admin.request().batch('UPDATE dbo.AttemptsMode SET shortReRead=0'); await p.end(); }
},30000);
it('lease vencida, checkpoint alterado e destino incorreto bloqueiam recuperação sem takeover',async () => {
  const { root,state } = await fresh(), p = await attemptsPool(state), b = await attemptsComponents(root,p), plan = attemptsFixturePlan(date,collation);
  try {
    await sql.admin.request().batch('UPDATE dbo.AttemptsMode SET failRead=1');
    const source = new AttemptsFixtureSource(sql.endpoint,attemptsFixtureFields(collation),plan.signature,plan.parameters);
    await expect(b.coordinator.receive(plan,source,signal())).rejects.toBeDefined();
    const before = await counters(), origin = await attemptsManifest(root), recovered = await attemptsComponents(root,p,'recovery-spool');
    const claims = await Promise.allSettled([claimAttemptsRecoveryOperation(p,origin,approval(state.testId,'log')),claimAttemptsRecoveryOperation(p,origin,approval(state.testId,'log'))]);
    expect(claims.filter(c => c.status === 'fulfilled')).toHaveLength(1);
    expect((await p.query('SELECT count(*) AS n FROM olos_export.attempts_recovery_operations')).rows[0].n).toBe('1');
    await p.query('UPDATE olos_export.checkpoints SET version=version+1');
    await expect(claimAttemptsRecoveryOperation(p,origin,approval(state.testId,'reread'))).rejects.toBeDefined();
    const rrPlan = { ...plan,signature: signature('ReReadData'),run: { ...plan.run,readType: 'reread' as const,pkRange: { start: '1',end: '3' } },parameters: { TableName: 'AttemptsRawData',DateExport: date,PKIDStart: '1',PKIDEnd: '3' } };
    const rrSource = new AttemptsFixtureSource(sql.endpoint,attemptsFixtureFields(collation),rrPlan.signature,rrPlan.parameters,3);
    await expect(resumedAttemptsCoordinator(origin,recovered,signal()).receive(rrPlan,rrSource,signal())).rejects.toBeDefined();
    expect(await counters()).toEqual(before);
    await p.query("UPDATE olos_export.source_locks SET lease_until=clock_timestamp()-interval '1 second' WHERE source_table='AttemptsRawData'");
    await expect(b.control.checkpoint({ ...((await attemptsManifest(root)).identity),signal: signal() })).rejects.toMatchObject({ code: 'LEASE_LOST' });
    const cases = await b.control.recoverExpired(); expect(cases).toHaveLength(1);
    await expect(b.control.startRun(plan.run,signal())).rejects.toMatchObject({ code: 'RECOVERY_REQUIRED' }); expect(await counters()).toEqual(before);
    await expect(assertAttemptsDestination({ ...state,testId: randomUUID() })).rejects.toBeDefined();
  } finally { await sql.admin.request().batch('UPDATE dbo.AttemptsMode SET failRead=0'); await p.end(); }
},30000);
it('outro endpoint, exceção TLS, outra tabela ou ResetReadData não entram no cliente Attempts',async () => {
  const fields = attemptsFixtureFields(collation), sig = signature('ReadDataCurrentDay');
  expect(() => new AttemptsFixtureSource({ ...sql.endpoint,server: '10.196.234.11' },fields,sig,{ TableName: 'AttemptsRawData' })).toThrow();
  expect(() => new AttemptsFixtureSource({ ...sql.endpoint,trustServerCertificate: true },fields,sig,{ TableName: 'AttemptsRawData' })).toThrow();
  expect(() => new AttemptsFixtureSource(sql.endpoint,fields,sig,{ TableName: 'Campaign' })).toThrow();
  expect(() => new AttemptsFixtureSource(sql.endpoint,fields,signature('ResetReadData'),{ TableName: 'AttemptsRawData',DateExport: date })).toThrow();
});
it('erro em OUTPUT após receber linhas não permite selo/sucesso ou nova chamada',async () => {
  const plan = attemptsFixturePlan(date,collation), source = new AttemptsFixtureSource(sql.endpoint,attemptsFixtureFields(collation),plan.signature,plan.parameters), events: string[] = [];
  await sql.admin.request().batch('UPDATE dbo.AttemptsMode SET outputError=1');
  try {
    await expect((async () => { for await (const e of source.read(plan,signal())) events.push(e.kind); })()).rejects.toMatchObject({ code: 'OUTPUT_ERROR' });
    expect(events).not.toContain('complete'); expect(source.metrics.calls).toBe(1);
    await expect(source.read(plan,signal()).next()).rejects.toMatchObject({ code: 'REAL_EXECUTION_BLOCKED' });
  } finally { await sql.admin.request().batch('UPDATE dbo.AttemptsMode SET outputError=0'); }
});

function logRecord(start = '1',end = '3',quantity = 3) {
  const a = Buffer.alloc(8), b = Buffer.alloc(8), q = Buffer.alloc(4), d = Buffer.alloc(3), t = Buffer.alloc(8);
  a.writeBigInt64LE(BigInt(start)); b.writeBigInt64LE(BigInt(end)); q.writeInt32LE(quantity);
  d.writeUIntLE(Math.round((Date.parse(`${date}T00:00:00Z`)+62135596800000)/86400000),0,3); t.writeUInt32LE(1,4);
  return { TableName: Buffer.from('AttemptsRawData').toString('hex'),PKColumnName: Buffer.from('AttemptsRawDataID').toString('hex'),DateExport: d.toString('hex'),
    PKIDStart: a.toString('hex'),PKIDEnd: b.toString('hex'),Quantity: q.toString('hex'),DateRead: t.toString('hex') };
}
it('Log BIGINT é exato acima de MAX_SAFE_INTEGER e DATETIME preserva ticks; null/bytes indevidos são recusados',() => {
  const row = decodeLogRecord(logRecord('9007199254740993','9007199254740995',3));
  expect(row.start).toBe('9007199254740993'); expect(row.end).toBe('9007199254740995'); expect(row.date).toBe(date); expect(row.dateRead).toBe('sql-datetime/v1:0000000001000000');
  expect(() => decodeLogRecord({ ...logRecord(),Quantity: null })).toThrow();
  expect(() => decodeLogRecord({ ...logRecord(),DateExport: 'ff' })).toThrow();
});
it('Log não atribui leitura ao worker; intervalos sobrepostos, duplicados ou fora da data nunca geram plano',() => {
  const plan = attemptsFixturePlan(date,collation), origin: SpoolManifest = { identity: { format: 'olos-spool/v1',batchId: randomUUID(),runId: randomUUID(),streamId: randomUUID(),fencingToken: '1',plan,expected: { version: '0',cursor: null } },sealed: false,state: 'recovery_required',rows: 0,bytes: 0,sha256: null };
  const row = decodeLogRecord(logRecord()), review = { operator: 'fixture-operator',approvalReference: 'FIXTURE',ownershipEvidence: 'exclusive fixture',completenessEvidence: 'inclusive fixture with 3 rows' };
  const selected = { date,start: '1',end: '3' };
  const recovery = planAttemptsRecovery(origin,'a'.repeat(64),[row],selected,review);
  validateLargePlan(rereadLargePlan(origin,recovery));
  for (const rows of [[],[row,row],[row,{ ...row,start: '2' }]]) expect(() => planAttemptsRecovery(origin,'a'.repeat(64),rows,selected,review)).toThrow();
  expect(() => planAttemptsRecovery(origin,'a'.repeat(64),[row],{ ...selected,date: '2026-10-02' },review)).toThrow();
  expect(() => planAttemptsRecovery(origin,'a'.repeat(64),[row],selected,{ ...review,ownershipEvidence: '' })).toThrow();
  expect(() => rereadLargePlan({ ...origin,sealed: true },recovery)).toThrow();
  expect(() => planAttemptsRecovery({ ...origin,state: 'receiving' },'a'.repeat(64),[row],selected,review)).toThrow();
  expect(() => planAttemptsRecovery(origin,'a'.repeat(64),[{ ...row,quantity: 0 }],selected,review)).toThrow();
  const bad = rereadLargePlan(origin,recovery); expect(() => validateLargePlan({ ...bad,parameters: { ...bad.parameters,DateExport: '2026-10-02' } })).toThrow();
});


it('executor recorrente realiza dois RPCs isolados e confirma dois runs sem duplicar', async () => {
  const { root, state } = await fresh();
  const p = await attemptsPool(state);
  const b = await attemptsComponents(root, p);
  const plan = attemptsFixturePlan(date, collation);
  const initial = await counters();

  const results: Array<{
    received: number;
    persisted: number;
    duplicates: number;
    rejected: number;
  }> = [];

  let done!: () => void;
  const completed = new Promise<void>(resolve => { done = resolve; });

  const runner = createAttemptsRecurringRunner({
    coordinator: b.coordinator,
    plan: () => attemptsFixturePlan(date, collation),
    source: () => new AttemptsFixtureSource(
      sql.endpoint,
      attemptsFixtureFields(collation),
      plan.signature,
      plan.parameters
    ),
    onSuccess: result => {
      results.push(result);
      if (results.length === 2) done();
    },
  }, 100);

  try {
    runner.start();

    await Promise.race([
      completed,
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error('TWO_CYCLES_TIMEOUT')), 15000)
      ),
    ]);

    await runner.stop();

    const after = await counters();

    expect(after.reads - initial.reads).toBe(2);
    expect(results).toHaveLength(2);
    expect(results[0]?.received).toBe(3);
    expect(results[0]?.persisted).toBe(3);
    expect(results[1]?.received).toBe(3);
    expect(results[1]?.persisted).toBe(0);
    expect(results[1]?.duplicates).toBe(3);

    const runs = await p.query(`
      SELECT state, received_count, persisted_count
      FROM olos_export.runs
      WHERE source_table = 'AttemptsRawData'
    `);

    expect(runs.rows).toHaveLength(2);
    expect(runs.rows.every(r => r.state === 'succeeded')).toBe(true);
    expect(await b.spool.active()).toBeNull();

  } finally {
    await runner.stop();
    await p.end();
  }
}, 30000);


it('recorrencia bloqueia novo RPC apos falha e reinicializacao', async () => {
  const { root, state } = await fresh();
  const p = await attemptsPool(state);
  const plan = attemptsFixturePlan(date, collation);
  const initial = await counters();

  const faulty = await attemptsComponents(root, p, 'spool', true);
  let failed!: () => void;
  const failure = new Promise<void>(resolve => { failed = resolve; });

  const first = new AttemptsCycleRunner(
    async signal => {
      try {
        await faulty.coordinator.receive(
          plan,
          new AttemptsFixtureSource(
            sql.endpoint,
            attemptsFixtureFields(collation),
            plan.signature,
            plan.parameters
          ),
          signal
        );
      } catch (error) {
        failed();
        throw error;
      }
    },
    100
  );

  try {
    first.start();

    await Promise.race([
      failure,
      new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error('FAILURE_TIMEOUT')), 15000)
      )
    ]);

    // Aguarda o tratamento assíncrono da falha.
    for (let i = 0; i < 50 && first.status !== 'failed'; i++) {
      await new Promise(resolve => setTimeout(resolve, 10));
    }

    expect(first.status).toBe('failed');

    const afterFailure = await counters();
    expect(afterFailure.reads - initial.reads).toBe(1);

    const original = await faulty.spool.active();
    expect(original).not.toBeNull();
    expect(original?.sealed).toBe(true);

    await first.stop();

    // Simula novo processo: novas instâncias, mesmo PostgreSQL e spool.
    const restarted = await attemptsComponents(root, p);
    const second = createAttemptsRecurringRunner({
      coordinator: restarted.coordinator,
      plan: () => attemptsFixturePlan(date, collation),
      source: () => new AttemptsFixtureSource(
        sql.endpoint,
        attemptsFixtureFields(collation),
        plan.signature,
        plan.parameters
      ),
    }, 100);

    try {
      second.start();

      for (let i = 0; i < 100 && second.status !== 'failed'; i++) {
        await new Promise(resolve => setTimeout(resolve, 20));
      }

      expect(second.status).toBe('failed');

      const afterRestart = await counters();
      expect(afterRestart.reads - initial.reads).toBe(1);

      const preserved = await restarted.spool.active();
      expect(preserved).not.toBeNull();
      expect(preserved?.identity.batchId).toBe(original?.identity.batchId);
      expect(preserved?.sha256).toBe(original?.sha256);

    } finally {
      await second.stop();
    }
  } finally {
    await first.stop();
    await p.end();
  }
}, 30000);
