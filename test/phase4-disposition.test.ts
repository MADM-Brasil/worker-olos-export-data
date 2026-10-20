import { beforeAll, afterAll, expect, it } from 'vitest';
import { mkdtemp, rm, readFile, writeFile, chmod } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { randomUUID } from 'node:crypto';
import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { isolatedSqlServer } from './support/sqlserver.js';
import { prepare, components, once, retained, reconcile, privateJson, assertFreshTest } from '../adapters/disposition-integration.js';
import { dispositionPlan, ControlledDispositionSource, replayDisposition } from '../src/integration/disposition.js';
import { approveDispositionEndpoint, assertDispositionCall, type DispositionApproval } from '../src/olos/tds/disposition-access.js';
import { fixtureSignature } from './support/olos-documented-fixture.js';
import { canonical } from '../src/persistence/validation.js';
import pg from 'pg';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
const exec = promisify(execFile);
let db: Awaited<ReturnType<typeof isolatedSqlServer>>, root: string, s: Awaited<ReturnType<typeof prepare>>, p: pg.Pool, coll: string;
const signal = () => new AbortController().signal;
const approval = (): DispositionApproval => ({ action: "ReadData('Disposition')",testId: s.testId,reference: 'TEST-ONLY',snapshotDate: s.snapshotDate,expiresAt: new Date(Date.now()+3600000).toISOString() });
const config = { server: '10.196.234.11',port: 1433,database: 'ExportDataV3',user: 'OEC',password: 'SYNTHETIC-NO-CONNECTION',ca: '-----BEGIN CERTIFICATE-----test',serverName: 'test.invalid' };
beforeAll(async () => {
  db = await isolatedSqlServer();
  const parent = await mkdtemp(join(tmpdir(),'phase4-')); root = join(parent,'work');
  s = await prepare(root,'2026-10-01');
  p = new pg.Pool({ host: '127.0.0.1',port: s.port,user: 'postgres',password: s.password,database: 'phase4_worker_fixture',options: '-c role=olos_export_worker' });
  await db.admin.request().batch(`CREATE TABLE dbo.DispositionFixture(DispositionId int NOT NULL,Description nvarchar(200) NOT NULL);
    INSERT dbo.DispositionFixture VALUES(1,N'Synthetic 😀'),(2,N''),(3,N'  Á  ');
    CREATE TABLE dbo.InvocationCount(n int NOT NULL); INSERT dbo.InvocationCount VALUES(0);`);
  await db.admin.request().batch(`CREATE PROCEDURE dbo.usp_ExportData_ReadData @TableName varchar(128),@Message varchar(max) OUTPUT,@ErrorMessage varchar(max) OUTPUT AS BEGIN SET NOCOUNT ON; UPDATE dbo.InvocationCount SET n=n+1; SELECT DispositionId,Description FROM dbo.DispositionFixture; SET @Message=NULL; SET @ErrorMessage=NULL; RETURN 0; END`);
  // Obter os bytes exatos por metadado da conexão da fixture, sem registros reais.
  const { Connection,Request } = await import('tedious');
  const c = new Connection({ server: '127.0.0.1',authentication: { type: 'default',options: { userName: 'sa',password: db.endpoint.password } },options: { database: 'phase3d_fixture',port: db.endpoint.port,serverName: 'localhost',encrypt: true,trustServerCertificate: false,cryptoCredentialsDetails: { ca: db.endpoint.ca } } });
  await new Promise<void>((resolve,reject) => c.connect(e => e ? reject(e) : resolve()));
  const r = new Request('SELECT * FROM dbo.DispositionFixture WHERE 1=0',() => {});
  r.on('columnMetadata',m => { if (!Array.isArray(m)) throw new Error('Metadata'); coll = m[1]!.collation!.toBuffer().toString('hex'); });
  await new Promise<void>((resolve,reject) => { r.on('requestCompleted',resolve); r.on('error',reject); c.execSql(r); }); c.close();
  expect(coll).toMatch(/^[a-f0-9]{10}$/);
},120000);
afterAll(async () => { await p?.end(); if (s) await exec('docker',['rm','-f','-v',s.container]); await db?.stop(); if (root) await rm(join(root,'..'),{ recursive: true,force: true }); });
it('aprovação expirada/ausente, endpoint, ação e procedimento errado falham sem rede',() => {
  const a = approval();
  for (const bad of [undefined,{ ...a,action: 'ReadDataCurrentDay' },{ ...a,expiresAt: '2020-01-01T00:00:00.000Z' },{ ...a,testId: randomUUID() }])
    expect(() => approveDispositionEndpoint(config,bad,s.testId)).toThrow();
  expect(() => approveDispositionEndpoint({ ...config,server: 'wrong.invalid' },a,s.testId)).toThrow();
  const endpoint = approveDispositionEndpoint(config,a,s.testId);
  expect(() => assertDispositionCall(endpoint,'ReadDataCurrentDay',{ TableName: 'Disposition' })).toThrow();
  assertDispositionCall(endpoint,'ReadData',{ TableName: 'Disposition' });
  expect(() => assertDispositionCall(endpoint,'ReadData',{ TableName: 'Disposition' })).toThrow();
});
it('uma leitura real de fixture → spool → PostgreSQL; quantidade variável, Unicode, vazio, replay e checkpoint único',async () => {
  const a = approval(),plan = dispositionPlan(a),b = await components(root,p),source = new ControlledDispositionSource(db.endpoint,plan);
  await assertFreshTest(p);
  await once(root,a,() => b.coordinator.receive(plan,source,signal()));
  const m = await retained(root),before = await reconcile(p,m,b.spool);
  expect(source.observedMetadata[1]).toMatchObject({ name: 'Description',nullable: false,dataLength: 400,collationHex: coll });
  await expect(assertFreshTest(p)).rejects.toBeDefined();
  expect(before.records).toBe(3); expect(before.checkpoint).toBe('1');
  expect((await p.query('SELECT payload FROM olos_export.disposition ORDER BY source_key')).rows.map(r => r.payload.Description)).toEqual(['Synthetic 😀','','  Á  ']);
  await replayDisposition(m,b.spool,b.target,signal());
  expect(await reconcile(p,m,b.spool)).toEqual(before);
  expect((await db.admin.request().query('SELECT n FROM dbo.InvocationCount')).recordset[0].n).toBe(1);
  await expect(once(root,a,async () => { throw new Error('must not call'); })).rejects.toBeDefined();
  await expect(source.read(plan,signal()).next()).rejects.toMatchObject({ code: 'REAL_EXECUTION_BLOCKED' });
});
it('falha simulada de destino retém spool; reinício recupera e replay não consulta SQL Server',async () => {
  const parent = await mkdtemp(join(tmpdir(),'phase4-recover-')),r = join(parent,'work'),state = await prepare(r,'2026-10-02');
  const pool = new pg.Pool({ host: '127.0.0.1',port: state.port,user: 'postgres',password: state.password,database: 'phase4_worker_fixture',options: '-c role=olos_export_worker' });
  try {
    const a = { ...approval(),testId: state.testId,snapshotDate: state.snapshotDate },plan = dispositionPlan(a),b = await components(r,pool,true),source = new ControlledDispositionSource(db.endpoint,plan,coll);
    await expect(once(r,a,() => b.coordinator.receive(plan,source,signal()))).rejects.toMatchObject({ code: 'PERSISTENCE_FAILED' });
    expect((await b.spool.active())?.state).toBe('uncertain'); expect((await pool.query('SELECT count(*) AS n FROM olos_export.disposition')).rows[0].n).toBe('0');
    const restarted = await components(r,pool); expect((await restarted.coordinator.recover(signal())).result?.received).toBe(3);
    const m = await retained(r); await replayDisposition(m,restarted.spool,restarted.target,signal());
    expect((await reconcile(pool,m,restarted.spool)).checkpoint).toBe('1'); expect(source.metrics.calls).toBe(1);
    await expect(once(r,a,async () => 'retry')).rejects.toBeDefined();
  } finally { await pool.end(); await exec('docker',['rm','-f','-v',state.container]); await rm(parent,{ recursive: true,force: true }); }
},30000);
it('contrato divergente e procedimentos extras são recusados; não há chamada de retorno automática',async () => {
  const plan = dispositionPlan(approval()),source = new ControlledDispositionSource(db.endpoint,plan,'0000000000');
  await expect(source.call(fixtureSignature('ReadDataCurrentDay'),{ TableName: 'Disposition' },signal()).next()).rejects.toMatchObject({ code: 'REAL_EXECUTION_BLOCKED' });
  await expect(source.read(plan,signal()).next()).rejects.toMatchObject({ code: 'INVALID_RESPONSE' });
  await expect(source.read(plan,signal()).next()).rejects.toMatchObject({ code: 'REAL_EXECUTION_BLOCKED' });
  expect(source.metrics.calls).toBe(1);
});
it('IDs duplicados no snapshot e outro TableName impedem conclusão; nunca assumem PK global',async () => {
  const plan = dispositionPlan(approval()),source = new ControlledDispositionSource(db.endpoint,plan,coll);
  await expect(source.call(plan.signature,{ TableName: 'Campaign' },signal()).next()).rejects.toMatchObject({ code: 'REAL_EXECUTION_BLOCKED' });
  await db.admin.request().batch("INSERT dbo.DispositionFixture VALUES(1,N'DUPLICATE-TEST')");
  const events: string[] = [];
  try { await expect((async () => { for await (const event of source.read(plan,signal())) events.push(event.kind); })()).rejects.toMatchObject({ code: 'INVALID_RESPONSE' }); }
  finally { await db.admin.request().batch("DELETE FROM dbo.DispositionFixture WHERE Description=N'DUPLICATE-TEST'"); }
  expect(events).not.toContain('complete'); expect(source.metrics.calls).toBe(1);
});
it('claim durável impede duas instâncias e sobrevive à falha do callback',async () => {
  const parent = await mkdtemp(join(tmpdir(),'phase4-claim-'));
  const a = approval(); let calls = 0;
  try {
    const results = await Promise.allSettled([once(parent,a,async () => { calls++; throw new Error('synthetic'); }),once(parent,a,async () => { calls++; })]);
    expect(results.every(r => r.status === 'rejected')).toBe(true); expect(calls).toBe(1);
    await expect(once(parent,a,async () => { calls++; })).rejects.toBeDefined(); expect(calls).toBe(1);
  } finally { await rm(parent,{ recursive: true,force: true }); }
});
it('timeout efetivo não provoca repetição da leitura',async () => {
  await db.admin.request().batch(`ALTER PROCEDURE dbo.usp_ExportData_ReadData @TableName varchar(128),@Message varchar(max) OUTPUT,@ErrorMessage varchar(max) OUTPUT AS BEGIN SET NOCOUNT ON; UPDATE dbo.InvocationCount SET n=n+1; WAITFOR DELAY '00:00:10'; SELECT * FROM dbo.DispositionFixture; END`);
  const plan = dispositionPlan(approval()),source = new ControlledDispositionSource(db.endpoint,plan,coll);
  await expect(source.call(plan.signature,plan.parameters,signal(),500).next()).rejects.toMatchObject({ code: 'TIMEOUT' });
  await expect(source.call(plan.signature,plan.parameters,signal(),500).next()).rejects.toMatchObject({ code: 'REAL_EXECUTION_BLOCKED' });
  expect(source.metrics.calls).toBe(1);
},15000);
it('arquivo de segredo inseguro é recusado; CLI não lê fonte em replay/status nem aceita PG corporativo',async () => {
  const file = join(root,'bad-secret.json'); await writeFile(file,'{"password":"SYNTHETIC"}',{ mode: 0o644 });
  await expect(privateJson(file)).rejects.toBeDefined(); await chmod(file,0o600); expect(await privateJson(file)).toEqual({ password: 'SYNTHETIC' });
  const sourceFile = join(root,'source-config.json'); const { password: _password,...publicConfig } = config; await writeFile(sourceFile,JSON.stringify({ source: publicConfig }),{ mode: 0o600 });
  const approved = spawn(process.execPath,['--import','tsx',fileURLToPath(new URL('../adapters/disposition-integration.ts',import.meta.url)),
    'approve','--work-dir',root,'--source-config',sourceFile,'--reference','TEST-ONLY','--expires-at',approval().expiresAt,'--confirm','AUTHORIZE_READ_DISPOSITION_ONCE'],{ env: { ...process.env,SYNC_ENABLED: 'false',PHASE4_OLOS_PASSWORD: config.password },stdio: ['ignore','pipe','pipe'] });
  let approvalOutput = ''; approved.stdout.on('data',d => { approvalOutput += d; }); approved.stderr.on('data',d => { approvalOutput += d; });
  expect(await new Promise(resolve => approved.on('exit',resolve)),approvalOutput).toBe(0);
  expect(approvalOutput).not.toContain(config.password);
  expect(await privateJson(join(root,'approval.json'))).toMatchObject({ approval: { action: "ReadData('Disposition')",testId: s.testId,reference: 'TEST-ONLY' } });
  const child = spawn(process.execPath,['--import','tsx',fileURLToPath(new URL('../adapters/disposition-integration.ts',import.meta.url)),'status','--work-dir',root,'--pg-url','postgres://corporate'],{ env: { ...process.env,SYNC_ENABLED: 'false' },stdio: ['ignore','pipe','pipe'] });
  let output = ''; child.stdout.on('data',d => { output += d; }); child.stderr.on('data',d => { output += d; });
  expect(await new Promise(resolve => child.on('exit',resolve))).toBe(1); expect(output).toContain('CONTROLLED_INTEGRATION_STOPPED'); expect(output).not.toContain('SYNTHETIC');
});
