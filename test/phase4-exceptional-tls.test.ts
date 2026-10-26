import { beforeAll, afterAll, expect, it } from 'vitest';
import { randomUUID } from 'node:crypto';
import { createServer } from 'node:net';
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { spawn, execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { Request, type ConnectionConfiguration } from 'tedious';
import pg from 'pg';
import { isolatedSqlServer } from './support/sqlserver.js';
import { syntheticTdsTls } from './support/tls-diagnostic-fixture.js';
import { prepare, components, retained, reconcile, once, onceExceptional, privateJson, assertNoSourceAttempt } from '../adapters/disposition-integration.js';
import { exceptionalConfigSchema, exceptionalConfigurationDigest, dispositionTlsMode } from '../adapters/disposition-exception-policy.js';
import { exceptionalTlsMode, exceptionalTlsRisk, exceptionalSourceSchema, validateExceptionalApproval,
  authorizeExceptionalDispositionEndpoint, authorizeIsolatedExceptionalDispositionEndpoint, assertExceptionalCall,
  claimExceptionalConnection, assertExceptionalRpc, type ExceptionalApproval } from '../src/olos/tds/disposition-exception.js';
import { dispositionPlan, ControlledDispositionSource, replayDisposition } from '../src/integration/disposition.js';
import { ExactConnection } from '../src/olos/tds/connection.js';
import { prepareTdsRequest } from '../src/olos/tds/source.js';
import { buildApp } from '../src/app.js';
import { loadConfig } from '../src/config/env.js';

const exec = promisify(execFile), signal = () => new AbortController().signal;
const fixedSource = { server: '10.196.234.11',port: 1433,database: 'ExportDataV3',user: 'OEC',serverName: 'OLOSMADSPODTB01' } as const;
const cliFile = fileURLToPath(new URL('../adapters/disposition-integration.ts',import.meta.url));
function approval(testId: string = randomUUID()): ExceptionalApproval {
  return { testId,action: "ReadData('Disposition')",reference: 'TEST-ONLY-TLS',operator: 'test-operator',
    snapshotDate: '2026-10-01',issuedAt: new Date().toISOString(),expiresAt: new Date(Date.now()+10*60000).toISOString(),
    tlsMode: exceptionalTlsMode,riskConfirmation: exceptionalTlsRisk,configurationDigest: 'a'.repeat(64) };
}
function bindingState() { return { testId: randomUUID(),container: 'a'.repeat(64),port: 15432,password: randomUUID(),snapshotDate: '2026-10-01' }; }

it('modo verificado permanece padrão; variável genérica não seleciona exceção',() => {
  expect(dispositionTlsMode({})).toBe('verified');
  expect(() => dispositionTlsMode({ '--tls-mode': exceptionalTlsMode })).toThrow();
  expect(() => dispositionTlsMode({ '--tls-risk-confirmation': exceptionalTlsRisk })).toThrow();
  expect(dispositionTlsMode({ '--tls-mode': exceptionalTlsMode,'--tls-risk-confirmation': exceptionalTlsRisk })).toBe(exceptionalTlsMode);
});
it.each(['operator','riskConfirmation','configurationDigest','reference','testId','tlsMode'])('aprovação exige %s',field => {
  const a = approval(), bad: Record<string,unknown> = { ...a }; delete bad[field];
  expect(() => validateExceptionalApproval(bad,a.testId)).toThrow();
});
it('aprovação expirada, futura, de outro teste ou acima de 15 minutos é recusada',() => {
  const a = approval();
  for (const bad of [undefined,{ ...a,expiresAt: '2020-01-01T00:00:00.000Z' },{ ...a,issuedAt: new Date(Date.now()+60000).toISOString() },
    { ...a,expiresAt: new Date(Date.now()+16*60000).toISOString() },{ ...a,testId: randomUUID() }])
    expect(() => validateExceptionalApproval(bad,a.testId)).toThrow();
});
it.each([{ server: '127.0.0.1' },{ port: 1434 },{ database: 'master' },{ user: 'sa' },{ procedure: 'ReadDataCurrentDay' },{ trustServerCertificate: true },{ encrypt: false }])('configuração corporativa fixa e sem flags TLS genéricas: %j',change => {
  expect(() => exceptionalSourceSchema.parse({ ...fixedSource,password: 'SYNTHETIC',...change })).toThrow();
});
it('digest vincula credencial, origem, collation e destino completo à aprovação',() => {
  const s = bindingState(), c = exceptionalConfigSchema.parse({ source: fixedSource });
  const digest = exceptionalConfigurationDigest(s,c,'SYNTHETIC');
  expect(digest).toMatch(/^[a-f0-9]{64}$/);
  for (const changed of [{ ...s,testId: randomUUID() },{ ...s,container: 'b'.repeat(64) },{ ...s,port: 15433 },{ ...s,password: 'OTHER' },{ ...s,snapshotDate: '2026-10-02' }])
    expect(exceptionalConfigurationDigest(changed,c,'SYNTHETIC')).not.toBe(digest);
  expect(exceptionalConfigurationDigest(s,c,'ROTATED')).not.toBe(digest);
  expect(exceptionalConfigurationDigest(s,{ ...c,collationHex: '0102030405' },'SYNTHETIC')).not.toBe(digest);
  expect(exceptionalConfigurationDigest(s,{ source: { ...c.source,serverName: 'changed.invalid' } },'SYNTHETIC')).not.toBe(digest);
  expect(exceptionalConfigurationDigest(s,{ source: { ...c.source,ca: 'OTHER_PUBLIC_CA' } },'SYNTHETIC')).not.toBe(digest);
});
it('capacidade corporativa não pode ser emitida em Vitest, Fastify, scheduler ou boot',async () => {
  const a = approval();
  expect(() => authorizeExceptionalDispositionEndpoint({ ...fixedSource,password: 'SYNTHETIC' },a,a)).toThrow();
  const app = buildApp(loadConfig({ LOG_LEVEL: 'silent',SYNC_ENABLED: 'false',PHASE4_TLS_MODE: exceptionalTlsMode }),[]);
  try { expect((await app.inject('/health/live')).statusCode).toBe(200); expect((await app.inject('/health/ready')).statusCode).toBe(503); }
  finally { await app.close(); }
  for (const file of ['src/main.ts','src/app.ts','src/config/env.ts','Dockerfile']) {
    const source = await readFile(new URL(`../${file}`,import.meta.url),'utf8');
    expect(source).not.toContain('exceptional-unverified-test'); expect(source).not.toContain('disposition-exception');
  }
});
it('claim excepcional é durável, concorrente, compartilhado com modo seguro e não apagado após erro',async () => {
  const root = await mkdtemp(join(tmpdir(),'phase4-tls-claim-')); const a = approval(); let calls = 0;
  try {
    // Either filesystem contender can win; the winner must fail to exercise durable failure fencing.
    const failAfterClaim=async()=>{calls++;throw new Error('synthetic');};
    const results = await Promise.allSettled([onceExceptional(root,a,failAfterClaim),onceExceptional(root,a,failAfterClaim)]);
    expect(results.every(r => r.status === 'rejected')).toBe(true); expect(calls).toBe(1);
    await expect(assertNoSourceAttempt(root)).rejects.toBeDefined();
    const { tlsMode: _,riskConfirmation: _r,operator: _o,issuedAt: _i,configurationDigest: _d,...base } = a;
    await expect(once(root,base,async () => { calls++; })).rejects.toBeDefined();
    expect(calls).toBe(1); expect(await privateJson(join(root,'source-attempt.json'))).toMatchObject({ serverIdentityValidated: false,tlsMode: exceptionalTlsMode,testId: a.testId });
  } finally { await rm(root,{ recursive: true,force: true }); }
});

let sql: Awaited<ReturnType<typeof isolatedSqlServer>>;
let parent: string, root: string, s: Awaited<ReturnType<typeof prepare>>, p: pg.Pool;
beforeAll(async () => {
  sql = await isolatedSqlServer({ selfSignedFallback: true });
  parent = await mkdtemp(join(tmpdir(),'phase4-exceptional-tls-')); root = join(parent,'work');
  s = await prepare(root,'2026-10-01');
  p = new pg.Pool({ host: '127.0.0.1',port: s.port,user: 'postgres',password: s.password,database: 'phase4_worker_fixture',options: '-c role=olos_export_worker' });
  await sql.admin.request().batch(`CREATE TABLE dbo.DispositionFixture(DispositionId int NOT NULL,Description nvarchar(200) NOT NULL);
    INSERT dbo.DispositionFixture VALUES(1,N'Synthetic 😀'),(2,N''),(3,N'  Á  ');
    CREATE TABLE dbo.InvocationCount(n int NOT NULL,encrypted varchar(10) NOT NULL); INSERT dbo.InvocationCount VALUES(0,'UNKNOWN');`);
  await sql.admin.request().batch(`CREATE PROCEDURE dbo.usp_ExportData_ReadData @TableName varchar(128),@Message varchar(max) OUTPUT,@ErrorMessage varchar(max) OUTPUT AS
    BEGIN SET NOCOUNT ON; UPDATE dbo.InvocationCount SET n=n+1,encrypted=(SELECT encrypt_option FROM sys.dm_exec_connections WHERE session_id=@@SPID);
    SELECT DispositionId,Description FROM dbo.DispositionFixture; SET @Message=NULL; SET @ErrorMessage=NULL; RETURN 0; END`);
},120000);
afterAll(async () => { await p?.end(); if (s) await exec('docker',['rm','-f','-v',s.container]); await sql?.stop(); if (parent) await rm(parent,{ recursive: true,force: true }); },30000);
function isolatedPermit(a = approval(s.testId), overrides: Partial<typeof sql.endpoint> = {}) {
  return authorizeIsolatedExceptionalDispositionEndpoint({ ...sql.endpoint,...overrides },a,a);
}
function connectionConfig(endpoint = isolatedPermit()): ConnectionConfiguration {
  return { server: endpoint.server,authentication: { type: 'default',options: { userName: endpoint.user,password: endpoint.password } },
    options: { database: endpoint.database,port: endpoint.port,serverName: 'localhost',encrypt: true,trustServerCertificate: true,
      cryptoCredentialsDetails: { ca: endpoint.ca,minVersion: 'TLSv1.2' },maxRetriesOnTransientErrors: 0,tdsVersion: '7_4' } };
}
it('fixture excepcional não permite host/banco corporativo nem credenciais alternativas',() => {
  const a = approval();
  for (const changed of [{ server: '10.196.234.11' },{ database: 'ExportDataV3' },{ user: 'OEC' }])
    expect(() => authorizeIsolatedExceptionalDispositionEndpoint({ ...sql.endpoint,...changed } as typeof sql.endpoint,a,a)).toThrow();
});
it('sem capacidade excepcional, autoassinado sem SAN falha no modo seguro antes do RPC',async () => {
  const plan = dispositionPlan(approval(s.testId)), source = new ControlledDispositionSource(sql.endpoint,plan);
  await expect(source.read(plan,signal()).next()).rejects.toMatchObject({ code: 'CONNECTION_FAILED' });
  expect(source.metrics.calls).toBe(0);
  expect(() => new ExactConnection(connectionConfig())).toThrow();
});
it.each(['encrypt','minVersion','retry','trust','port','database','connector','cipher'])('gate da conexão excepcional recusa %s alterado',field => {
  const endpoint = isolatedPermit(), config = connectionConfig(endpoint);
  assertExceptionalCall(endpoint,'ReadData',{ TableName: 'Disposition' });
  const options = config.options!;
  if (field === 'encrypt') options.encrypt = false;
  if (field === 'trust') options.trustServerCertificate = false;
  if (field === 'minVersion') options.cryptoCredentialsDetails!.minVersion = 'TLSv1.1';
  if (field === 'retry') options.maxRetriesOnTransientErrors = 1;
  if (field === 'port') options.port = endpoint.port+1;
  if (field === 'database') options.database = 'master';
  if (field === 'connector') options.connector = async () => { throw new Error('must not connect'); };
  if (field === 'cipher') options.cryptoCredentialsDetails!.ciphers = 'DEFAULT:@SECLEVEL=0';
  expect(() => claimExceptionalConnection(config,endpoint)).toThrow();
});
it('capacidade só admite Disposition, ReadData e exatamente um RPC',() => {
  const endpoint = isolatedPermit(), plan = dispositionPlan(approval(s.testId));
  expect(() => assertExceptionalCall(endpoint,'ReadDataCurrentDay',{ TableName: 'Disposition' })).toThrow();
  expect(() => assertExceptionalCall(endpoint,'ReadData',{ TableName: 'Campaign' })).toThrow();
  assertExceptionalCall(endpoint,'ReadData',{ TableName: 'Disposition' });
  claimExceptionalConnection(connectionConfig(endpoint),endpoint);
  const request = new Request('dbo.usp_ExportData_ReadData',() => {}); prepareTdsRequest(request,plan.signature,plan.parameters,30000);
  const wrong = new Request('dbo.usp_ExportData_ResetReadData',() => {});
  expect(() => assertExceptionalRpc(wrong,endpoint)).toThrow();
  assertExceptionalRpc(request,endpoint);
  expect(() => assertExceptionalRpc(request,endpoint)).toThrow();
  expect(() => claimExceptionalConnection(connectionConfig(endpoint),endpoint)).toThrow();
});
it('pipeline real isolado aceita autoassinado com exceção, comprova criptografia no servidor e faz replay local',async () => {
  const a = approval(s.testId), plan = dispositionPlan(a), b = await components(root,p), reader = new ControlledDispositionSource(isolatedPermit(a),plan);
  await onceExceptional(root,a,() => b.coordinator.receive(plan,reader,signal()));
  const m = await retained(root), before = await reconcile(p,m,b.spool);
  expect(before.records).toBe(3); expect(before.checkpoint).toBe('1');
  expect(reader.transportSecurity).toEqual([{ encrypted: true,identityValidated: false,protocol: 'TLSv1.2',tlsMode: exceptionalTlsMode }]);
  expect((await sql.admin.request().query('SELECT n,encrypted FROM dbo.InvocationCount')).recordset[0]).toEqual({ n: 1,encrypted: 'TRUE' });
  await replayDisposition(m,b.spool,b.target,signal()); expect(await reconcile(p,m,b.spool)).toEqual(before);
  expect((await sql.admin.request().query('SELECT n FROM dbo.InvocationCount')).recordset[0].n).toBe(1);
  expect(reader.metrics.calls).toBe(1); expect(reader.metrics.pauses).toBe(3);
  await expect(reader.read(plan,signal()).next()).rejects.toMatchObject({ code: 'REAL_EXECUTION_BLOCKED' });
},30000);
it('falha de destino preserva spool selado, reinício recupera localmente sem novo RPC',async () => {
  const dir = await mkdtemp(join(tmpdir(),'phase4-tls-recovery-')), work = join(dir,'work'), state = await prepare(work,'2026-10-01');
  const pool = new pg.Pool({ host: '127.0.0.1',port: state.port,user: 'postgres',password: state.password,database: 'phase4_worker_fixture',options: '-c role=olos_export_worker' });
  try {
    const a = approval(state.testId), plan = dispositionPlan(a), b = await components(work,pool,true), reader = new ControlledDispositionSource(isolatedPermit(a),plan);
    const n = (await sql.admin.request().query('SELECT n FROM dbo.InvocationCount')).recordset[0].n;
    await expect(onceExceptional(work,a,() => b.coordinator.receive(plan,reader,signal()))).rejects.toMatchObject({ code: 'PERSISTENCE_FAILED' });
    const sealed = await retained(work); expect(sealed.sealed).toBe(true); expect(sealed.state).toBe('uncertain');
    expect((await pool.query('SELECT count(*) AS n FROM olos_export.disposition')).rows[0].n).toBe('0');
    const restarted = await components(work,pool); expect((await restarted.coordinator.recover(signal())).result?.received).toBe(3);
    await replayDisposition(await retained(work),restarted.spool,restarted.target,signal());
    expect((await reconcile(pool,await retained(work),restarted.spool)).checkpoint).toBe('1');
    expect((await sql.admin.request().query('SELECT n FROM dbo.InvocationCount')).recordset[0].n).toBe(n+1);
    await expect(onceExceptional(work,a,async () => 'retry')).rejects.toBeDefined();
  } finally { await pool.end(); await exec('docker',['rm','-f','-v',state.container]); await rm(dir,{ recursive: true,force: true }); }
},30000);
it('conexão recusada não dispara RPC ou retry, e o claim continua bloqueando outra tentativa',async () => {
  const dir = await mkdtemp(join(tmpdir(),'phase4-tls-network-')), a = approval(), plan = dispositionPlan(a);
  const server = createServer(); await new Promise<void>(resolve => server.listen(0,'127.0.0.1',resolve));
  const address = server.address(); if (!address || typeof address === 'string') throw new Error('fixture');
  await new Promise<void>(resolve => server.close(() => resolve()));
  const reader = new ControlledDispositionSource(isolatedPermit(a,{ port: address.port }),plan);
  try {
    await expect(onceExceptional(dir,a,() => reader.read(plan,signal()).next())).rejects.toMatchObject({ code: 'CONNECTION_FAILED' });
    await expect(reader.read(plan,signal()).next()).rejects.toMatchObject({ code: 'REAL_EXECUTION_BLOCKED' });
    await expect(onceExceptional(dir,a,async () => 'retry')).rejects.toBeDefined(); expect(reader.metrics.calls).toBe(0);
  } finally { await rm(dir,{ recursive: true,force: true }); }
});
it('servidor que responde ENCRYPT_OFF nunca recebe LOGIN7 nem credenciais',async () => {
  const server = await syntheticTdsTls({ cert: '',key: '',response: Buffer.from([1,0,6,0,1,255,0]),stallTls: true });
  try {
    const a = approval(), plan = dispositionPlan(a), reader = new ControlledDispositionSource(isolatedPermit(a,{ port: server.port }),plan);
    await expect(reader.call(plan.signature,plan.parameters,signal(),1000).next()).rejects.toMatchObject({ code: 'CONNECTION_FAILED' });
    expect(server.sessions).toHaveLength(1); expect(server.sessions[0]!.types).toEqual([0x12]);
    expect(reader.metrics.calls).toBe(0); expect(reader.transportSecurity).toEqual([]);
  } finally { await server.stop(); }
});
it('falha de rede depois do TCP gera exatamente uma conexão, sem reconectar',async () => {
  let attempts = 0;
  const server = createServer(socket => { attempts++; socket.destroy(); });
  await new Promise<void>(resolve => server.listen(0,'127.0.0.1',resolve));
  const address = server.address(); if (!address || typeof address === 'string') throw new Error('fixture');
  try {
    const a = approval(), plan = dispositionPlan(a), reader = new ControlledDispositionSource(isolatedPermit(a,{ port: address.port }),plan);
    await expect(reader.read(plan,signal()).next()).rejects.toMatchObject({ code: 'CONNECTION_FAILED' });
    await new Promise(resolve => setTimeout(resolve,100)); expect(attempts).toBe(1); expect(reader.metrics.calls).toBe(0);
    await expect(reader.read(plan,signal()).next()).rejects.toMatchObject({ code: 'REAL_EXECUTION_BLOCKED' });
  } finally { await new Promise<void>(resolve => server.close(() => resolve())); }
});

async function cli(args: string[]) {
  const child = spawn(process.execPath,['--import','tsx',cliFile,...args],{
    env: { ...process.env,SYNC_ENABLED: 'false',PHASE4_OLOS_PASSWORD: 'SYNTHETIC-CLI-NO-CONNECTION' },stdio: ['ignore','pipe','pipe'] });
  let output = ''; child.stdout.on('data',d => { output += d; }); child.stderr.on('data',d => { output += d; });
  const code = await new Promise<number | null>((resolve,reject) => { child.on('exit',resolve); child.on('error',reject); });
  expect(output).not.toContain('SYNTHETIC-CLI-NO-CONNECTION'); return { code,output };
}
it('CLI só aprova exceção com risco/operador e recusa digest alterado antes do claim e da origem',async () => {
  const file = join(root,'exceptional-config.json'); await writeFile(file,JSON.stringify({ source: fixedSource }),{ mode: 0o600 });
  // Fresh durable state only for this approval test; the main root has already attempted a read.
  const dir = await mkdtemp(join(tmpdir(),'phase4-tls-cli-')), work = join(dir,'work'), state = await prepare(work,'2026-10-01');
  try {
    const common = ['--work-dir',work,'--source-config',file,'--tls-mode',exceptionalTlsMode];
    const read = ['read',...common,'--confirm','READ_DISPOSITION_ONCE','--tls-risk-confirmation',exceptionalTlsRisk];
    expect((await cli(read)).code).toBe(1); await assertNoSourceAttempt(work);
    expect((await cli([...read,'--pg-url','postgres://corporate'])).code).toBe(1);
    const approve = ['approve',...common,'--reference','TEST-ONLY','--operator','test-operator','--expires-at',new Date(Date.now()+10*60000).toISOString(),'--confirm','AUTHORIZE_READ_DISPOSITION_ONCE'];
    expect((await cli(approve)).code).toBe(1);
    expect((await cli([...approve,'--tls-risk-confirmation',exceptionalTlsRisk])).code).toBe(0);
    const accepted = await privateJson(join(work,'exceptional-tls-approval.json'));
    expect(accepted).toMatchObject({ testId: state.testId,tlsMode: exceptionalTlsMode,operator: 'test-operator' });
    await writeFile(join(work,'exceptional-tls-approval.json'),JSON.stringify({ ...(accepted as ExceptionalApproval),expiresAt: '2020-01-01T00:00:00.000Z' }),{ mode: 0o600 });
    expect((await cli(read)).code).toBe(1); await assertNoSourceAttempt(work);
    await writeFile(join(work,'exceptional-tls-approval.json'),JSON.stringify(accepted),{ mode: 0o600 });
    await writeFile(file,JSON.stringify({ source: { ...fixedSource,serverName: 'changed.invalid' } }),{ mode: 0o600 });
    expect((await cli(['read',...common,'--confirm','READ_DISPOSITION_ONCE','--tls-risk-confirmation',exceptionalTlsRisk])).code).toBe(1);
    await assertNoSourceAttempt(work);
    const badState = { ...state,testId: randomUUID() }; await writeFile(join(work,'state.json'),JSON.stringify(badState),{ mode: 0o600 });
    await writeFile(join(work,'ready.json'),JSON.stringify({ testId: badState.testId }),{ mode: 0o600 });
    expect((await cli([...approve,'--tls-risk-confirmation',exceptionalTlsRisk])).code).toBe(1);
  } finally { await exec('docker',['rm','-f','-v',state.container]); await rm(dir,{ recursive: true,force: true }); }
},30000);
