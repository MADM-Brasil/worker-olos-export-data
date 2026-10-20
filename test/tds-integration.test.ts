import { spawn } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { afterAll, beforeAll, expect, it } from 'vitest';
import { mkdtemp, rm, readFile, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { isolatedSqlServer } from './support/sqlserver.js';
import { IsolatedTdsSource, type TdsEvent } from '../src/olos/tds/source.js';
import type { TdsField } from '../src/olos/tds/raw-parser.js';
import { fixtureSignature } from './support/olos-documented-fixture.js';
import { decimalFromTdsBody, legacyDatetimeFromTdsBody } from '../src/olos/exact-value-prototypes.js';
import { largePlan } from './support/large-batch-fixture.js';
import { isolatedPostgres } from './support/postgres.js';
import { LocalSpool } from '../adapters/local-spool.js';
import { spoolLimits } from '../src/large-batch/config.js';
import { PostgresPersistence } from '../src/persistence/store.js';
import { PostgresLargeBatchPersistence } from '../src/persistence/large-batch.js';
import { LargeBatchCoordinator } from '../src/large-batch/coordinator.js';

let db: Awaited<ReturnType<typeof isolatedSqlServer>>;
let fields: TdsField[], collationHex: string;
const signal = () => new AbortController().signal;
beforeAll(async () => {
  db = await isolatedSqlServer();
  await db.admin.request().batch(`CREATE TABLE dbo.ExactFixture (
    Id bigint NOT NULL, Amount decimal(38,18) NULL, At datetime NULL,
    UnicodeText nvarchar(100) NULL, RawText varchar(100) COLLATE Latin1_General_100_CI_AS NULL, FixedText char(4) COLLATE Latin1_General_100_CI_AS NULL);
    INSERT dbo.ExactFixture VALUES (9223372036854775807,12345678901234567890.123456789012345678,
      DATEADD(millisecond,3,CONVERT(datetime,'20200603',112)),N'á😀',CHAR(128)+'X','A  '),(-9223372036854775808,NULL,NULL,N'',NULL,'');`);
  // Colunas atuais desta fixture (não da Olos). Capturadas antes da execução do caminho binário.
  // Collation binária via metadados TDS de um Request Tedious isolado, sem dados pessoais.
  const { Connection,Request } = await import('tedious');
  const c = new Connection({ server: db.endpoint.server, authentication: { type: 'default',options: { userName: 'sa',password: db.endpoint.password } },options: {
    database: 'phase3d_fixture',port: db.endpoint.port,encrypt: true,trustServerCertificate: false,serverName: 'localhost',cryptoCredentialsDetails: { ca: db.endpoint.ca } } });
  await new Promise<void>((resolve,reject) => c.connect(e => e ? reject(e) : resolve()));
  const metadata: TdsField[] = [];
  const r = new Request('SELECT * FROM dbo.ExactFixture WHERE 1=0',error => { if (error) throw error; });
  r.on('columnMetadata',m => { if (!Array.isArray(m)) throw new Error('Unexpected metadata'); collationHex = m[4]!.collation!.toBuffer().toString('hex');
    const types = ['bigint','decimal','datetime','nvarchar','varchar','char'] as const;
    for (let i=0;i<m.length;i++) {
      const f: TdsField = { name: m[i]!.colName,type: types[i]!,nullable: Boolean(m[i]!.flags & 1),
        ...(i===1 ? { precision: 38,scale: 18 } : {}),
        ...(i>=3 ? { maxBytes: i===3 ? 200 : i===4 ? 100 : 4,sqlLength: i===5 ? 4 : 100,collationHex: m[i]!.collation!.toBuffer().toString('hex'),...(i===3 ? {} : { encoding: 'windows-1252' as const }) } : {}) };
      metadata.push(f);
    }
  });
  await new Promise<void>((resolve,reject) => { r.on('requestCompleted',resolve); r.on('error',reject); c.execSql(r); }); c.close(); fields = metadata;
  await db.admin.request().batch(`CREATE PROCEDURE dbo.usp_ExportData_ReadData @TableName varchar(128),@Message varchar(max) OUTPUT,@ErrorMessage varchar(max) OUTPUT AS
    BEGIN SET NOCOUNT ON; SELECT * FROM dbo.ExactFixture; SET @Message='synthetic'; SET @ErrorMessage=NULL; RETURN 0; END`);
},120000);
afterAll(async () => { await db?.stop(); });

it('SQL Server efetivo: ticks DATETIME, BIGINT, DECIMAL, Unicode, bytes CHAR/VARCHAR, null e outputs exatos',async () => {
  const source = new IsolatedTdsSource(db.endpoint,fields), rows = [];
  let complete;
  for await (const event of source.call(fixtureSignature('ReadData'),{ TableName: 'Disposition' },signal())) {
    if (event.kind === 'row') rows.push(event.values); else complete = event;
  }
  expect(rows).toHaveLength(2);
  expect(Buffer.from(rows[0]![0]!).readBigInt64LE()).toBe(9223372036854775807n);
  expect(Buffer.from(rows[1]![0]!).readBigInt64LE()).toBe(-9223372036854775808n);
  expect(decimalFromTdsBody(rows[0]![1]!,38,18)).toBe('12345678901234567890.123456789012345678');
  expect(legacyDatetimeFromTdsBody(rows[0]![2]!)?.ticks300).toBe('1');
  expect(Buffer.from(rows[0]![3]!).toString('utf16le')).toBe('á😀');
  expect(Buffer.from(rows[0]![4]!)).toEqual(Buffer.from([128,88]));
  expect(Buffer.from(rows[0]![5]!).toString()).toBe('A   ');
  expect(rows[1]!.slice(1,3)).toEqual([null,null]); expect(rows[1]![3]).toHaveLength(0);
  expect(complete).toMatchObject({ kind: 'complete',returnValue: 0,datasetCount: 1,message: 'synthetic',errorMessage: null });
  expect(source.metrics.peakQueuedRows).toBe(1); expect(source.metrics.pauses).toBe(2);
},30000);

it('assinaturas oficiais das cinco procedures, DATE, NVARCHAR e extremos BIGINT em RPC real isolado',async () => {
  const signatures = {
    ReadDataCurrentDay: '@TableName varchar(128),@Message varchar(4000) OUTPUT,@ErrorMessage varchar(4000) OUTPUT',
    ReadDataPreviousDays: '@TableName varchar(128),@Message varchar(4000) OUTPUT,@ErrorMessage varchar(4000) OUTPUT',
    LogReadData: '@tablename nvarchar(128),@DateExport date,@Message varchar(4000) OUTPUT,@ErrorMessage varchar(4000) OUTPUT',
    ReReadData: '@TableName varchar(128),@DateExport date,@PKIDStart bigint,@PKIDEnd bigint,@Message varchar(4000) OUTPUT,@ErrorMessage varchar(4000) OUTPUT',
  } as const;
  for (const [name,args] of Object.entries(signatures)) {
    const proc = name as keyof typeof signatures;
    const condition = proc === 'ReReadData' ? "IF @PKIDStart <> -9223372036854775808 OR @PKIDEnd <> 9223372036854775807 OR @DateExport <> '20200603' THROW 50000,'invalid fixture input',1;" : proc === 'LogReadData' ? "IF @DateExport <> '20200603' OR @tablename <> N'Users' THROW 50000,'invalid fixture input',1;" : '';
    await db.admin.request().batch(`CREATE OR ALTER PROCEDURE dbo.usp_ExportData_${name} ${args} AS BEGIN SET NOCOUNT ON;
      ${condition} SELECT * FROM dbo.ExactFixture; SET @Message='fixture'; SET @ErrorMessage=NULL; RETURN 7; END`);
    const values = proc === 'LogReadData' ? { tablename: 'Users',DateExport: '2020-06-03' } : proc === 'ReReadData' ?
      { TableName: 'AttemptsRawData',DateExport: '2020-06-03',PKIDStart: '-9223372036854775808',PKIDEnd: '9223372036854775807' } : { TableName: 'AttemptsRawData' };
    const source = new IsolatedTdsSource(db.endpoint,fields);
    const events: TdsEvent[] = []; for await (const event of source.call(fixtureSignature(proc),values,signal())) events.push(event);
    expect(events.at(-1)).toMatchObject({ kind: 'complete',returnValue: 7,message: 'fixture' });
    expect(events.filter(e => e.kind === 'row')).toHaveLength(2);
  }
},30000);

it('pausa efetiva, retomada, exclusão e cancelamento sem reutilizar execução pendente',async () => {
  const source = new IsolatedTdsSource(db.endpoint,fields), abort = new AbortController();
  const iterator = source.call(fixtureSignature('ReadData'),{ TableName: 'Users' },abort.signal);
  expect((await iterator.next()).value?.kind).toBe('row');
  const paused = source.metrics.rows; await new Promise(r => setTimeout(r,50)); expect(source.metrics.rows).toBe(paused);
  await expect(source.call(fixtureSignature('ReadData'),{ TableName: 'Users' },signal()).next()).rejects.toMatchObject({ code: 'BUSY' });
  abort.abort(); await expect(iterator.next()).rejects.toMatchObject({ code: 'ABORTED',outcome: 'unknown' });
  const again = source.call(fixtureSignature('ReadData'),{ TableName: 'Users' },signal());
  expect((await again.next()).value?.kind).toBe('row'); await again.return(undefined);
},30000);

it('erro após resposta parcial, timeout e datasets extras nunca produzem complete',async () => {
  const source = new IsolatedTdsSource(db.endpoint,fields);
  for (const body of ["SELECT * FROM dbo.ExactFixture; THROW 50001,'synthetic error only',1;", "SELECT * FROM dbo.ExactFixture; WAITFOR DELAY '00:00:10';", "SELECT * FROM dbo.ExactFixture; SELECT * FROM dbo.ExactFixture;"]) {
    await db.admin.request().batch(`CREATE OR ALTER PROCEDURE dbo.usp_ExportData_ReadDataPreviousDays @TableName varchar(128),@Message varchar(4000) OUTPUT,@ErrorMessage varchar(4000) OUTPUT AS BEGIN SET NOCOUNT ON; ${body} SET @Message=NULL; SET @ErrorMessage=NULL; END`);
    const events: TdsEvent[] = [];
    await expect((async () => { for await (const event of source.call(fixtureSignature('ReadDataPreviousDays'),{ TableName: 'AttemptsRawData' },signal(),500)) events.push(event); })()).rejects.toMatchObject({ outcome: 'unknown' });
    expect(events.some(e => e.kind === 'complete')).toBe(false);
  }
},30000);

it('metadados incompatíveis, outputs de erro e resposta vazia são diferenciados em SQL Server',async () => {
  await expect((async () => { for await (const _ of new IsolatedTdsSource(db.endpoint,[...fields].reverse()).call(fixtureSignature('ReadData'),{ TableName: 'Users' },signal())) {} })()).rejects.toMatchObject({ code: 'INVALID_RESPONSE' });
  await db.admin.request().batch(`CREATE OR ALTER PROCEDURE dbo.usp_ExportData_ReadDataPreviousDays @TableName varchar(128),@Message varchar(4000) OUTPUT,@ErrorMessage varchar(4000) OUTPUT AS BEGIN SET NOCOUNT ON; SELECT * FROM dbo.ExactFixture WHERE 1=0; SET @Message=NULL; SET @ErrorMessage='synthetic refusal'; RETURN 9; END`);
  const events: TdsEvent[] = []; for await (const event of new IsolatedTdsSource(db.endpoint,fields).call(fixtureSignature('ReadDataPreviousDays'),{ TableName: 'AttemptsRawData' },signal())) events.push(event);
  expect(events).toHaveLength(1); expect(events[0]).toMatchObject({ kind: 'complete',returnValue: 9,errorMessage: 'synthetic refusal' });
},30000);

it('DATE retornado por LogReadData é adquirido em três bytes, sem Date',async () => {
  await db.admin.request().batch(`CREATE OR ALTER PROCEDURE dbo.usp_ExportData_LogReadData @tablename nvarchar(128),@DateExport date,@Message varchar(4000) OUTPUT,@ErrorMessage varchar(4000) OUTPUT AS BEGIN SET NOCOUNT ON; SELECT @DateExport AS DateExport; SET @Message=NULL; SET @ErrorMessage=NULL; RETURN 0; END`);
  const source = new IsolatedTdsSource(db.endpoint,[{ name: 'DateExport',type: 'date',nullable: true }]);
  const events: TdsEvent[] = []; for await (const e of source.call(fixtureSignature('LogReadData'),{ tablename: 'Users',DateExport: '2020-06-03' },signal())) events.push(e);
  const row = events[0]; expect(row?.kind).toBe('row');
  if (row?.kind !== 'row') throw new Error('Expected binary date');
  expect(Buffer.from(row.values[0]!).toString('hex')).toBe('2a410b');
},30000);

it('TLS sem CA válida falha antes do dispatch; uma nova chamada explícita válida reconecta',async () => {
  const source = new IsolatedTdsSource({ ...db.endpoint,ca: '-----BEGIN CERTIFICATE-----invalid-----END CERTIFICATE-----' },fields);
  await expect(source.call(fixtureSignature('ReadData'),{ TableName: 'Users' },signal()).next()).rejects.toMatchObject({ code: 'CONNECTION_FAILED',outcome: 'not_sent' });
  expect(source.metrics.calls).toBe(0);
  const good = new IsolatedTdsSource(db.endpoint,fields), iterator = good.call(fixtureSignature('ReadData'),{ TableName: 'Users' },signal());
  expect((await iterator.next()).value?.kind).toBe('row'); await iterator.return(undefined);
},30000);

it('200.000 registros de SQL Server → TLS/TDS → spool → PostgreSQL, heap limitado e checkpoint único',async () => {
  const pg = await isolatedPostgres(), root = await mkdtemp(join(tmpdir(),'olos-real-tds-'));
  try {
    const plan = largePlan();
    const wire = plan.fields.map(f => f.type === 'varchar' ? { ...f,encoding: 'windows-1252' as const } : f);
    const fullFields: TdsField[] = wire.map(f => ({ ...f,...(f.type === 'varchar' ? { sqlLength: f.name === 'CampaignData' ? 'max' as const : f.maxBytes!,collationHex } : {}) }));
    const columns = wire.map(f => `[${f.name}] ${f.type === 'varchar' ? `varchar(${f.name === 'CampaignData' ? 'max' : f.maxBytes}) COLLATE Latin1_General_100_CI_AS` : f.type} ${f.nullable ? 'NULL' : 'NOT NULL'}`).join(',');
    const select = wire.map(f => f.name === 'AttemptsRawDataID' ? 'CONVERT(int,ROW_NUMBER() OVER (ORDER BY (SELECT NULL)))' :
      f.type === 'int' ? '0' : f.type === 'datetime' ? "DATEADD(millisecond,3,CONVERT(datetime,'20200603',112))" : f.name === 'CampaignData' ? "CAST('synthetic Á' AS varchar(max))" : "'X'").join(',');
    await db.admin.request().batch(`CREATE TABLE dbo.AttemptsFixture (${columns}); INSERT dbo.AttemptsFixture SELECT TOP(200000) ${select} FROM sys.all_objects a CROSS JOIN sys.all_objects b;`);
    await db.admin.request().batch(`CREATE OR ALTER PROCEDURE dbo.usp_ExportData_ReadDataCurrentDay @TableName varchar(128),@Message varchar(4000) OUTPUT,@ErrorMessage varchar(4000) OUTPUT AS BEGIN SET NOCOUNT ON; SELECT * FROM dbo.AttemptsFixture; SET @Message='synthetic'; SET @ErrorMessage=NULL; RETURN 0; END`);
    const p = { ...plan,fields: wire };
    await pg.admin.query(await readFile(new URL('../migrations/005_large_batch_control.sql',import.meta.url),'utf8'));
    await pg.admin.query(await readFile(new URL('../permissions/large-batch.sql',import.meta.url),'utf8'));
    const c = p.contract;
    await pg.admin.query(`INSERT INTO olos_export.source_contracts(source_table,version,approved,fields,key_fields,layout_evidence,key_evidence) VALUES ($1,$2,true,$3::jsonb,$4,$5,$6)`,[c.source_table,c.version,JSON.stringify(c.fields),c.key_fields,c.layout_evidence,c.key_evidence]);
    const child = spawn(process.execPath,['--expose-gc','--max-old-space-size=128','--max-semi-space-size=4','--import','tsx',fileURLToPath(new URL('./support/tds-load.ts',import.meta.url))],{ stdio: ['pipe','pipe','pipe'] });
    let out = '', errors = '';
    child.stdout.on('data',b => { if (out.length > 65536) child.kill(); else out += b.toString(); });
    child.stderr.on('data',b => { if (errors.length < 4096) errors += b.toString(); });
    const timeout = setTimeout(() => child.kill('SIGKILL'),180000);
    child.stdin.end(JSON.stringify({ root,connection: pg.connection,endpoint: db.endpoint,plan: p,fields: fullFields }));
    const exit = await new Promise<number | null>((resolve,reject) => { child.on('error',reject); child.on('exit',resolve); }).finally(() => clearTimeout(timeout));
    expect(exit,errors).toBe(0);
    const report = JSON.parse(out);
    expect(report.result).toEqual({ received: 200000,persisted: 200000,duplicates: 0,rejected: 0 });
    expect(report.saved).toBe('200000'); expect(report.checkpoint).toBe('1');
    expect(report.sourceMetrics).toMatchObject({ calls: 1,rows: 200000,pauses: 200000,resumes: 200000,peakQueuedRows: 1 });
    expect(report.peakHeap).toBeLessThan(report.heapLimit); expect(report.heapLimit).toBeLessThanOrEqual(140*1024**2);
    expect(report.peakBufferedBytes).toBeLessThanOrEqual(65536);
    await writeFile('/tmp/phase3d-real-load-results.json',JSON.stringify({ sqlServerVersion: db.version,...report },null,2),{ mode: 0o600 });
  } finally { await pg.stop(); await rm(root,{ recursive: true,force: true }); }
},240000);

it('perda efetiva da conexão SQL Server após linhas: sem terminal nem replay remoto automático',async () => {
  await db.admin.request().batch(`CREATE OR ALTER PROCEDURE dbo.usp_ExportData_ReadDataPreviousDays @TableName varchar(128),@Message varchar(4000) OUTPUT,@ErrorMessage varchar(4000) OUTPUT AS BEGIN SET NOCOUNT ON; SELECT TOP(1000) e.* FROM dbo.ExactFixture e CROSS JOIN sys.all_objects a; RAISERROR(N'fixture flush',0,1) WITH NOWAIT; WAITFOR DELAY '00:00:30'; END`);
  const source = new IsolatedTdsSource(db.endpoint,fields), iterator = source.call(fixtureSignature('ReadDataPreviousDays'),{ TableName: 'AttemptsRawData' },signal());
  expect((await iterator.next()).value?.kind).toBe('row');
  await db.disconnect();
  await expect(iterator.next()).rejects.toMatchObject({ outcome: 'unknown' });
  expect(source.metrics.calls).toBe(1);
},30000);
