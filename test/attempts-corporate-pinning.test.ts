import { beforeAll, afterAll, expect, it, vi } from 'vitest';
import { Socket } from 'node:net';
import { X509Certificate, randomUUID } from 'node:crypto';
import type { ConnectionConfiguration } from 'tedious';
import { Request } from 'tedious';
import { parseOlosPinApproval } from '../src/security/olos-certificate-pin.js';
import { prepareCorporateAttemptsEndpoint, matchesPreparedAttemptsConnection, assertCorporateAttemptsExecution } from '../src/olos/tds/attempts-access.js';
import { ExactConnection } from '../src/olos/tds/connection.js';
import {InspectionDiagnostics} from '../src/olos/tds/inspection-diagnostics.js';
import { PinnedMessageIO } from '../src/diagnostics/pinned-message-io.js';
import { IsolatedTdsSource } from '../src/olos/tds/source.js';
import { reportedSignature, verifiedProcedureContracts } from '../src/olos/procedure-signatures.js';
import type { Procedure } from '../src/olos/contracts.js';
import { attemptsFixturePlan, attemptsFixtureFields } from '../src/integration/attempts/contracts.js';
import { planAttemptsRecovery, planCorporateAttemptsRecovery, rereadLargePlan, type LogInterval } from '../src/integration/attempts/recovery.js';
import { prepareCorporateAttemptsReRead, prepareCorporateAttemptsRecoveryPort } from '../src/integration/attempts/corporate-recovery.js';
import { LargeBatchCoordinator } from '../src/large-batch/coordinator.js';
import { spoolLimits } from '../src/large-batch/config.js';
import type { SpoolManifest } from '../src/large-batch/types.js';
import { syntheticCertificates, syntheticTdsTls } from './support/tls-diagnostic-fixture.js';
let certs: Awaited<ReturnType<typeof syntheticCertificates>>;
beforeAll(async () => { certs = await syntheticCertificates(); },30000);
afterAll(async () => { await certs?.stop(); });
const config = { server: '10.196.234.11',port: 1433,database: 'ExportDataV3',user: 'OEC',password: 'SYNTHETIC-NOT-A-REAL-CREDENTIAL' };
function pin(purpose = 'OLOS_ATTEMPTS_TDS_ACQUISITION') { return parseOlosPinApproval({ version: 1,purpose,host: config.server,port: 1433,database: config.database,
  approvalId: 'synthetic',approvedBy: 'synthetic-operator',approvedAt: new Date(Date.now()-1000).toISOString(),expiresAt: new Date(Date.now()+60000).toISOString(),
  fingerprintSha256: new X509Certificate(certs.selfSigned).fingerprint256,certificatePem: certs.selfSigned }); }
function sig(name: Procedure) { return reportedSignature(name,{ evidence: 'synthetic reported signature; NOT a corporate acceptance',
  nullable: Object.fromEntries(verifiedProcedureContracts[name].server.parameters.map(p => [p.name,p.direction === 'output'])) }); }
function endpoint() { return prepareCorporateAttemptsEndpoint(config,pin(),sig('ReadDataCurrentDay'),{ TableName: 'AttemptsRawData' }); }
function connectionConfig(): ConnectionConfiguration { return { server: config.server,authentication: { type: 'default',options: { userName: config.user,password: config.password } },
  options: { port: 1433,database: config.database,serverName: config.server,encrypt: true,trustServerCertificate: false,tdsVersion: '7_4',maxRetriesOnTransientErrors: 0,
    cryptoCredentialsDetails: { minVersion: 'TLSv1.2',maxVersion: 'TLSv1.2' } } }; }
it('prepara somente endpoint fixo/assinatura oficial e não promove autorização de diagnóstico', () => {
  expect(Object.isFrozen(endpoint())).toBe(true);
  expect(() => prepareCorporateAttemptsEndpoint(config,pin('OLOS_TDS_TLS_METADATA_ONLY'),sig('ReadDataCurrentDay'),{ TableName: 'AttemptsRawData' })).toThrow();
  for (const override of [{ server: '127.0.0.1' },{ port: 1434 },{ database: 'public' },{ user: 'sa' },{ trustServerCertificate: true }])
    expect(() => prepareCorporateAttemptsEndpoint({ ...config,...override },pin(),sig('ReadDataCurrentDay'),{ TableName: 'AttemptsRawData' })).toThrow();
  for (const proc of ['ResetReadData','DataAvailable','ReadData'] as const)
    expect(() => prepareCorporateAttemptsEndpoint(config,pin(),sig(proc),{ TableName: 'AttemptsRawData' })).toThrow();
  expect(() => prepareCorporateAttemptsEndpoint(config,pin(),sig('ReadDataCurrentDay'),{ TableName: 'Users' })).toThrow();
  const wrong = { ...sig('LogReadData'),parameters: sig('LogReadData').parameters.map(p => p.name === 'tablename' ? { ...p,type: { kind: 'varchar' as const,length: 128 } } : p) };
  expect(() => prepareCorporateAttemptsEndpoint(config,pin(),wrong,{ tablename: 'AttemptsRawData',DateExport: '2026-10-01' })).toThrow();
});
it('configuração alterada e capacidade clonada não liberam Connection; conectar/RPC bloqueados antes de rede', () => {
  const ep = endpoint(), c = connectionConfig();
  expect(matchesPreparedAttemptsConnection(c,ep)).toBe(true);
  expect(matchesPreparedAttemptsConnection({ ...c,server: 'another' },ep)).toBe(false);
  expect(matchesPreparedAttemptsConnection(c,{ ...ep })).toBe(false);
  for (const options of [{ encrypt: false },{ trustServerCertificate: true },{ port: 1434 },{ maxRetriesOnTransientErrors: 1 },{ cryptoCredentialsDetails: { minVersion: 'TLSv1.1' as const } }])
    expect(() => new ExactConnection({ ...c,options: { ...c.options,...options } },ep)).toThrow();
  const connect = vi.spyOn(Socket.prototype,'connect').mockImplementation(() => { throw new Error('UNEXPECTED_NETWORK'); });
  try {
    const conn = new ExactConnection(c,ep);
    expect(() => conn.connect()).toThrow('ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED');
    expect(() => conn.connectOnPort(1433,false,new AbortController().signal)).toThrow('ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED');
    expect(() => conn.sendLogin7Packet()).toThrow('ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED');
    expect(() => conn.callProcedure(new Request('dbo.usp_ExportData_ReReadData',() => {}))).toThrow('ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED');
    expect(connect).not.toHaveBeenCalled(); conn.close();
  } finally { connect.mockRestore(); }
});
it('coordenador rejeita capacidade preparada antes de startRun, spool ou PostgreSQL; origem nunca é chamada', async () => {
  const source = new IsolatedTdsSource(endpoint(),attemptsFixtureFields('0000000000'));
  const startRun = vi.fn(), active = vi.fn(), preflight = vi.fn();
  const coordinator = new LargeBatchCoordinator({ startRun } as never,{ active } as never,{ preflight } as never,spoolLimits());
  await expect(coordinator.receive(attemptsFixturePlan('2026-10-01','0000000000'),source,new AbortController().signal)).rejects.toThrow('ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED');
  expect(startRun).not.toHaveBeenCalled(); expect(active).not.toHaveBeenCalled(); expect(preflight).not.toHaveBeenCalled();
  await expect(source.call(sig('ReadDataCurrentDay'),{ TableName: 'AttemptsRawData' },new AbortController().signal).next()).rejects.toThrow('ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED');
  expect(source.metrics.calls).toBe(0);
});
it.each([false,true])('Connection real do Tedious instala o IO existente; certificado substituído=%s; sem LOGIN7/SQL',async substitute => {
  const server = await syntheticTdsTls({ cert: substitute ? certs.cert : certs.selfSigned,key: substitute ? certs.key : certs.selfKey });
  let loginReached = false, secured = false;
  class BeforeLogin extends ExactConnection {
    override sendLogin7Packet(): void { loginReached = true; queueMicrotask(() => this.close()); }
  }
  const c = new BeforeLogin({ server: '127.0.0.1',authentication: { type: 'default',options: { userName: 'synthetic',password: 'synthetic' } },
    options: { port: server.port,database: 'phase3d_fixture',serverName: 'localhost',encrypt: true,trustServerCertificate: false,connectTimeout: 1500,
      maxRetriesOnTransientErrors: 0,cryptoCredentialsDetails: { ca: certs.selfSigned,minVersion: 'TLSv1.2',maxVersion: 'TLSv1.2' } } },undefined,pin());
  const diagnostics=new InspectionDiagnostics();c.inspectionDiagnostics=diagnostics.observe;
  c.on('secure',tls => { secured = tls.authorized && tls.getProtocol() === 'TLSv1.2'; }); c.on('error',() => {});
  try {
    await new Promise<void>((resolve,reject) => {
      const timeout = setTimeout(() => { c.close(); reject(new Error('SYNTHETIC_TIMEOUT')); },3000);
      c.once('end',() => { clearTimeout(timeout); resolve(); }); c.connect(() => {});
    });
    expect(c.messageIo).toBeInstanceOf(PinnedMessageIO);
    expect(diagnostics.snapshot().observedStages).toContain('TLS_HANDSHAKE_STARTED');
    if(substitute)expect(diagnostics.snapshot()).toMatchObject({phase:'TLS_HANDSHAKE',rpcDispatchState:'NOT_OBSERVED'});
    else expect(diagnostics.snapshot().observedStages).toContain('TLS_VALIDATED');
    expect(loginReached).toBe(!substitute); expect(secured).toBe(!substitute);
    expect(server.sessions).toHaveLength(1); expect(server.sessions[0]?.applicationBytes).toBe(0);
    expect(server.sessions[0]?.types.every(t => t === 0x12)).toBe(true);
  } finally { c.close(); await server.stop(); }
});
const review = { operator: 'synthetic-reviewer',approvalReference: 'synthetic-review',ownershipEvidence: 'manual evidence required independently of log',completenessEvidence: 'synthetic count and endpoints only' };
function recoveryCase() {
  const plan = attemptsFixturePlan('2026-10-01','0000000000');
  const origin: SpoolManifest = { identity: { format: 'olos-spool/v1',batchId: randomUUID(),runId: randomUUID(),streamId: randomUUID(),fencingToken: '1',plan,
    expected: { version: '0',cursor: null } as never },state: 'recovery_required',sealed: false,rows: 1,bytes: 4,sha256: null };
  const interval: LogInterval = { table: 'AttemptsRawData',key: 'AttemptsRawDataID',date: '2026-10-01',start: '1',end: '3',quantity: 3,dateRead: 'sql-datetime/v1:0000000000000000' };
  return { origin,interval,selection: { date: interval.date,start: '1',end: '3' } };
}
it('reutiliza revisão de recuperação, vincula digest/faixa/data e não confunde plano corporativo com fixture executável', () => {
  const { origin,interval,selection } = recoveryCase(), before = structuredClone(origin), logDigest = 'a'.repeat(64);
  expect(planAttemptsRecovery(origin,logDigest,[interval],selection,review).scope).toBe('isolated-fixture');
  const prepared = prepareCorporateAttemptsReRead(config,pin(),sig('ReReadData'),origin,logDigest,[interval],selection,review);
  expect(prepared).toMatchObject({ executionAllowed: false,remoteOwnershipProvenByLog: false,recovery: { scope: 'corporate-prepared-blocked',caseId: origin.identity.batchId } });
  expect(prepared.call.values).toEqual({ TableName: 'AttemptsRawData',DateExport: interval.date,PKIDStart: '1',PKIDEnd: '3' });
  expect(() => assertCorporateAttemptsExecution(prepared.endpoint)).toThrow('ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED');
  expect(() => rereadLargePlan(origin,prepared.recovery)).toThrow('RECOVERY_CASE_MISMATCH');
  for (const intervals of [[],[interval,interval],[{ ...interval,key: 'CallId' } as never]])
    expect(() => planCorporateAttemptsRecovery(origin,logDigest,intervals,selection,review)).toThrow();
  expect(() => planCorporateAttemptsRecovery(origin,logDigest,[interval],selection,{ ...review,ownershipEvidence: '' })).toThrow();
  expect(origin).toEqual(before);
});
it('porta de recuperação existente audita tentativa bloqueada; não classifica sucesso nem altera checkpoint',async () => {
  const audit = { recordRecoveryAttempt: vi.fn(async () => {}) }, classify = vi.fn(() => 'remote_read_observed' as const);
  const port = prepareCorporateAttemptsRecoveryPort(config,pin(),audit,{ logSignature: sig('LogReadData'),rereadSignature: sig('ReReadData'),evidence: 'synthetic',
    operatorApprovalReference: 'synthetic',parameters: () => ({}),classifyLog: classify });
  const request = { caseId: randomUUID(),sourceTable: 'AttemptsRawData' as const,readType: 'current_day' as const,exportDate: '2026-10-01',pkRange: null };
  await expect(port.inspectLogReadData(request,new AbortController().signal)).rejects.toMatchObject({ code: 'EXECUTION_FAILED',outcome: 'unknown' });
  expect(classify).not.toHaveBeenCalled(); expect(audit.recordRecoveryAttempt).toHaveBeenCalledWith(request.caseId,'inspect_log','failed',null);
});
