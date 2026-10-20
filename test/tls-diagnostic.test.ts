import { beforeAll, afterAll, expect, it } from 'vitest';
import { X509Certificate } from 'node:crypto';
import { createServer } from 'node:net';
import { readFile } from 'node:fs/promises';
import { classifyTlsError, diagnoseTdsTls, encryptionFromPrelogin, publicCertificate, type TlsDiagnosticOptions } from '../src/diagnostics/tds-tls.js';
import { parseTlsArguments } from '../adapters/olos-tls-diagnostic.js';
import { syntheticCertificates, syntheticTdsTls } from './support/tls-diagnostic-fixture.js';

let certificates: Awaited<ReturnType<typeof syntheticCertificates>>;
beforeAll(async () => { certificates = await syntheticCertificates(); },30000);
afterAll(async () => { await certificates?.stop(); });

async function run(overrides: Partial<TlsDiagnosticOptions> = {}, serverOptions: Partial<Parameters<typeof syntheticTdsTls>[0]> = {}) {
  const server = await syntheticTdsTls({ cert: certificates.cert,key: certificates.key,...serverOptions });
  try {
    const result = await diagnoseTdsTls({ host: '127.0.0.1', port: server.port, serverName: 'localhost', mode: 'verify',
      ca: certificates.ca, timeoutMs: 2000, ...overrides });
    // Close has time to reach peer. No decrypted client application data, no LOGIN7/RPC/batch.
    await new Promise(resolve => setTimeout(resolve,20));
    expect(server.sessions).toHaveLength(1);
    expect(server.sessions[0]!.types.every(type => type === 0x12)).toBe(true);
    expect(server.sessions[0]!.applicationBytes).toBe(0);
    return result;
  } finally { await server.stop(); }
}
it('TLS validado após PRELOGIN fragmentado; encerra antes de LOGIN7 e SQL',async () => {
  const result = await run({}, { fragmented: true });
  expect(result).toMatchObject({ code: 'VERIFIED', verified: true, chain: 'valid', name: 'match' });
  expect(result.certificate).toEqual(publicCertificate(new X509Certificate(certificates.cert).raw));
});
it('extrai somente os seis metadados públicos autorizados, sem chave/certificado bruto', () => {
  const meta = publicCertificate(new X509Certificate(certificates.cert).raw);
  expect(Object.keys(meta).sort()).toEqual(['subject','issuer','subjectAltName','validFrom','validTo','fingerprintSha256'].sort());
  expect(meta.subject).toBe('CN=localhost');
  expect(meta.issuer).toBe('CN=diagnostic-synthetic-ca');
  expect(meta.subjectAltName).toContain('DNS:localhost');
  expect(meta.fingerprintSha256).toMatch(/^(?:[A-F0-9]{2}:){31}[A-F0-9]{2}$/);
  expect(JSON.stringify(meta)).not.toContain('PRIVATE KEY');
});
it('cadeia não confiável é recusada em verify, sem fallback nem segunda conexão', async () => {
  const result = await run({ ca: undefined } as unknown as Partial<TlsDiagnosticOptions>);
  expect(result).toMatchObject({ code: 'CERT_UNTRUSTED', verified: false });
});
it('nome incompatível é distinto de cadeia não confiável',async () => {
  expect(await run({ serverName: 'incorrect.example' })).toMatchObject({ code: 'CERT_NAME_MISMATCH', verified: false });
});
it('validade expirada é recusada',async () => {
  expect(await run({}, { cert: certificates.expired })).toMatchObject({ code: 'CERT_EXPIRED', verified: false });
});
it('inspeção não validada obtém metadados sem aprovar cadeia ou nome',async () => {
  const result = await run({ mode: 'inspect-unverified', serverName: 'incorrect.example', ca: undefined } as unknown as Partial<TlsDiagnosticOptions>);
  expect(result).toMatchObject({ code: 'INSPECTED_UNVERIFIED', verified: false, chain: 'unverified', name: 'mismatch' });
  expect(result.certificate?.subject).toBe('CN=localhost');
});
it('inspeção com cadeia válida continua informativa e nunca retorna VERIFIED',async () => {
  expect(await run({ mode: 'inspect-unverified' })).toMatchObject({ code: 'INSPECTED_UNVERIFIED', verified: false });
});
it('incompatibilidade TLS não ativa versões/cifras legadas',async () => {
  expect(await run({}, { minVersion: 'TLSv1.3' })).toMatchObject({ code: 'TLS_INCOMPATIBLE', verified: false });
});
it('recusa servidor sem criptografia e nunca envia autenticação em claro',async () => {
  expect(await run({}, { response: Buffer.from([1,0,6,0,1,255,2]) })).toMatchObject({ code: 'TLS_NOT_SUPPORTED' });
});
it('timeout absoluto, sem retry automático',async () => {
  expect(await run({ timeoutMs: 100 },{ silent: true })).toMatchObject({ code: 'TIMEOUT' });
});
it('timeout durante handshake TLS fecha a sessão, sem fallback',async () => {
  expect(await run({ timeoutMs: 100 },{ stallTls: true })).toMatchObject({ code: 'TIMEOUT' });
});
it('interrupção de rede durante TLS não é confundida com certificado inválido',async () => {
  expect(await run({}, { disconnectTls: true })).toMatchObject({ code: 'NETWORK_FAILURE' });
});
it('fechamento prematuro é falha de rede, sem retry',async () => {
  expect(await run({}, { disconnect: true })).toMatchObject({ code: 'NETWORK_FAILURE' });
});
it('limita bytes totais antes de decodificar respostas ilimitadas',async () => {
  expect(await run({}, { flood: true })).toMatchObject({ code: 'LIMIT_EXCEEDED' });
});
it('resposta de tipo inesperado é rejeitada antes do TLS',async () => {
  expect(await run({}, { packetType: 0x10 })).toMatchObject({ code: 'INVALID_TDS' });
});
it('PRELOGIN malformado é rejeitado',async () => {
  expect(await run({}, { response: Buffer.from([1,0,0,0,1,255,1]) })).toMatchObject({ code: 'INVALID_TDS' });
});
it('porta fechada é falha de rede',async () => {
  const server = createServer(); await new Promise<void>(resolve => server.listen(0,'127.0.0.1',resolve));
  const address = server.address(); if (!address || typeof address === 'string') throw new Error('fixture');
  await new Promise<void>(resolve => server.close(() => resolve()));
  expect(await diagnoseTdsTls({ host: '127.0.0.1',port: address.port,serverName: 'localhost',mode: 'verify' })).toMatchObject({ code: 'NETWORK_FAILURE' });
});
it.each(['ECONNREFUSED','EHOSTUNREACH','ENETUNREACH','ENOTFOUND','ECONNRESET'])('classifica rede: %s', code => {
  expect(classifyTlsError({ code })).toBe('NETWORK_FAILURE');
});
it.each([Buffer.alloc(0),Buffer.from([1]),Buffer.from([255]),Buffer.from([1,255,255,0,1,255,1])])('valida tabela de offsets PRELOGIN',data => {
  expect(() => encryptionFromPrelogin(data)).toThrow();
});
const args = ['--authorize','TDS_TLS_METADATA_ONLY','--server-name','olos.example'];
it('CLI fixa endpoint, modo estrito e não aceita credenciais/SQL/host alternativo', () => {
  expect(parseTlsArguments(args,{ SYNC_ENABLED: 'false' }).options).toMatchObject({ host: '10.196.234.11',port: 1433,mode: 'verify' });
  for (const flag of ['--password','--user','--query','--host','--procedure','--trust-server-certificate'])
    expect(() => parseTlsArguments([...args,flag,'something'],{ SYNC_ENABLED: 'false' })).toThrow();
});
it.each([{}, { SYNC_ENABLED: 'true' }, { SYNC_ENABLED: 'false',NODE_TLS_REJECT_UNAUTHORIZED: '0' }])('CLI exige sync=false e validação global ativa',env => {
  expect(() => parseTlsArguments(args,env)).toThrow();
});
it('CLI exige autorização explícita e rejeita flags duplicadas/soltas/modo desconhecido', () => {
  for (const bad of [[],args.slice(2),[...args,'--mode','unsafe'],[...args,'--mode'],[...args,'--server-name','another']])
    expect(() => parseTlsArguments(bad,{ SYNC_ENABLED: 'false' })).toThrow();
});
it('rejeita campos extras e CA com chave privada sem abrir conexão', async () => {
  const options = { host: '127.0.0.1',port: 1,serverName: 'localhost',mode: 'verify' as const };
  expect(await diagnoseTdsTls({ ...options, ca: certificates.key })).toMatchObject({ code: 'INVALID_CONFIGURATION' });
  expect(await diagnoseTdsTls({ ...options, ca: '-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----' })).toMatchObject({ code: 'INVALID_CONFIGURATION' });
  expect(await diagnoseTdsTls({ ...options,password: 'never-send' } as TlsDiagnosticOptions)).toMatchObject({ code: 'INVALID_CONFIGURATION' });
});
it('isolamento arquitetural: diagnóstico não cria Connection/Request nem importa gates/PG/runtime',async () => {
  const source = await readFile(new URL('../src/diagnostics/tds-tls.ts',import.meta.url),'utf8');
  const adapter = await readFile(new URL('../adapters/olos-tls-diagnostic.ts',import.meta.url),'utf8');
  expect(source).not.toMatch(/new (?:Connection|Request)\b|\.callProcedure\(|sendLogin7Packet|\.execSql\(/);
  expect(source+adapter).not.toMatch(/from ['"].*(?:disposition|postgres|persistence|main\.js|app\.js)/);
});
