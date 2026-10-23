import { beforeAll, afterAll, expect, it, vi } from 'vitest';
import { X509Certificate } from 'node:crypto';
import { Socket } from 'node:net';
import { createRequire } from 'node:module';
import { mkdtemp, writeFile, chmod, symlink, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { parseOlosPinApproval, loadOlosPinApproval, verifyPinnedPeer, type OlosPinApproval } from '../src/security/olos-certificate-pin.js';
import { PinnedMessageIO } from '../src/diagnostics/pinned-message-io.js';
import { diagnoseTdsTls } from '../src/diagnostics/tds-tls.js';
import { parseTlsArguments } from '../adapters/olos-tls-diagnostic.js';
import { syntheticCertificates, syntheticTdsTls } from './support/tls-diagnostic-fixture.js';
const require = createRequire(import.meta.url);
const Debug = require('tedious/lib/debug.js') as typeof import('tedious/lib/debug.js').default;
let certificates: Awaited<ReturnType<typeof syntheticCertificates>>;
beforeAll(async () => { certificates = await syntheticCertificates(); },30000);
afterAll(async () => { await certificates?.stop(); });
function input(overrides: Record<string,unknown> = {}) {
  return { version: 1, purpose: 'OLOS_TDS_TLS_METADATA_ONLY',host: '10.196.234.11',port: 1433,database: 'ExportDataV3',
    approvalId: 'synthetic-approval', approvedBy: 'synthetic-operator',approvedAt: new Date(Date.now()-1000).toISOString(),
    expiresAt: new Date(Date.now()+3600000).toISOString(),certificatePem: certificates.selfSigned,
    fingerprintSha256: new X509Certificate(certificates.selfSigned).fingerprint256,...overrides };
}
async function handshake(approval: OlosPinApproval, useDifferentLeaf = false, minVersion = 'TLSv1.2',serverLeaf=certificates.selfSigned) {
  const peer = await syntheticTdsTls({ cert: useDifferentLeaf ? certificates.cert : serverLeaf,
    key: useDifferentLeaf ? certificates.key : certificates.selfKey });
  const socket = new Socket();
  const io = new PinnedMessageIO(socket,4096,new Debug(),approval);
  const incoming = (io as unknown as { incomingMessageStream: { destroy(): void } }).incomingMessageStream;
  let timeout: ReturnType<typeof setTimeout> | undefined;
  const failure = new Promise<never>((_,reject) => {
    socket.on('error',reject);
    timeout = setTimeout(() => reject(new Error('FIXTURE_TIMEOUT')),2000);
  });
  try {
    await Promise.race([new Promise<void>(resolve => socket.connect(peer.port,'127.0.0.1',resolve)),failure]);
    io.sendMessage(0x12,Buffer.from([1,0,6,0,1,255,1]));
    const response = await Promise.race([io.readMessage(),failure]);
    for await (const _chunk of response) { /* PRELOGIN only */ }
    await Promise.race([io.startTls({ minVersion: minVersion as 'TLSv1.2',maxVersion: 'TLSv1.2' },'ignored-dns',false),failure]);
    expect(io.securePair?.cleartext.authorized).toBe(true);
    expect(io.securePair?.cleartext.encrypted).toBe(true);
    expect(io.securePair?.cleartext.getProtocol()).toBe('TLSv1.2');
  } finally {
    clearTimeout(timeout);
    io.securePair?.cleartext.destroy(); io.securePair?.encrypted.destroy();
    socket.destroy(); incoming.destroy(); io.outgoingMessageStream.destroy();
    await peer.stop();
    expect(peer.sessions).toHaveLength(1);
    expect(peer.sessions[0]?.types.every(type => type === 0x12)).toBe(true);
    expect(peer.sessions[0]?.applicationBytes).toBe(0);
  }
}
it('pin aprovado autentica TLS 1.2 com certificado autoassinado sem SAN; zero LOGIN7/SQL',async () => {
  expect(new X509Certificate(certificates.selfSigned).subjectAltName).toBeUndefined();
  expect(new X509Certificate(certificates.selfSigned).ca).toBe(false);
  await handshake(parseOlosPinApproval(input()));
});
it('autentica folha autoassinada CA:FALSE com keyUsage TLS sem keyCertSign',async()=>{
  const cert=new X509Certificate(certificates.serverLeaf);
  expect(cert.ca).toBe(false);expect(cert.subjectAltName).toBeUndefined();expect(cert.verify(cert.publicKey)).toBe(true);
  const approval=parseOlosPinApproval(input({certificatePem:certificates.serverLeaf,fingerprintSha256:cert.fingerprint256}));
  await handshake(approval,false,'TLSv1.2',certificates.serverLeaf);
});
it('autentica a folha exata aprovada mesmo quando o AKI impede construir sua cadeia',async()=>{
  const cert=new X509Certificate(certificates.directTrustLeaf);
  expect(cert.ca).toBe(false);expect(cert.subject).toBe(cert.issuer);expect(cert.verify(cert.publicKey)).toBe(true);
  await handshake(parseOlosPinApproval(input({certificatePem:certificates.directTrustLeaf,fingerprintSha256:cert.fingerprint256})),false,'TLSv1.2',certificates.directTrustLeaf);
});
it('recusa outra folha autoassinada mesmo com a mesma chave pública aprovada',async()=>{
  const approved=new X509Certificate(certificates.serverLeaf),presented=new X509Certificate(certificates.directTrustLeaf);
  expect(approved.publicKey.equals(presented.publicKey)).toBe(true);expect(approved.fingerprint256).not.toBe(presented.fingerprint256);
  await expect(handshake(parseOlosPinApproval(input({certificatePem:certificates.serverLeaf,fingerprintSha256:approved.fingerprint256})),false,'TLSv1.2',certificates.directTrustLeaf)).rejects.toThrow();
});
it('recusa certificado sem aprovação validada antes de criar o transporte TLS',async()=>{
  await expect(handshake(input({certificatePem:certificates.serverLeaf,fingerprintSha256:new X509Certificate(certificates.serverLeaf).fingerprint256}) as OlosPinApproval,false,'TLSv1.2',certificates.serverLeaf)).rejects.toMatchObject({code:'PIN_APPROVAL_REQUIRED'});
});
it('recusa folha autoassinada expirada mesmo com fingerprint correspondente',()=>{
  const cert=new X509Certificate(certificates.expiredServerLeaf);
  expect(cert.verify(cert.publicKey)).toBe(true);
  expect(()=>parseOlosPinApproval(input({certificatePem:certificates.expiredServerLeaf,fingerprintSha256:cert.fingerprint256}))).toThrow('CERT_HAS_EXPIRED');
});
it('revalida a validade do certificado ao iniciar TLS, mesmo com aprovação ainda vigente',async()=>{
  const cert=new X509Certificate(certificates.serverLeaf),end=Date.parse(cert.validTo);
  const approval=parseOlosPinApproval(input({certificatePem:certificates.serverLeaf,fingerprintSha256:cert.fingerprint256,expiresAt:new Date(end+86400000).toISOString()}));
  const clock=vi.spyOn(Date,'now').mockReturnValue(end+1);
  try{await expect(handshake(approval,false,'TLSv1.2',certificates.serverLeaf)).rejects.toMatchObject({code:'CERT_HAS_EXPIRED'});}
  finally{clock.mockRestore();}
});
it('recusa autoassinatura inválida mesmo quando o fingerprint aprova os bytes adulterados',()=>{
  const raw=Buffer.from(new X509Certificate(certificates.serverLeaf).raw);raw[raw.length-1]=raw[raw.length-1]!^1;
  const cert=new X509Certificate(raw);expect(cert.verify(cert.publicKey)).toBe(false);
  expect(()=>parseOlosPinApproval(input({certificatePem:cert.toString(),fingerprintSha256:cert.fingerprint256}))).toThrow('PIN_APPROVAL_INVALID');
});
it('outra folha na mesma cadeia é recusada pelo pin, apesar de cadeia confiável',async () => {
  await expect(handshake(parseOlosPinApproval(input({ certificatePem: certificates.ca,fingerprintSha256: new X509Certificate(certificates.ca).fingerprint256 })),true)).rejects.toMatchObject({ code: 'PIN_MISMATCH' });
});
it('TLS inferior a 1.2 é recusado antes de negociar',async () => {
  await expect(handshake(parseOlosPinApproval(input()),false,'TLSv1.1')).rejects.toMatchObject({ code: 'PIN_CONFIGURATION_INVALID' });
});
it('fingerprint observado sem certificado aprovado não constitui autorização', () => {
  expect(() => parseOlosPinApproval({ fingerprintSha256: input().fingerprintSha256 })).toThrow('PIN_APPROVAL_INVALID');
  expect(() => parseOlosPinApproval(input({ approvedBy: '' }))).toThrow();
  expect(() => parseOlosPinApproval(input({ fingerprintSha256: '00:'.repeat(31)+'00' }))).toThrow('PIN_MISMATCH');
});
it.each([{ host: '127.0.0.1' },{ port: 1434 },{ database: 'another' },{ purpose: 'SQL' },{ trustServerCertificate: true }])('restringe aprovação ao endpoint e propósito oficiais: %j',override => {
  expect(() => parseOlosPinApproval(input(override))).toThrow('PIN_APPROVAL_INVALID');
});
it('aprovação expirada, futura ou superior a sete dias é recusada', () => {
  for (const override of [{ expiresAt: new Date(Date.now()-100).toISOString() },
    { approvedAt: new Date(Date.now()+10000).toISOString() },
    { expiresAt: new Date(Date.now()+8*86400000).toISOString() }]) expect(() => parseOlosPinApproval(input(override))).toThrow();
});
it('PEM inválido, chave privada e certificado não autoassinado são recusados', () => {
  for (const pem of [certificates.key,certificates.ca+certificates.key,'invalid'])
    expect(() => parseOlosPinApproval(input({ certificatePem: pem }))).toThrow();
  expect(() => parseOlosPinApproval(input({ certificatePem: certificates.cert,fingerprintSha256: new X509Certificate(certificates.cert).fingerprint256 }))).toThrow('PIN_APPROVAL_INVALID');
});
it('aprovação forjada e certificado substituído são recusados em comparação de DER SHA-256', () => {
  const peer = { raw: new X509Certificate(certificates.ca).raw };
  expect(verifyPinnedPeer(input() as OlosPinApproval,peer as never)).toMatchObject({ code: 'PIN_APPROVAL_REQUIRED' });
  expect(verifyPinnedPeer(parseOlosPinApproval(input()),{ raw: new X509Certificate(certificates.cert).raw } as never)).toMatchObject({ code: 'PIN_MISMATCH' });
});
it('arquivo privado exige proprietário, 0600, conteúdo estrito e ausência de symlink', async () => {
  const dir = await mkdtemp(join(tmpdir(),'olos-pin-approval-'));
  const file = join(dir,'approval.json');
  try {
    await writeFile(file,JSON.stringify(input()),{ mode: 0o600 });
    expect(await loadOlosPinApproval(file)).toMatchObject({ approvalId: 'synthetic-approval' });
    await chmod(file,0o644); await expect(loadOlosPinApproval(file)).rejects.toThrow();
    await chmod(file,0o600); await symlink(file,join(dir,'link'));
    await expect(loadOlosPinApproval(join(dir,'link'))).rejects.toThrow();
  } finally { await rm(dir,{ recursive: true,force: true }); }
});
it('CLI exige modo e aprovação explícitos, preserva verify padrão e recusa CA simultânea', () => {
  const base = ['--authorize','TDS_TLS_METADATA_ONLY','--server-name','olos.example'];
  expect(parseTlsArguments(base,{ SYNC_ENABLED: 'false' }).options.mode).toBe('verify');
  expect(() => parseTlsArguments([...base,'--mode','verify-pinned'],{ SYNC_ENABLED: 'false' })).toThrow();
  const args = [...base,'--mode','verify-pinned','--pin-approval-file','/private/approval.json'];
  expect(parseTlsArguments(args,{ SYNC_ENABLED: 'false' }).pinFile).toBe('/private/approval.json');
  expect(() => parseTlsArguments([...args,'--ca-file','/ca'],{ SYNC_ENABLED: 'false' })).toThrow();
});
it('diagnóstico recusa pin ausente ou endpoint alternativo sem abrir conexão',async () => {
  const options = { host: '127.0.0.1',port: 1,serverName: 'localhost',mode: 'verify-pinned' as const };
  expect(await diagnoseTdsTls(options)).toMatchObject({ code: 'INVALID_CONFIGURATION', verified: false });
  expect(await diagnoseTdsTls({ ...options,pinApproval: parseOlosPinApproval(input()) })).toMatchObject({ code: 'INVALID_CONFIGURATION' });
});

it('recusa bypass de confiança, cifras customizadas e relaxamento global antes de abrir TLS',async () => {
  const socket = new Socket();
  const io = new PinnedMessageIO(socket,4096,new Debug(),parseOlosPinApproval(input()));
  const details = { minVersion: 'TLSv1.2' as const,maxVersion: 'TLSv1.2' as const };
  try {
    await expect(io.startTls(details,'ignored',true)).rejects.toMatchObject({ code: 'PIN_CONFIGURATION_INVALID' });
    await expect(io.startTls({ ...details,ciphers: 'DEFAULT' },'ignored',false)).rejects.toThrow();
    vi.stubEnv('NODE_TLS_REJECT_UNAUTHORIZED','0');
    await expect(io.startTls(details,'ignored',false)).rejects.toMatchObject({ code: 'PIN_CONFIGURATION_INVALID' });
  } finally {
    vi.unstubAllEnvs(); socket.destroy(); io.outgoingMessageStream.destroy();
    (io as unknown as { incomingMessageStream: { destroy(): void } }).incomingMessageStream.destroy();
  }
});
