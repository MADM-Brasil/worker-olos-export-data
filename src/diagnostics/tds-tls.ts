import { createRequire } from 'node:module';
import { Socket, isIP } from 'node:net';
import { X509Certificate } from 'node:crypto';
import { checkServerIdentity, type TLSSocket } from 'node:tls';
import type { Transform } from 'node:stream';

import { PinnedMessageIO } from './pinned-message-io.js';
import { assertOlosPinApproval, type OlosPinApproval } from '../security/olos-certificate-pin.js';

const require = createRequire(import.meta.url);
// Internal, read-only APIs: pinned and exercised against an isolated SQL Server.
// No Connection/Request object: this transport has no LOGIN7 or query path.
const MessageIO = require('tedious/lib/message-io.js') as typeof import('tedious/lib/message-io.js').default;
const PreloginPayload = require('tedious/lib/prelogin-payload.js') as typeof import('tedious/lib/prelogin-payload.js').default;
const Debug = require('tedious/lib/debug.js') as typeof import('tedious/lib/debug.js').default;

export type DiagnosticCode = 'VERIFIED' | 'PIN_VERIFIED' | 'PIN_REJECTED' | 'INSPECTED_UNVERIFIED' | 'CERT_UNTRUSTED' | 'CERT_NAME_MISMATCH' |
  'CERT_EXPIRED' | 'CERT_INVALID' | 'TLS_INCOMPATIBLE' | 'TLS_NOT_SUPPORTED' | 'NETWORK_FAILURE' |
  'TIMEOUT' | 'INVALID_TDS' | 'LIMIT_EXCEEDED' | 'UNSUPPORTED_DRIVER' | 'INVALID_CONFIGURATION' | 'CERT_EXPORT_FAILED';
export interface CertificateMetadata {
  subject: string;
  issuer: string;
  subjectAltName: string | null;
  validFrom: string;
  validTo: string;
  fingerprintSha256: string;
}
export interface TlsDiagnosticOptions {
  host: string;
  port: number;
  serverName: string;
  mode: 'verify' | 'inspect-unverified' | 'verify-pinned';
  pinApproval?: OlosPinApproval;
  ca?: string;
  timeoutMs?: number;
}
export interface TlsDiagnosticResult {
  code: DiagnosticCode;
  verified: boolean;
  identity?: 'certificate-sha256-pin';
  certificate?: CertificateMetadata;
  /** Inspection is never an approval, even when the chain happens to verify. */
  chain: 'valid' | 'unverified' | 'not-assessed';
  name: 'match' | 'mismatch' | 'not-assessed';
}
class DiagnosticError extends Error {
  constructor(readonly code: DiagnosticCode) { super(code); }
}
export function classifyTlsError(error: unknown): DiagnosticCode {
  if (error instanceof DiagnosticError) return error.code;
  const code = error && typeof error === 'object' && 'code' in error ? String(error.code) : '';
  if (code.startsWith('PIN_')) return 'PIN_REJECTED';
  if (code === 'ERR_TLS_CERT_ALTNAME_INVALID') return 'CERT_NAME_MISMATCH';
  if (code === 'CERT_HAS_EXPIRED' || code === 'CERT_NOT_YET_VALID') return 'CERT_EXPIRED';
  if (['DEPTH_ZERO_SELF_SIGNED_CERT','SELF_SIGNED_CERT_IN_CHAIN','UNABLE_TO_VERIFY_LEAF_SIGNATURE',
    'UNABLE_TO_GET_ISSUER_CERT','UNABLE_TO_GET_ISSUER_CERT_LOCALLY','CERT_UNTRUSTED'].includes(code)) return 'CERT_UNTRUSTED';
  if (code.startsWith('ERR_SSL_') || code.startsWith('ERR_TLS_')) return 'TLS_INCOMPATIBLE';
  if (code.startsWith('CERT_') || code === 'INVALID_CA') return 'CERT_INVALID';
  if (['ECONNREFUSED','ECONNRESET','EHOSTUNREACH','ENETUNREACH','ENOTFOUND','EAI_AGAIN','EPIPE'].includes(code)) return 'NETWORK_FAILURE';
  if (code === 'ETIMEDOUT') return 'TIMEOUT';
  return 'INVALID_TDS';
}
export function publicCertificate(raw: Buffer): CertificateMetadata {
  const cert = new X509Certificate(raw);
  return { subject: cert.subject, issuer: cert.issuer, subjectAltName: cert.subjectAltName ?? null,
    validFrom: cert.validFrom, validTo: cert.validTo, fingerprintSha256: cert.fingerprint256 };
}
/** Validate offsets before the driver's permissive PRELOGIN decoder. */
export function encryptionFromPrelogin(data: Buffer): number {
  const entries: { token: number; offset: number; size: number }[] = [];
  let p = 0;
  while (p < data.length && data[p] !== 0xff) {
    if (p + 5 > data.length || entries.length >= 32) throw new DiagnosticError('INVALID_TDS');
    entries.push({ token: data[p]!, offset: data.readUInt16BE(p + 1), size: data.readUInt16BE(p + 3) });
    p += 5;
  }
  if (data[p] !== 0xff || new Set(entries.map(e => e.token)).size !== entries.length) throw new DiagnosticError('INVALID_TDS');
  for (const e of entries) if (e.offset <= p || e.offset + e.size > data.length) throw new DiagnosticError('INVALID_TDS');
  const e = entries.find(e => e.token === 1);
  if (!e || e.size !== 1 || data[e.offset]! > 3) throw new DiagnosticError('INVALID_TDS');
  return data[e.offset]!;
}
function validate(options: TlsDiagnosticOptions): void {
  if (require('tedious/package.json').version !== '18.6.2') throw new DiagnosticError('UNSUPPORTED_DRIVER');
  if (Object.keys(options).some(k => !['host','port','serverName','mode','ca','timeoutMs','pinApproval'].includes(k)) ||
    !isIP(options.host) || !Number.isInteger(options.port) || options.port < 1 || options.port > 65535 ||
    typeof options.serverName !== 'string' || !/^[a-zA-Z0-9][a-zA-Z0-9.:-]{0,252}$/.test(options.serverName) ||
    !['verify','inspect-unverified','verify-pinned'].includes(options.mode) ||
    options.timeoutMs !== undefined && (!Number.isInteger(options.timeoutMs) || options.timeoutMs < 50 || options.timeoutMs > 30000) ||
    options.ca !== undefined && (options.ca.length > 131072 || !/^(?:\s*-----BEGIN CERTIFICATE-----[A-Za-z0-9+/=\s]+-----END CERTIFICATE-----\s*)+$/.test(options.ca)) ||
    process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new DiagnosticError('INVALID_CONFIGURATION');
  if (options.mode === 'verify-pinned') {
    if (!options.pinApproval || options.pinApproval.purpose !== 'OLOS_TDS_TLS_METADATA_ONLY' || options.host !== '10.196.234.11' || options.port !== 1433 || options.ca !== undefined) throw new DiagnosticError('INVALID_CONFIGURATION');
    assertOlosPinApproval(options.pinApproval);
  } else if (options.pinApproval !== undefined) throw new DiagnosticError('INVALID_CONFIGURATION');
  if (options.ca) {
    try {
      for (const pem of options.ca.match(/-----BEGIN CERTIFICATE-----[A-Za-z0-9+/=\s]+-----END CERTIFICATE-----/g) ?? []) new X509Certificate(pem);
    } catch { throw new DiagnosticError('INVALID_CONFIGURATION'); }
  }
}
/** One TCP attempt, PRELOGIN + TLS only. No credentials, database, SQL or retry API. */
export async function diagnoseTdsTls(options: TlsDiagnosticOptions, exportCertificate?: (pem: string) => Promise<void>): Promise<TlsDiagnosticResult> {
  const empty = { verified: false, chain: 'not-assessed', name: 'not-assessed' } as const;
  try { validate(options); } catch (error) { return { ...empty, code: classifyTlsError(error) }; }
  const socket = new Socket();
  const io = options.mode === 'verify-pinned'
    ? new PinnedMessageIO(socket,4096,new Debug(),options.pinApproval!)
    : new MessageIO(socket, 4096, new Debug());
  // This private stream belongs solely to this instance; never mutate driver prototypes.
  const incoming = (io as unknown as { incomingMessageStream: Transform }).incomingMessageStream;
  let phase: 'prelogin' | 'tls' = 'prelogin';
  let timer: ReturnType<typeof setTimeout> | undefined;
  let received = 0;
  let certificatePem: string | undefined;
  let result: TlsDiagnosticResult;
  let abort!: (error: unknown) => void;
  const stopped = new Promise<never>((_, reject) => { abort = reject; });
  const fail = (error: unknown) => { abort(error); socket.destroy(); };
  socket.on('error', fail);
  socket.on('end', () => fail(new DiagnosticError('NETWORK_FAILURE')));
  socket.on('close', () => abort(new DiagnosticError('NETWORK_FAILURE')));
  incoming.on('error', () => fail(new DiagnosticError('INVALID_TDS')));
  io.outgoingMessageStream.on('error', fail);
  // Absolute byte/deadline budgets, including fragmented or unfinished messages.
  socket.prependListener('data', (data: Buffer) => {
    received += data.length;
    if (received > 256 * 1024) fail(new DiagnosticError('LIMIT_EXCEEDED'));
  });
  timer = setTimeout(() => fail(new DiagnosticError('TIMEOUT')), options.timeoutMs ?? 10000);
  try {
    const work = async (): Promise<TlsDiagnosticResult> => {
      await new Promise<void>(resolve => { socket.once('connect', resolve); socket.connect(options.port, options.host); });
      io.sendMessage(0x12, new PreloginPayload({ encrypt: true, version: { major: 18, minor: 6, build: 2, subbuild: 0 } }).data);
      const response = await io.readMessage();
      if (response.type !== 0x04) throw new DiagnosticError('INVALID_TDS');
      const chunks: Buffer[] = [];
      let size = 0;
      for await (const chunk of response) {
        size += chunk.length;
        if (size > 16384) throw new DiagnosticError('LIMIT_EXCEEDED');
        chunks.push(chunk);
      }
      if (![1,3].includes(encryptionFromPrelogin(Buffer.concat(chunks)))) throw new DiagnosticError('TLS_NOT_SUPPORTED');
      phase = 'tls';
      // TDS 7.x with this pinned driver supports TLS 1.2. No legacy fallback/cipher override.
      await io.startTls({ minVersion: 'TLSv1.2', maxVersion: 'TLSv1.2', ...(options.ca ? { ca: options.ca } : {}) },
        options.serverName, options.mode === 'inspect-unverified');
      const tls: TLSSocket | undefined = io.securePair?.cleartext;
      const cert = tls?.getPeerCertificate();
      if (!tls || !cert?.raw) throw new DiagnosticError('CERT_INVALID');
      if (exportCertificate) certificatePem = new X509Certificate(cert.raw).toString();
      if (options.mode === 'verify-pinned') return { code: 'PIN_VERIFIED', verified: true, identity: 'certificate-sha256-pin',
        certificate: publicCertificate(cert.raw), chain: 'valid', name: checkServerIdentity(options.serverName,cert) ? 'mismatch' : 'match' };
      const matches = checkServerIdentity(options.serverName, cert) === undefined;
      if (options.mode === 'verify' && (!tls.authorized || !matches)) throw new DiagnosticError(matches ? 'CERT_UNTRUSTED' : 'CERT_NAME_MISMATCH');
      return { code: options.mode === 'verify' ? 'VERIFIED' : 'INSPECTED_UNVERIFIED', verified: options.mode === 'verify',
        certificate: publicCertificate(cert.raw), chain: tls.authorized ? 'valid' : 'unverified', name: matches ? 'match' : 'mismatch' };
    };
    result = await Promise.race([work(), stopped]);
  } catch (error) {
    // OpenSSL errors have stable codes; do not expose messages/stacks, certificate dumps or wire buffers.
    const code = classifyTlsError(error);
    result = { ...empty, code: phase === 'prelogin' && code === 'TLS_INCOMPATIBLE' ? 'INVALID_TDS' : code };
  } finally {
    clearTimeout(timer);
    socket.unpipe();
    io.outgoingMessageStream.unpipe();
    io.securePair?.cleartext.destroy();
    io.securePair?.encrypted.destroy();
    incoming.destroy();
    io.outgoingMessageStream.destroy();
    socket.destroy();
  }
  // Export only after the diagnostic transport is closed. PEM never enters the JSON result.
  if (exportCertificate && certificatePem && result.certificate) {
    try { await exportCertificate(certificatePem); }
    catch { return { ...result, code: 'CERT_EXPORT_FAILED' }; }
  }
  return result;
}
