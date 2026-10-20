import { X509Certificate, createHash, timingSafeEqual } from 'node:crypto';
import { constants } from 'node:fs';
import { open, lstat, realpath } from 'node:fs/promises';
import { resolve, dirname, isAbsolute } from 'node:path';
import { z } from 'zod';
import type { PeerCertificate } from 'node:tls';

const schema = z.object({
  version: z.literal(1), purpose: z.enum(['OLOS_TDS_TLS_METADATA_ONLY','OLOS_ATTEMPTS_TDS_ACQUISITION']),
  host: z.literal('10.196.234.11'), port: z.literal(1433), database: z.literal('ExportDataV3'),
  approvalId: z.string().min(1).max(128), approvedBy: z.string().min(1).max(128),
  approvedAt: z.string().datetime(), expiresAt: z.string().datetime(),
  fingerprintSha256: z.string().regex(/^(?:[a-fA-F0-9]{2}:){31}[a-fA-F0-9]{2}$/),
  certificatePem: z.string().max(32768).regex(/^-----BEGIN CERTIFICATE-----\r?\n[A-Za-z0-9+/=\r\n]+-----END CERTIFICATE-----\s*$/),
}).strict();
export type OlosPinApproval = Readonly<z.infer<typeof schema>>;
const approvals = new WeakSet<object>();
export function pinError(code: string): Error & { code: string } { return Object.assign(new Error(code), { code }); }
function digest(raw: Buffer): Buffer { return createHash('sha256').update(raw).digest(); }
function expected(pin: string): Buffer { return Buffer.from(pin.replaceAll(':',''), 'hex'); }
export function assertOlosPinApproval(approval: OlosPinApproval): void {
  if (!approvals.has(approval)) throw pinError('PIN_APPROVAL_REQUIRED');
  const now = Date.now();
  if (Date.parse(approval.approvedAt) > now || Date.parse(approval.expiresAt) <= now) throw pinError('PIN_APPROVAL_EXPIRED');
}
/** Explicit approval only. Never accepts an observed fingerprint as approval. */
export function parseOlosPinApproval(input: unknown): OlosPinApproval {
  const result = schema.safeParse(input);
  if (!result.success) throw pinError('PIN_APPROVAL_INVALID');
  const approval = Object.freeze(result.data);
  const start = Date.parse(approval.approvedAt), end = Date.parse(approval.expiresAt);
  if (end <= start || end - start > 7 * 86400000) throw pinError('PIN_APPROVAL_INVALID');
  let cert: X509Certificate;
  try { cert = new X509Certificate(approval.certificatePem); } catch { throw pinError('PIN_APPROVAL_INVALID'); }
  if (!timingSafeEqual(digest(cert.raw), expected(approval.fingerprintSha256))) throw pinError('PIN_MISMATCH');
  // This mode deliberately supports only an explicitly approved self-signed leaf.
  if (cert.subject !== cert.issuer || !cert.verify(cert.publicKey)) throw pinError('PIN_APPROVAL_INVALID');
  if (Date.parse(cert.validFrom) > Date.now() || Date.parse(cert.validTo) <= Date.now()) throw pinError('CERT_HAS_EXPIRED');
  approvals.add(approval);
  assertOlosPinApproval(approval);
  return approval;
}
export function verifyPinnedPeer(approval: OlosPinApproval, cert: PeerCertificate): Error | undefined {
  try {
    assertOlosPinApproval(approval);
    if (!cert.raw || !timingSafeEqual(digest(cert.raw), expected(approval.fingerprintSha256))) throw pinError('PIN_MISMATCH');
    const peer = new X509Certificate(cert.raw);
    if (Date.parse(peer.validFrom) > Date.now() || Date.parse(peer.validTo) <= Date.now()) throw pinError('CERT_HAS_EXPIRED');
    return undefined;
  } catch (error) { return error instanceof Error ? error : pinError('PIN_MISMATCH'); }
}
/** No symlinks, owner-only file, bounded read; no credential or key material accepted. */
export async function loadOlosPinApproval(path: string): Promise<OlosPinApproval> {
  if (!isAbsolute(path) || await realpath(path) !== resolve(path)) throw pinError('PIN_APPROVAL_INVALID');
  const parent = await lstat(dirname(path));
  if (parent.uid !== process.getuid?.() || (parent.mode & 0o022) !== 0) throw pinError('PIN_APPROVAL_INVALID');
  const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const stat = await file.stat();
    if (!stat.isFile() || stat.uid !== process.getuid?.() || (stat.mode & 0o777) !== 0o600 || stat.size > 49152) throw pinError('PIN_APPROVAL_INVALID');
    const buffer = Buffer.alloc(49153);
    const { bytesRead } = await file.read(buffer,0,buffer.length,0);
    if (bytesRead > 49152) throw pinError('PIN_APPROVAL_INVALID');
    let input: unknown;
    try { input = JSON.parse(buffer.subarray(0,bytesRead).toString('utf8')); } catch { throw pinError('PIN_APPROVAL_INVALID'); }
    return parseOlosPinApproval(input);
  } finally { await file.close(); }
}
