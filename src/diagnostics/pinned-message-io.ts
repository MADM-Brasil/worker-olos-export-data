import { createRequire } from 'node:module';
import { connect, type SecureContextOptions } from 'node:tls';
import type { Duplex, Transform } from 'node:stream';
import { assertOlosPinApproval, verifyPinnedPeer, pinError, type OlosPinApproval } from '../security/olos-certificate-pin.js';
const require = createRequire(import.meta.url);
const MessageIO = require('tedious/lib/message-io.js') as typeof import('tedious/lib/message-io.js').default;
const Pair = require('native-duplexpair') as new () => { socket1: Duplex; socket2: Duplex };

/** Instance-only TDS 7.x TLS negotiation. No prototype changes, LOGIN7 or RPC API.
 * Tedious 18.6.2 MessageIO does not forward checkServerIdentity to tls.connect.
 * Keep certificate validation enabled; only the approved self-signed leaf is trusted.
 */
export class PinnedMessageIO extends MessageIO {
  private used = false;
  constructor(...args: [...ConstructorParameters<typeof MessageIO>, OlosPinApproval]) {
    const [socket, size, debug, approval] = args;
    super(socket,size,debug); this.approval = approval;
  }
  private readonly approval: OlosPinApproval;
  override startTls(details: SecureContextOptions, _hostname: string, trustServerCertificate: boolean): Promise<void> {
    if (require('tedious/package.json').version !== '18.6.2' || this.used || trustServerCertificate ||
      details.minVersion !== 'TLSv1.2' || details.maxVersion !== 'TLSv1.2' ||
      Object.keys(details).some(k => !['minVersion','maxVersion'].includes(k)) || process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') {
      return Promise.reject(pinError('PIN_CONFIGURATION_INVALID'));
    }
    try { assertOlosPinApproval(this.approval); } catch (error) { return Promise.reject(error); }
    this.used = true;
    const pair = new Pair();
    const tls = connect({ socket: pair.socket1, host: this.approval.host,
      minVersion: 'TLSv1.2', maxVersion: 'TLSv1.2', rejectUnauthorized: true,
      ca: this.approval.certificatePem,
      checkServerIdentity: (_name, peer) => verifyPinnedPeer(this.approval,peer) });
    this.securePair = { cleartext: tls, encrypted: pair.socket2 };
    const incoming = (this as unknown as { incomingMessageStream: Transform }).incomingMessageStream;
    return new Promise<void>((resolve,reject) => {
      let finished = false;
      const cleanup = () => { pair.socket2.removeListener('readable',onReadable); tls.removeListener('secureConnect',onSecure); tls.removeListener('error',fail); };
      const fail = (error: unknown) => {
        if (finished) return;
        finished = true; cleanup(); tls.destroy(); pair.socket2.destroy(); reject(error);
      };
      const onSecure = () => {
        const peer = tls.getPeerCertificate();
        const error = verifyPinnedPeer(this.approval,peer);
        if (!tls.authorized || tls.getProtocol() !== 'TLSv1.2' || error) { fail(error ?? pinError('PIN_TLS_REJECTED')); return; }
        finished = true; cleanup(); tls.disableRenegotiation();
        tls.on('error',error => this.socket.destroy(error));
        tls.setMaxSendFragment(this.outgoingMessageStream.packetSize);
        this.outgoingMessageStream.unpipe(this.socket); this.socket.unpipe(incoming);
        this.socket.pipe(pair.socket2); pair.socket2.pipe(this.socket);
        tls.pipe(incoming); this.outgoingMessageStream.pipe(tls);
        this.tlsNegotiationComplete = true; this.emit('secure',tls); resolve();
      };
      const onReadable = () => {
        if (finished) return;
        const chunks: Buffer[] = []; let chunk: Buffer | null;
        while ((chunk = pair.socket2.read() as Buffer | null) !== null) chunks.push(chunk);
        this.sendMessage(0x12,Buffer.concat(chunks));
        void this.readMessage().then(async response => {
          if (response.type !== 0x04) throw pinError('INVALID_TDS');
          if (!finished) pair.socket2.once('readable',onReadable);
          for await (const data of response) { if (!finished) pair.socket2.write(data); }
        }).catch(fail);
      };
      tls.once('error',fail); tls.once('secureConnect',onSecure); pair.socket2.once('readable',onReadable);
    });
  }
}
