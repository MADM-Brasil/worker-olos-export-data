import { createRequire } from 'node:module';
import { connect, type SecureContextOptions } from 'node:tls';
import type { Duplex, Transform } from 'node:stream';
import { assertOlosPinApproval, verifyPinnedPeer, pinError, type OlosPinApproval } from '../security/olos-certificate-pin.js';
import {reportTransportDiagnostic,type InspectionDiagnosticObserver} from '../olos/tds/inspection-diagnostics.js';
const require = createRequire(import.meta.url);
const MessageIO = require('tedious/lib/message-io.js') as typeof import('tedious/lib/message-io.js').default;
const Message = require('tedious/lib/message.js') as typeof import('tedious/lib/message.js').default;
const PreloginPayload = require('tedious/lib/prelogin-payload.js') as typeof import('tedious/lib/prelogin-payload.js').default;
const Pair = require('native-duplexpair') as new () => { socket1: Duplex; socket2: Duplex };

/** Instance-only TDS 7.x TLS negotiation. No prototype changes, LOGIN7 or RPC API.
 * Tedious 18.6.2 MessageIO does not forward checkServerIdentity to tls.connect.
 * Keep certificate validation enabled; only the approved self-signed leaf is trusted.
 */
export class PinnedMessageIO extends MessageIO {
  inspectionDiagnostics:InspectionDiagnosticObserver|undefined;
  private used = false;
  private initialMessage = true;
  constructor(...args: [...ConstructorParameters<typeof MessageIO>, OlosPinApproval]) {
    const [socket, size, debug, approval] = args;
    super(socket,size,debug); this.approval = approval;
    const incoming=(this as unknown as {incomingMessageStream:Transform}).incomingMessageStream;
    incoming.on('error',error=>reportTransportDiagnostic(this.inspectionDiagnostics,'TDS_PACKET_STREAM_ERROR',error));
  }
  override async readMessage():Promise<InstanceType<typeof Message>>{
    let response:InstanceType<typeof Message>;
    try{response=await super.readMessage();}
    catch(error){reportTransportDiagnostic(this.inspectionDiagnostics,'TDS_MESSAGE_READ_FAILED',error);throw error;}
    if(!this.initialMessage)return response;
    this.initialMessage=false;
    this.inspectionDiagnostics?.({kind:'stage',stage:'PRELOGIN_RESPONSE_RECEIVED'});
    reportTransportDiagnostic(this.inspectionDiagnostics,'PRELOGIN_RESPONSE_RECEIVED',undefined,response.type);
    // The driver's SENT_PRELOGIN enter() does not catch decoder exceptions. Decode
    // the same complete message here so rejection reaches its guarded readMessage().
    const chunks:Buffer[]=[];
    try{for await(const chunk of response)chunks.push(chunk as Buffer);}
    catch(error){reportTransportDiagnostic(this.inspectionDiagnostics,'TDS_MESSAGE_STREAM_ERROR',error,response.type);throw error;}
    const body=Buffer.concat(chunks);
    try{new PreloginPayload(body);}
    catch(error){reportTransportDiagnostic(this.inspectionDiagnostics,'PRELOGIN_DECODE_FAILED',error,response.type);throw error;}
    const replay=new Message({type:response.type,resetConnection:false});replay.end(body);return replay;
  }
  private readonly approval: OlosPinApproval;
  override startTls(details: SecureContextOptions, _hostname: string, trustServerCertificate: boolean): Promise<void> {
    this.inspectionDiagnostics?.({kind:'stage',stage:'TLS_HANDSHAKE_STARTED'});
    const rejected=(error:unknown)=>{reportTransportDiagnostic(this.inspectionDiagnostics,'TLS_CONFIGURATION_REJECTED',error);return Promise.reject(error);};
    if (require('tedious/package.json').version !== '18.6.2' || this.used || trustServerCertificate ||
      details.minVersion !== 'TLSv1.2' || details.maxVersion !== 'TLSv1.2' ||
      Object.keys(details).some(k => !['minVersion','maxVersion'].includes(k)) || process.env.NODE_TLS_REJECT_UNAUTHORIZED === '0') {
      return rejected(pinError('PIN_CONFIGURATION_INVALID'));
    }
    try { assertOlosPinApproval(this.approval); } catch (error) { return rejected(error); }
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
      const cleanup = () => {
        pair.socket2.removeListener('readable',onReadable);tls.removeListener('secureConnect',onSecure);
        this.socket.removeListener('error',onSocketError);this.socket.removeListener('end',onSocketEnd);this.socket.removeListener('close',onSocketClose);
      };
      const onSocketError=(error:Error)=>{reportTransportDiagnostic(this.inspectionDiagnostics,'TLS_SOCKET_ERROR',error);fail(error);};
      const onSocketEnd=()=>{const error=pinError('ECONNRESET');reportTransportDiagnostic(this.inspectionDiagnostics,'TLS_SOCKET_ENDED',error);fail(error);};
      const onSocketClose=()=>{const error=pinError('ECONNRESET');reportTransportDiagnostic(this.inspectionDiagnostics,'TLS_SOCKET_CLOSED',error);fail(error);};
      const fail = (error: unknown) => {
        if (finished) return;
        reportTransportDiagnostic(this.inspectionDiagnostics,'TLS_HANDSHAKE_FAILED',error);
        finished = true; cleanup(); tls.destroy(); pair.socket2.destroy(); reject(error);
      };
      const onSecure = () => {
        if(finished)return;
        const peer = tls.getPeerCertificate();
        const error = verifyPinnedPeer(this.approval,peer);
        if (!tls.authorized || tls.getProtocol() !== 'TLSv1.2' || error) { fail(error ?? pinError('PIN_TLS_REJECTED')); return; }
        finished = true; cleanup(); tls.disableRenegotiation();
        // Retain the error listener across failure cleanup to absorb late TLS errors.
        tls.setMaxSendFragment(this.outgoingMessageStream.packetSize);
        this.outgoingMessageStream.unpipe(this.socket); this.socket.unpipe(incoming);
        this.socket.pipe(pair.socket2); pair.socket2.pipe(this.socket);
        tls.pipe(incoming); this.outgoingMessageStream.pipe(tls);
        this.inspectionDiagnostics?.({kind:'stage',stage:'TLS_VALIDATED'});
        this.tlsNegotiationComplete = true; this.emit('secure',tls); resolve();
      };
      const onReadable = () => {
        if (finished) return;
        const chunks: Buffer[] = []; let chunk: Buffer | null;
        while ((chunk = pair.socket2.read() as Buffer | null) !== null) chunks.push(chunk);
        this.sendMessage(0x12,Buffer.concat(chunks));
        void this.readMessage().then(async response => {
          if(finished)return;
          reportTransportDiagnostic(this.inspectionDiagnostics,'TLS_RESPONSE_RECEIVED',undefined,response.type);
          // Tedious accepts the handshake stream independently of the envelope
          // type. Support its TABULAR_RESULT/PRELOGIN envelopes; TLS still verifies
          // the certificate, exact SHA-256 pin and TLS 1.2 before LOGIN7.
          if(response.type!==0x04 && response.type!==0x12){
            const error=pinError('INVALID_TDS');reportTransportDiagnostic(this.inspectionDiagnostics,'TLS_RESPONSE_TYPE_UNEXPECTED',error,response.type);throw error;
          }
          if (!finished) pair.socket2.once('readable',onReadable);
          try{for await (const data of response) { if (!finished) pair.socket2.write(data); }}
          catch(error){reportTransportDiagnostic(this.inspectionDiagnostics,'TDS_MESSAGE_STREAM_ERROR',error,response.type);throw error;}
        }).catch(fail);
      };
      tls.on('error',error=>{
        if(this.tlsNegotiationComplete){reportTransportDiagnostic(this.inspectionDiagnostics,'TLS_STREAM_FAILED',error);this.socket.destroy(error);}
        else fail(error);
      });
      pair.socket2.on('error',error=>{if(!this.tlsNegotiationComplete)fail(error);else this.socket.destroy(error);});
      this.socket.once('error',onSocketError);this.socket.once('end',onSocketEnd);this.socket.once('close',onSocketClose);
      tls.once('secureConnect',onSecure);pair.socket2.once('readable',onReadable);
    });
  }
}
