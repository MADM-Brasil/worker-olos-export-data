import { PinnedMessageIO } from '../../diagnostics/pinned-message-io.js';
import { assertOlosPinApproval, verifyPinnedPeer, type OlosPinApproval } from '../../security/olos-certificate-pin.js';
import { isPreparedAttemptsEndpoint, claimAttemptsConnection, claimAttemptsSocket, claimAttemptsLogin, matchesPreparedAttemptsConnection, attemptsPin, assertCorporateAttemptsExecution, assertPreparedAttemptsRpc, type PreparedAttemptsEndpoint } from './attempts-access.js';
import { matchesDispositionConnection, assertDispositionRpc, type DispositionEndpoint } from './disposition-access.js';
import { isExceptionalDispositionEndpoint, claimExceptionalConnection } from './disposition-exception.js';
import type { Socket } from 'node:net';
import { EventEmitter } from 'node:events';
import { Readable } from 'node:stream';
import { createRequire } from 'node:module';
import { Connection, type Request, type ConnectionConfiguration } from 'tedious';
type Message = InstanceType<typeof import('tedious/lib/message.js').default>;
import type {ColumnMetadata} from 'tedious/lib/token/colmetadata-token-parser.js';
import type { TokenHandler } from 'tedious/lib/token/handler.js';
import type { Parser } from 'tedious/lib/token/token-stream-parser.js';
import { ExactTokenParser, type TdsField, type RawParserLimits } from './raw-parser.js';
import { OlosError } from '../types.js';

const version = createRequire(import.meta.url)('tedious/package.json').version as string;
export class ExactConnection extends Connection {
  private exceptionalConnectUsed = false;
  constructor(config: ConnectionConfiguration, private readonly dispositionEndpoint?: DispositionEndpoint | PreparedAttemptsEndpoint, private readonly fixturePin?: OlosPinApproval) {
    if (version !== '18.6.2') throw new OlosError('CONTRACT_UNCONFIRMED');
    if (fixturePin) {
      assertOlosPinApproval(fixturePin);
      if (dispositionEndpoint || config.server !== '127.0.0.1' || config.options?.database !== 'phase3d_fixture' ||
        config.options.cryptoCredentialsDetails?.minVersion !== 'TLSv1.2' || config.options.cryptoCredentialsDetails.maxVersion !== 'TLSv1.2') throw new OlosError('REAL_EXECUTION_BLOCKED');
    }
    if (dispositionEndpoint && isPreparedAttemptsEndpoint(dispositionEndpoint)) {
      if (!matchesPreparedAttemptsConnection(config,dispositionEndpoint)) throw new OlosError('REAL_EXECUTION_BLOCKED');
    }
    else if (dispositionEndpoint && isExceptionalDispositionEndpoint(dispositionEndpoint)) claimExceptionalConnection(config,dispositionEndpoint);
    else if (!matchesDispositionConnection(config,dispositionEndpoint) && (config.server !== '127.0.0.1' || config.options?.database !== 'phase3d_fixture' ||
      config.options.encrypt !== true || config.options.trustServerCertificate !== false ||
      config.options.serverName !== 'localhost' || !config.options.cryptoCredentialsDetails?.ca)) throw new OlosError('REAL_EXECUTION_BLOCKED');
    super(config);
  }
  override connect(listener?: (err?: Error) => void): void {
    if (this.dispositionEndpoint && isPreparedAttemptsEndpoint(this.dispositionEndpoint)) {
      if (!matchesPreparedAttemptsConnection(this.config,this.dispositionEndpoint)) throw new OlosError('REAL_EXECUTION_BLOCKED');
      claimAttemptsConnection(this.dispositionEndpoint);
    }
    super.connect(listener);
  }
  override connectOnPort(port: number, multiSubnetFailover: boolean, signal: AbortSignal, connector?: () => Promise<Socket>): void {
    if (this.dispositionEndpoint && isPreparedAttemptsEndpoint(this.dispositionEndpoint)) {
      assertCorporateAttemptsExecution(this.dispositionEndpoint);
      if(this.routingData || port!==1433 || multiSubnetFailover || connector || !matchesPreparedAttemptsConnection(this.config,this.dispositionEndpoint))throw new OlosError('REAL_EXECUTION_BLOCKED');
      claimAttemptsSocket(this.dispositionEndpoint);
    }
    if (this.dispositionEndpoint && isExceptionalDispositionEndpoint(this.dispositionEndpoint)) {
      if (this.exceptionalConnectUsed || this.routingData || port !== this.config.options.port || multiSubnetFailover || connector) {
        this.emit('connect',new OlosError('REAL_EXECUTION_BLOCKED')); this.close(); return;
      }
      this.exceptionalConnectUsed = true;
    }
    super.connectOnPort(port,multiSubnetFailover,signal,connector);
  }
  override socketHandlingForSendPreLogin(socket: Socket): void {
    const corporate = this.dispositionEndpoint && isPreparedAttemptsEndpoint(this.dispositionEndpoint) ? this.dispositionEndpoint : undefined;
    const pin = corporate ? attemptsPin(corporate) : this.fixturePin;
    if (!pin) return super.socketHandlingForSendPreLogin(socket);
    if(corporate){try{assertCorporateAttemptsExecution(corporate);}catch(error){socket.destroy();throw error;}}
    // Narrow instance hook: preserve Tedious state machine; install IO before PRELOGIN.
    socket.on('error',error => this.socketError(error));
    socket.on('close',() => this.socketClose()); socket.on('end',() => this.socketEnd());
    socket.setKeepAlive(true,30000);
    this.messageIo = new PinnedMessageIO(socket,this.config.options.packetSize,this.debug,pin);
    this.messageIo.on('secure',tls => this.emit('secure',tls));
    this.socket = socket; this.closed = false;
    this.sendPreLogin(); this.transitionTo(this.STATE.SENT_PRELOGIN);
  }
  override sendLogin7Packet(): void {
    const corporate = this.dispositionEndpoint && isPreparedAttemptsEndpoint(this.dispositionEndpoint) ? this.dispositionEndpoint : undefined;
    if (corporate) {
      assertCorporateAttemptsExecution(corporate);
      const tls=this.messageIo.securePair?.cleartext;
      if(!this.messageIo.tlsNegotiationComplete || !tls?.authorized || tls.getProtocol()!=='TLSv1.2' || verifyPinnedPeer(attemptsPin(corporate),tls.getPeerCertificate()))throw new OlosError('REAL_EXECUTION_BLOCKED');
      claimAttemptsLogin(corporate);
    }
    if (this.fixturePin) {
      const tls = this.messageIo.securePair?.cleartext;
      if (!this.messageIo.tlsNegotiationComplete || !tls?.authorized || tls.getProtocol() !== 'TLSv1.2' || verifyPinnedPeer(this.fixturePin,tls.getPeerCertificate())) {
        this.emit('connect',new OlosError('REAL_EXECUTION_BLOCKED')); this.close(); return;
      }
    }
    if (this.dispositionEndpoint && isExceptionalDispositionEndpoint(this.dispositionEndpoint) &&
      (!this.messageIo.tlsNegotiationComplete || !['TLSv1.2','TLSv1.3'].includes(this.messageIo.securePair?.cleartext.getProtocol() ?? ''))) {
      this.emit('connect',new OlosError('REAL_EXECUTION_BLOCKED')); this.close(); return;
    }
    super.sendLogin7Packet();
  }
  override callProcedure(request: Request): void { if (this.dispositionEndpoint && isPreparedAttemptsEndpoint(this.dispositionEndpoint)) assertPreparedAttemptsRpc(request,this.dispositionEndpoint); else if (this.dispositionEndpoint) assertDispositionRpc(request,this.dispositionEndpoint); super.callProcedure(request); }
  acquisition: { metadataObserved?: (columns:readonly ColumnMetadata[])=>void; fields: readonly TdsField[] | undefined; limits: RawParserLimits; token: (value: { name: string; [key: string]: unknown }) => void; fail: () => void } | undefined;
  /** Request response only: LOGIN7, passwords and authentication messages never enter this sink. */
  responseCapture: ((chunk: Buffer) => Promise<void>) | undefined;
  private responseReplayed=false;
  responseReplay: (() => AsyncIterable<Buffer>) | undefined;
  override createTokenStreamParser(message: Message, handler: TokenHandler): Parser {
    if (version !== '18.6.2') throw new OlosError('CONTRACT_UNCONFIRMED');
    if (!this.acquisition || handler.constructor.name !== 'RequestTokenHandler') return super.createTokenStreamParser(message,handler);
    const acquisition = this.acquisition;
    const capture = this.responseCapture;
    const replay=this.responseReplay;
    if(replay){if(this.responseReplayed)throw new OlosError('INVALID_RESPONSE','unknown');this.responseReplayed=true;}
    async function* payload() {
      for await (const chunk of message) {
        if (!Buffer.isBuffer(chunk)) throw new OlosError('INVALID_RESPONSE','unknown');
        if (capture) await capture(chunk); // disk backpressure BEFORE parsing or lossy conversions
        if(!replay)yield chunk;
      }
      if(replay)yield* replay();
    }
    const raw = new ExactTokenParser(payload(),this.debug,this.config.options,acquisition.fields,acquisition.limits,acquisition.metadataObserved);
    const emitter = new EventEmitter();
    const parser = Readable.from(raw.tokens(),{ objectMode: true, highWaterMark: 1 });
    parser.on('data', token => {
      try {
        acquisition.token(token);
        // Mantém state machine, erros, OUTPUT, doneProc e row do driver; sem debug de valores.
        const dispatch = handler[token.handlerName as keyof TokenHandler] as (t: unknown) => void;
        dispatch.call(handler,token);
      } catch { acquisition.fail(); parser.destroy(); this.close(); }
    });
    parser.on('error',() => { acquisition.fail(); this.close(); });
    parser.on('end',() => emitter.emit('end'));
    return Object.assign(emitter,{ parser, debug: this.debug, options: this.config.options,
      pause: () => parser.pause(), resume: () => parser.resume() }) as Parser;
  }
}
