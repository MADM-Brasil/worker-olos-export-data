import {beforeAll,afterAll,it,expect,vi} from 'vitest';
import {createRequire} from 'node:module';
import {Socket} from 'node:net';
import {X509Certificate} from 'node:crypto';
import type {Transform} from 'node:stream';
import {PinnedMessageIO} from '../src/diagnostics/pinned-message-io.js';
import {parseOlosPinApproval} from '../src/security/olos-certificate-pin.js';
import {InspectionDiagnostics,inspectionTechnicalErrorCode,reportInspectionFailure,reportTransportDiagnostic} from '../src/olos/tds/inspection-diagnostics.js';
import {syntheticCertificates,syntheticTdsTls} from './support/tls-diagnostic-fixture.js';
const require=createRequire(import.meta.url);
const MessageIO=require('tedious/lib/message-io.js') as typeof import('tedious/lib/message-io.js').default;
const Debug=require('tedious/lib/debug.js') as typeof import('tedious/lib/debug.js').default;
let certificates:Awaited<ReturnType<typeof syntheticCertificates>>;
beforeAll(async()=>{certificates=await syntheticCertificates();},30000);
afterAll(async()=>{await certificates?.stop();});
function approval(){return parseOlosPinApproval({version:1,purpose:'OLOS_ATTEMPTS_TDS_ACQUISITION',host:'10.196.234.11',port:1433,database:'ExportDataV3',approvalId:'synthetic',approvedBy:'synthetic',approvedAt:new Date(Date.now()-1000).toISOString(),expiresAt:new Date(Date.now()+60000).toISOString(),certificatePem:certificates.selfSigned,fingerprintSha256:new X509Certificate(certificates.selfSigned).fingerprint256});}
async function negotiate(pinned:boolean,handshakePacketType:number,fragmented=false,multiPacket=false,options:Partial<Parameters<typeof syntheticTdsTls>[0]>={},expectFailure=false){
 const server=await syntheticTdsTls({cert:pinned?certificates.selfSigned:certificates.cert,key:pinned?certificates.selfKey:certificates.key,handshakePacketType,fragmented,multiPacket,...options});
 const socket=new Socket(),diagnostics=new InspectionDiagnostics();
 const io=pinned?new PinnedMessageIO(socket,4096,new Debug(),approval()):new MessageIO(socket,4096,new Debug());
 if(io instanceof PinnedMessageIO)io.inspectionDiagnostics=diagnostics.observe;
 const incoming=(io as unknown as {incomingMessageStream:Transform}).incomingMessageStream;
 let timer:ReturnType<typeof setTimeout>|undefined;
 const stopped=new Promise<never>((_,reject)=>{socket.on('error',reject);incoming.on('error',reject);timer=setTimeout(()=>reject(Error('SYNTHETIC_TIMEOUT')),2500);});
 try{
  await Promise.race([(async()=>{
   await new Promise<void>(resolve=>socket.connect(server.port,'127.0.0.1',resolve));
   io.sendMessage(0x12,Buffer.from([1,0,6,0,1,255,1]));
   const initial=await io.readMessage();for await(const _ of initial){}
   await io.startTls({minVersion:'TLSv1.2',maxVersion:'TLSv1.2',...(!pinned?{ca:certificates.ca}:{})},'localhost',false);
  })(),stopped]);
  expect(io.securePair?.cleartext.authorized).toBe(true);expect(io.tlsNegotiationComplete).toBe(true);
  expect(server.sessions).toHaveLength(1);expect(server.sessions[0]?.applicationBytes).toBe(0);
  expect(server.sessions[0]?.types.every(t=>t===0x12)).toBe(true);
  return {diagnostic:diagnostics.snapshot(),errorCode:null};
 }catch(error){
  if(!expectFailure)throw error;
  expect(io.tlsNegotiationComplete).toBe(false);expect(server.sessions).toHaveLength(1);expect(server.sessions[0]?.applicationBytes).toBe(0);
  return {diagnostic:diagnostics.snapshot(),errorCode:inspectionTechnicalErrorCode(error,'CONNECTION_FAILED')};
 }finally{
  clearTimeout(timer);socket.unpipe();io.outgoingMessageStream.unpipe();io.securePair?.cleartext.destroy();io.securePair?.encrypted.destroy();incoming.destroy();io.outgoingMessageStream.destroy();socket.destroy();await server.stop();
 }
}
it.each([0x04,0x12])('Tedious 18.6.2 accepts handshake envelope %i with verified TLS',async type=>{await negotiate(false,type);});
it.each([0x04,0x12])('pinned TLS matches upstream handshake envelope %i without LOGIN7/RPC',async type=>{await negotiate(true,type);});
it.each([false,true])('pinned TLS supports fragmented TCP and multi-packet TDS=%s',async multi=>{await negotiate(true,0x04,true,multi);});

it.each([0x01,0x10,0x17])('unexpected handshake type %i remains INVALID_TDS with its packet type',async type=>{
 const result=await negotiate(true,type,false,false,{},true);
 expect(result).toMatchObject({errorCode:'INVALID_TDS',diagnostic:{phase:'TLS_HANDSHAKE',errorCode:'INVALID_TDS',rpcDispatchState:'NOT_OBSERVED',transport:expect.arrayContaining([{phase:'TLS_HANDSHAKE',lastObservedStage:'TLS_HANDSHAKE_STARTED',reason:'TLS_RESPONSE_TYPE_UNEXPECTED',packetType:type,errorCode:'INVALID_TDS',nativeErrorType:'Error'}])}});
});
it('malformed packet header is distinct from an unexpected handshake type',async()=>{
 const {diagnostic}=await negotiate(true,4,false,false,{invalidHandshakeHeader:true},true);
 expect(diagnostic.transport).toEqual(expect.arrayContaining([expect.objectContaining({reason:'TDS_PACKET_STREAM_ERROR',nativeErrorType:'ConnectionError'})]));
 expect(diagnostic.transport.some(d=>d.reason==='TLS_RESPONSE_TYPE_UNEXPECTED')).toBe(false);
});
it('malformed PRELOGIN decoder failure reaches the guarded message read',async()=>{
 const {diagnostic,errorCode}=await negotiate(true,4,false,false,{response:Buffer.from([1,0,255,0,1,255,1])},true);
 expect(errorCode).toBe('ERR_OUT_OF_RANGE');expect(diagnostic).toMatchObject({phase:'PRELOGIN',errorCode:'ERR_OUT_OF_RANGE'});
 expect(diagnostic.transport).toEqual(expect.arrayContaining([expect.objectContaining({reason:'PRELOGIN_DECODE_FAILED',packetType:4,nativeErrorType:'RangeError'})]));
 expect(diagnostic.observedStages).not.toContain('TLS_HANDSHAKE_STARTED');
});
it('invalid TLS record preserves OpenSSL code instead of classifying it as INVALID_TDS',async()=>{
 const {diagnostic,errorCode}=await negotiate(true,4,false,false,{handshakeResponse:Buffer.from('SYNTHETIC_INVALID_TLS_RECORD')},true);
 expect(errorCode).toBe('ERR_SSL_WRONG_VERSION_NUMBER');expect(diagnostic.errorCode).toBe(errorCode);
 expect(diagnostic.transport).toEqual(expect.arrayContaining([expect.objectContaining({reason:'TLS_HANDSHAKE_FAILED',errorCode})]));
 expect(JSON.stringify(diagnostic)).not.toContain('SYNTHETIC_INVALID_TLS_RECORD');
});
it('socket termination during TLS settles negotiation without starting LOGIN7 or retrying',async()=>{
 const {diagnostic,errorCode}=await negotiate(true,4,false,false,{disconnectTls:true},true);
 expect(errorCode).toBe('ECONNRESET');expect(diagnostic.transport).toEqual(expect.arrayContaining([expect.objectContaining({reason:'TLS_SOCKET_ENDED'})]));
});
it('TLS version mismatch and a substituted certificate remain blocked',async()=>{
 for(const options of [{minVersion:'TLSv1.3' as const},{cert:certificates.cert,key:certificates.key}]){
  const {diagnostic,errorCode}=await negotiate(true,0x12,false,false,options,true);
  expect(errorCode).not.toBe('INVALID_TDS');expect(diagnostic.observedStages).not.toContain('TLS_VALIDATED');
  expect(diagnostic.transport).toEqual(expect.arrayContaining([expect.objectContaining({reason:'TLS_HANDSHAKE_FAILED'})]));
 }
});
it('concurrent TLS error, socket termination and late secure event preserve one failure',async()=>{
 const server=await syntheticTdsTls({cert:certificates.selfSigned,key:certificates.selfKey,stallTls:true});
 const socket=new Socket(),io=new PinnedMessageIO(socket,4096,new Debug(),approval()),diagnostic=new InspectionDiagnostics();io.inspectionDiagnostics=diagnostic.observe;
 const incoming=(io as unknown as {incomingMessageStream:Transform}).incomingMessageStream;
 const secure=vi.fn();io.on('secure',secure);socket.on('error',()=>{});incoming.on('error',()=>{});
 try{
  await new Promise<void>(resolve=>socket.connect(server.port,'127.0.0.1',resolve));
  io.sendMessage(0x12,Buffer.from([1,0,6,0,1,255,1]));const initial=await io.readMessage();for await(const _ of initial){}
  const endListeners=socket.listenerCount('end'),closeListeners=socket.listenerCount('close');
  const started=io.startTls({minVersion:'TLSv1.2',maxVersion:'TLSv1.2'},'localhost',false);
  const checked=expect(started).rejects.toMatchObject({code:'ERR_SSL_WRONG_VERSION_NUMBER'});
  const tls=io.securePair!.cleartext;
  tls.emit('error',Object.assign(Error('PRIVATE_TLS_MESSAGE'),{code:'ERR_SSL_WRONG_VERSION_NUMBER'}));
  socket.emit('end');tls.emit('error',Object.assign(Error('PRIVATE_LATE_ERROR'),{code:'ECONNRESET'}));tls.emit('secureConnect');
  await checked;await new Promise<void>(resolve=>setImmediate(resolve));
  expect(secure).not.toHaveBeenCalled();expect(io.tlsNegotiationComplete).toBe(false);
  expect(diagnostic.snapshot()).toMatchObject({errorCode:'ERR_SSL_WRONG_VERSION_NUMBER',rpcDispatchState:'NOT_OBSERVED'});
  expect(JSON.stringify(diagnostic.snapshot())).not.toContain('PRIVATE_');
  expect(socket.listenerCount('end')).toBeLessThanOrEqual(endListeners);expect(socket.listenerCount('close')).toBeLessThanOrEqual(closeListeners);
 }finally{socket.unpipe();io.outgoingMessageStream.unpipe();io.securePair?.cleartext.destroy();io.securePair?.encrypted.destroy();incoming.destroy();io.outgoingMessageStream.destroy();socket.destroy();await server.stop();}
});
it('diagnostics prefer the original code, are bounded, and strip unsafe fields at the observer boundary',()=>{
 const secret='PRIVATE_TLS_OR_SQL_MESSAGE',observer=vi.fn(),native={code:'INVALID_TDS',name:secret,stack:secret};
 reportTransportDiagnostic(observer,'TLS_HANDSHAKE_FAILED',native,999);
 expect(observer).toHaveBeenCalledWith({kind:'transport',reason:'TLS_HANDSHAKE_FAILED',errorCode:'INVALID_TDS',nativeErrorType:'UNKNOWN'});
 const tracker=new InspectionDiagnostics();tracker.observe({kind:'stage',stage:'TLS_HANDSHAKE_STARTED'});
 reportInspectionFailure(tracker.observe,{code:'ESOCKET',message:secret,cause:{code:'INVALID_TDS',message:secret}});
 reportInspectionFailure(tracker.observe,{code:'CONNECTION_FAILED'});
 for(let i=0;i<40;i++)reportTransportDiagnostic(tracker.observe,'TLS_SOCKET_CLOSED',{code:'ECONNRESET',message:secret});
 expect(tracker.snapshot()).toMatchObject({errorCode:'INVALID_TDS',transport:expect.any(Array)});
 expect(tracker.snapshot().transport).toHaveLength(32);expect(JSON.stringify(tracker.snapshot())).not.toContain(secret);
});

it('a full diagnostic budget still retains the first failure and its reason',()=>{
 const diagnostics=new InspectionDiagnostics();diagnostics.observe({kind:'stage',stage:'TLS_HANDSHAKE_STARTED'});
 for(let i=0;i<40;i++)reportTransportDiagnostic(diagnostics.observe,'TLS_RESPONSE_RECEIVED',undefined,4);
 reportTransportDiagnostic(diagnostics.observe,'TLS_RESPONSE_TYPE_UNEXPECTED',{code:'INVALID_TDS'},16);
 expect(diagnostics.snapshot()).toMatchObject({errorCode:'INVALID_TDS',transport:expect.arrayContaining([expect.objectContaining({reason:'TLS_RESPONSE_TYPE_UNEXPECTED',packetType:16})])});
 expect(diagnostics.snapshot().transport).toHaveLength(32);
});
