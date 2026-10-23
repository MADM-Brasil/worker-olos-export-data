import {beforeAll,afterAll,beforeEach,afterEach,it,expect,vi} from 'vitest';
import {mkdtemp,writeFile,readFile,readdir,stat,rm} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {Socket} from 'node:net';
import {X509Certificate} from 'node:crypto';
vi.mock('node:tls',async original=>{
 const tls=await original<typeof import('node:tls')>();return {...tls,connect:vi.fn(tls.connect)};
});
import {connect as tlsConnect} from 'node:tls';
import {runTlsCli,parseTlsArguments} from '../adapters/olos-tls-diagnostic.js';
import {diagnoseTdsTls} from '../src/diagnostics/tds-tls.js';
import {parseOlosPinApproval} from '../src/security/olos-certificate-pin.js';
import {syntheticCertificates,syntheticTdsTls} from './support/tls-diagnostic-fixture.js';
let certs:Awaited<ReturnType<typeof syntheticCertificates>>,peer:Awaited<ReturnType<typeof syntheticTdsTls>>|undefined;
let root:string,file:string,output:ReturnType<typeof vi.spyOn>;
const env={SYNC_ENABLED:'false'};
const fingerprint=(pem:string)=>new X509Certificate(pem).fingerprint256;
function input(pem=certs.selfSigned){return {version:1,purpose:'OLOS_ATTEMPTS_TDS_ACQUISITION',host:'10.196.234.11',port:1433,database:'ExportDataV3',
 approvalId:'synthetic',approvedBy:'synthetic',approvedAt:new Date(Date.now()-1000).toISOString(),expiresAt:new Date(Date.now()+60000).toISOString(),certificatePem:pem,fingerprintSha256:fingerprint(pem)};}
const args=()=>['--authorize','TDS_TLS_METADATA_ONLY','--mode','verify-corporate-pinned','--server-name','10.196.234.11','--pin-approval-file',file];
const result=()=>JSON.parse(String(output.mock.calls.at(-1)?.[0]));
beforeAll(async()=>{certs=await syntheticCertificates();},30000);
afterAll(async()=>{await certs?.stop();});
beforeEach(async()=>{
 root=await mkdtemp(join(tmpdir(),'olos-corporate-tls-diagnostic-'));file=join(root,'existing-approval.json');
 await writeFile(file,JSON.stringify(input()),{mode:0o600});peer=undefined;
 output=vi.spyOn(process.stdout,'write').mockImplementation(()=>true);vi.mocked(tlsConnect).mockClear();
 const original=Socket.prototype.connect;
 vi.spyOn(Socket.prototype,'connect').mockImplementation(function(this:Socket,...parameters:unknown[]){
  expect(parameters[0]).toBe(1433);expect(parameters[1]).toBe('10.196.234.11');
  if(!peer)throw Error('UNEXPECTED_NETWORK_ACCESS');
  return (original as (...args:unknown[])=>Socket).call(this,peer.port,'127.0.0.1',...parameters.slice(2));
 } as typeof Socket.prototype.connect);
});
afterEach(async()=>{
 if(peer){
  expect(peer.sessions).toHaveLength(1);
  for(const session of peer.sessions){expect(session.types.every(t=>t===0x12)).toBe(true);expect(session.applicationBytes).toBe(0);}
  await peer.stop();
 }
 const printed=output.mock.calls.map((c:unknown[])=>String(c[0])).join('');
 expect(printed).not.toMatch(/BEGIN|synthetic|subject|issuer|validFrom|validTo|certificatePem|stack|password/);
 expect(vi.mocked(Socket.prototype.connect).mock.calls.length).toBeLessThanOrEqual(1);
 vi.restoreAllMocks();await rm(root,{recursive:true,force:true});
});
async function run(options:Partial<Parameters<typeof syntheticTdsTls>[0]>={},pem=certs.selfSigned){
 await writeFile(file,JSON.stringify(input(pem)),{mode:0o600});const before=await readFile(file),permissions=(await stat(file)).mode;
 peer=await syntheticTdsTls({cert:certs.selfSigned,key:certs.selfKey,...options});
 const exit=await runTlsCli(args(),env);await new Promise(resolve=>setTimeout(resolve,20));
 expect(await readFile(file)).toEqual(before);expect(await readdir(root)).toEqual(['existing-approval.json']);
 expect((await stat(file)).mode).toBe(permissions);
 expect(Object.keys(result()).sort()).toEqual(['code','stage','fingerprintSha256'].sort());
 return exit;
}
it('reuses the corporate PinnedMessageIO TLS options and existing approval, with no LOGIN7 or files',async()=>{
 expect(await run({fragmented:true,multiPacket:true})).toBe(0);
 expect(result()).toEqual({code:'PIN_VERIFIED',stage:'TLS_VALIDATED',fingerprintSha256:fingerprint(certs.selfSigned)});
 expect(tlsConnect).toHaveBeenCalledTimes(1);
 expect(vi.mocked(tlsConnect).mock.calls[0]?.[0]).toMatchObject({host:'10.196.234.11',ca:certs.selfSigned,rejectUnauthorized:true,allowPartialTrustChain:true,minVersion:'TLSv1.2',maxVersion:'TLSv1.2',checkServerIdentity:expect.any(Function)});
});
it('retains DEPTH_ZERO_SELF_SIGNED_CERT and the actual rejected remote fingerprint before Node destroys TLS',async()=>{
 expect(await run({},certs.serverLeaf)).toBe(1);
 expect(result()).toEqual({code:'DEPTH_ZERO_SELF_SIGNED_CERT',stage:'TLS_HANDSHAKE_STARTED',fingerprintSha256:fingerprint(certs.selfSigned)});
 expect(result().fingerprintSha256).not.toBe(fingerprint(certs.serverLeaf));expect(tlsConnect).toHaveBeenCalledTimes(1);
});
it('retains a pin mismatch after successful chain validation, without accepting the peer',async()=>{
 expect(await run({cert:certs.cert,key:certs.key},certs.ca)).toBe(1);
 expect(result()).toEqual({code:'PIN_MISMATCH',stage:'TLS_HANDSHAKE_STARTED',fingerprintSha256:fingerprint(certs.cert)});
});
it('preserves certificate expiration errors and the rejected peer fingerprint',async()=>{
 expect(await run({cert:certs.expired,key:certs.key},certs.ca)).toBe(1);
 expect(result()).toEqual({code:'CERT_HAS_EXPIRED',stage:'TLS_HANDSHAKE_STARTED',fingerprintSha256:fingerprint(certs.expired)});
});
it.each([
 [{disconnect:true},'NETWORK_FAILURE','PRELOGIN_SENT'],
 [{disconnectTls:true},'NETWORK_FAILURE','TLS_HANDSHAKE_STARTED'],
 [{handshakeResponse:Buffer.from('PRIVATE_INVALID_TLS_BYTES')},'ERR_SSL_WRONG_VERSION_NUMBER','TLS_HANDSHAKE_STARTED']
] as const)('reports unavailable certificate as null: %j',async(options,code,stage)=>{
 expect(await run(options)).toBe(1);expect(result()).toEqual({code,stage,fingerprintSha256:null});
 expect(JSON.stringify(result())).not.toContain('PRIVATE_INVALID_TLS_BYTES');
});
it('times out once during TLS with no retry or fingerprint fabrication',async()=>{
 peer=await syntheticTdsTls({cert:certs.selfSigned,key:certs.selfKey,stallTls:true});
 const diagnostic=await diagnoseTdsTls({host:'10.196.234.11',port:1433,serverName:'ignored',mode:'verify-corporate-pinned',pinApproval:parseOlosPinApproval(input()),timeoutMs:100});
 expect(diagnostic.pinnedDiagnostic).toEqual({code:'TIMEOUT',stage:'TLS_HANDSHAKE_STARTED',fingerprintSha256:null});
 expect(diagnostic.verified).toBe(false);expect(tlsConnect).toHaveBeenCalledTimes(1);
});
it.each(['wrong-purpose','expired','wrong-pin'])('rejects %s approval locally without mutation or network',async kind=>{
 const approval=input();
 if(kind==='wrong-purpose')approval.purpose='OLOS_TDS_TLS_METADATA_ONLY';
 if(kind==='expired')approval.expiresAt=new Date(Date.now()-100).toISOString();
 if(kind==='wrong-pin')approval.fingerprintSha256='00:'.repeat(31)+'00';
 await writeFile(file,JSON.stringify(approval));const before=await readFile(file);
 expect(await runTlsCli(args(),env)).not.toBe(0);
 expect(Object.keys(result()).sort()).toEqual(['code','stage','fingerprintSha256'].sort());
 expect(result()).toMatchObject({stage:'AUTHORIZATION_STARTED',fingerprintSha256:null});
 expect(await readFile(file)).toEqual(before);expect(Socket.prototype.connect).not.toHaveBeenCalled();expect(tlsConnect).not.toHaveBeenCalled();
});
it('rejects exports, CA overrides, credentials, alternate destinations and TLS bypass before connection',async()=>{
 for(const extra of [['--export-certificate-pem',join(root,'export.pem')],['--ca-file',file],['--password','secret'],['--host','127.0.0.1'],['--procedure','ReadDataCurrentDay']]){
  expect(await runTlsCli([...args(),...extra],env)).toBe(64);
  expect(result()).toEqual({code:'CONFIGURATION_OR_AUTHORIZATION_REJECTED',stage:'AUTHORIZATION_STARTED',fingerprintSha256:null});
 }
 for(const unsafe of [{SYNC_ENABLED:'true'},{SYNC_ENABLED:'false',NODE_TLS_REJECT_UNAUTHORIZED:'0'}])expect(()=>parseTlsArguments(args(),unsafe)).toThrow();
 expect(Socket.prototype.connect).not.toHaveBeenCalled();expect(await readdir(root)).toEqual(['existing-approval.json']);
});
it('the core also refuses export callbacks and alternate corporate endpoints',async()=>{
 const options={host:'10.196.234.11',port:1433,serverName:'ignored',mode:'verify-corporate-pinned' as const,pinApproval:parseOlosPinApproval(input())},exporter=vi.fn();
 for(const value of [await diagnoseTdsTls(options,exporter),await diagnoseTdsTls({...options,host:'127.0.0.1'})])expect(value.pinnedDiagnostic?.code).toBe('INVALID_CONFIGURATION');
 expect(exporter).not.toHaveBeenCalled();expect(Socket.prototype.connect).not.toHaveBeenCalled();
});
