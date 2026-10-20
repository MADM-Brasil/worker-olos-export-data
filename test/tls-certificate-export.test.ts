import {beforeAll,afterAll,beforeEach,afterEach,it,expect,vi} from 'vitest';
import {mkdtemp,readFile,writeFile,chmod,rm,readdir,lstat,symlink} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {X509Certificate} from 'node:crypto';
import {syntheticCertificates,syntheticTdsTls} from './support/tls-diagnostic-fixture.js';

const state=vi.hoisted(()=>({port:0,before:undefined as (()=>Promise<void>)|undefined}));
vi.mock('../src/diagnostics/tds-tls.js',async original=>{
  const core=await original<typeof import('../src/diagnostics/tds-tls.js')>();
  return {...core,diagnoseTdsTls:vi.fn(async(options,exporter)=>{
    await state.before?.();
    // CLI still fixes the corporate address; tests redirect only to a loopback synthetic peer.
    return core.diagnoseTdsTls({...options,host:'127.0.0.1',port:state.port,serverName:'localhost',timeoutMs:2000},exporter);
  })};
});
import {diagnoseTdsTls} from '../src/diagnostics/tds-tls.js';
import {runTlsCli,parseTlsArguments} from '../adapters/olos-tls-diagnostic.js';
let certs:Awaited<ReturnType<typeof syntheticCertificates>>,peer:Awaited<ReturnType<typeof syntheticTdsTls>>;
let root:string,path:string,output:ReturnType<typeof vi.spyOn>;
const args=['--authorize','TDS_TLS_METADATA_ONLY','--server-name','10.196.234.11','--mode','inspect-unverified'];
const env={SYNC_ENABLED:'false'};
beforeAll(async()=>{certs=await syntheticCertificates();},30000);
afterAll(async()=>{await certs?.stop();});
beforeEach(async()=>{
  root=await mkdtemp(join(tmpdir(),'olos-pem-export-'));path=join(root,'public.pem');
  peer=await syntheticTdsTls({cert:certs.selfSigned,key:certs.selfKey});state.port=peer.port;state.before=undefined;
  vi.mocked(diagnoseTdsTls).mockClear();output=vi.spyOn(process.stdout,'write').mockImplementation(()=>true);
});
afterEach(async()=>{
  for(const session of peer.sessions){expect(session.types.every(t=>t===0x12)).toBe(true);expect(session.applicationBytes).toBe(0);}
  const text=output.mock.calls.map((c:unknown[])=>String(c[0])).join('');expect(text).not.toContain('-----BEGIN');expect(text).not.toContain(certs.selfKey);
  vi.restoreAllMocks();await peer.stop();await chmod(root,0o700);await rm(root,{recursive:true,force:true});
});
const exportArgs=()=>[...args,'--export-certificate-pem',path];
const result=()=>JSON.parse(String(output.mock.calls.at(-1)?.[0]));

it('exports exactly the public leaf, 0600, with matching DER SHA-256; inspection remains unverified',async()=>{
  expect(await runTlsCli(exportArgs(),env)).toBe(2);
  const pem=await readFile(path,'utf8'),cert=new X509Certificate(pem),stat=await lstat(path);
  expect(cert.raw).toEqual(new X509Certificate(certs.selfSigned).raw);expect(stat.mode&0o777).toBe(0o600);expect(stat.uid).toBe(process.getuid?.());
  expect(result()).toMatchObject({code:'INSPECTED_UNVERIFIED',verified:false,certificate:{fingerprintSha256:cert.fingerprint256}});
  expect(Object.keys(result().certificate).sort()).toEqual(['subject','issuer','validFrom','validTo','fingerprintSha256'].sort());
  expect(await readdir(root)).toEqual(['public.pem']);expect(peer.sessions).toHaveLength(1);
});
it('does not create files without the explicit export option',async()=>{
  expect(await runTlsCli(args,env)).toBe(2);expect(await readdir(root)).toEqual([]);
});
it('refuses an existing file before connecting and never overwrites it',async()=>{
  await writeFile(path,'existing evidence',{mode:0o600});
  expect(await runTlsCli(exportArgs(),env)).toBe(64);expect(diagnoseTdsTls).not.toHaveBeenCalled();
  expect(await readFile(path,'utf8')).toBe('existing evidence');
});
it('refuses symlink destinations without changing their targets',async()=>{
  const target=join(root,'target');await writeFile(target,'preserve');await symlink(target,path);
  expect(await runTlsCli(exportArgs(),env)).toBe(64);expect(diagnoseTdsTls).not.toHaveBeenCalled();
  expect(await readFile(target,'utf8')).toBe('preserve');
});
it('refuses non-private parent directories before opening a connection',async()=>{
  await chmod(root,0o755);
  expect(await runTlsCli(exportArgs(),env)).toBe(64);expect(diagnoseTdsTls).not.toHaveBeenCalled();
  expect(await readdir(root)).toEqual([]);
});
it('refuses symlink parent paths before connecting',async()=>{
  const alias=root+'-alias';await symlink(root,alias);
  try{path=join(alias,'public.pem');expect(await runTlsCli(exportArgs(),env)).toBe(64);expect(diagnoseTdsTls).not.toHaveBeenCalled();}
  finally{await rm(alias);}
});
it('does not publish PEM after failed certificate verification and removes staging files',async()=>{
  const strict=[...args];strict[strict.indexOf('inspect-unverified')]='verify';
  expect(await runTlsCli([...strict,'--export-certificate-pem',path],env)).toBe(1);
  expect(result().code).toBe('CERT_UNTRUSTED');expect(await readdir(root)).toEqual([]);expect(peer.sessions).toHaveLength(1);
});
it('preserves a concurrently created destination and reports export failure without another connection',async()=>{
  state.before=()=>writeFile(path,'concurrent evidence',{mode:0o600});
  expect(await runTlsCli(exportArgs(),env)).toBe(1);expect(result().code).toBe('CERT_EXPORT_FAILED');
  expect(await readFile(path,'utf8')).toBe('concurrent evidence');expect(await readdir(root)).toEqual(['public.pem']);expect(peer.sessions).toHaveLength(1);
});
it('requires an absolute normalized path, explicit authorization, and rejects duplicate options',()=>{
  for(const invalid of [[...args,'--export-certificate-pem','relative.pem'],[...args,'--export-certificate-pem','/tmp/../public.pem'],[...exportArgs(),'--export-certificate-pem',path],exportArgs().slice(2)])
    expect(()=>parseTlsArguments(invalid,env)).toThrow();
});
