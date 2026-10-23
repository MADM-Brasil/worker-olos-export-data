import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { mkdtemp, readFile, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { createRequire } from 'node:module';
import { createServer, type Socket } from 'node:net';
import { TLSSocket, createSecureContext, type SecureVersion } from 'node:tls';
import type { Duplex } from 'node:stream';
const exec = promisify(execFile);
const Pair = createRequire(import.meta.url)('native-duplexpair') as new () => { socket1: Duplex; socket2: Duplex };

export async function syntheticCertificates() {
  const directory = await mkdtemp(join(tmpdir(),'olos-tls-diagnostic-'));
  try {
    await exec('openssl',['req','-x509','-newkey','rsa:2048','-nodes','-sha256','-days','2','-subj','/CN=diagnostic-synthetic-ca',
      '-keyout',join(directory,'ca.key'),'-out',join(directory,'ca.crt')]);
    await exec('openssl',['req','-new','-newkey','rsa:2048','-nodes','-subj','/CN=localhost',
      '-keyout',join(directory,'server.key'),'-out',join(directory,'server.csr')]);
    await writeFile(join(directory,'extensions.cnf'),'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n');
    for (const [file,days] of [['server.crt','2'],['expired.crt','-1']]) {
      await exec('openssl',['x509','-req','-in',join(directory,'server.csr'),'-CA',join(directory,'ca.crt'),'-CAkey',join(directory,'ca.key'),
        '-CAcreateserial','-out',join(directory,file!),'-days',days!,'-sha256','-extfile',join(directory,'extensions.cnf')]);
    }
    await exec('openssl',['req','-x509','-newkey','rsa:2048','-nodes','-sha256','-days','2','-subj','/CN=synthetic-fallback',
      '-addext','basicConstraints=critical,CA:FALSE','-addext','extendedKeyUsage=serverAuth',
      '-keyout',join(directory,'self.key'),'-out',join(directory,'self.crt')]);
    // A self-signed server leaf is not a certificate-signing CA. This keyUsage
    // distinguishes it from the older fixture, which had no keyUsage restriction.
    await exec('openssl',['req','-new','-key',join(directory,'self.key'),'-subj','/CN=SSL_Self_Signed_Fallback',
      '-out',join(directory,'leaf.csr')]);
    await writeFile(join(directory,'leaf.cnf'),'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\n');
    for(const [file,days] of [['leaf.crt','2'],['expired-leaf.crt','-1']]){
      await exec('openssl',['x509','-req','-in',join(directory,'leaf.csr'),'-signkey',join(directory,'self.key'),
        '-out',join(directory,file!),'-days',days!,'-sha256','-extfile',join(directory,'leaf.cnf')]);
    }
    // A cryptographically self-signed leaf with a nonmatching issuer lookup hint:
    // the approval verifies the signature, but ordinary chain building fails.
    await writeFile(join(directory,'direct-leaf.cnf'),'basicConstraints=critical,CA:FALSE\nkeyUsage=critical,digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth\nsubjectKeyIdentifier=hash\n2.5.29.35=DER:30:16:80:14:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00:00\n');
    await exec('openssl',['x509','-req','-in',join(directory,'leaf.csr'),'-signkey',join(directory,'self.key'),
      '-out',join(directory,'direct-leaf.crt'),'-days','2','-sha256','-extfile',join(directory,'direct-leaf.cnf')]);
    return { selfSigned: await readFile(join(directory,'self.crt'),'utf8'), selfKey: await readFile(join(directory,'self.key'),'utf8'),
      serverLeaf:await readFile(join(directory,'leaf.crt'),'utf8'),expiredServerLeaf:await readFile(join(directory,'expired-leaf.crt'),'utf8'),
      directTrustLeaf:await readFile(join(directory,'direct-leaf.crt'),'utf8'),
      ca: await readFile(join(directory,'ca.crt'),'utf8'), caKey: await readFile(join(directory,'ca.key'),'utf8'), cert: await readFile(join(directory,'server.crt'),'utf8'),
      expired: await readFile(join(directory,'expired.crt'),'utf8'), key: await readFile(join(directory,'server.key'),'utf8'),
      stop: () => rm(directory,{ recursive: true, force: true }) };
  } catch (error) { await rm(directory,{ recursive: true, force: true }); throw error; }
}

/** Synthetic PRELOGIN/TLS peer, not a SQL Server. Records every decrypted client byte. */
export async function syntheticTdsTls(options: { cert: string; key: string; minVersion?: SecureVersion;
  response?: Buffer; packetType?: number; silent?: boolean; disconnect?: boolean; flood?: boolean; fragmented?: boolean;
  stallTls?: boolean; disconnectTls?: boolean; handshakePacketType?: number; multiPacket?: boolean; handshakeResponse?: Buffer; invalidHandshakeHeader?: boolean }) {
  const sockets = new Set<Socket>();
  const sessions: { types: number[]; applicationBytes: number; secure: boolean }[] = [];
  const server = createServer(socket => {
    sockets.add(socket);
    const session = { types: [] as number[], applicationBytes: 0, secure: false };
    sessions.push(session);
    let buffer = Buffer.alloc(0), first = true, raw = false;
    let pair: InstanceType<typeof Pair> | undefined, tls: TLSSocket | undefined;
    const packet = (body: Buffer, type: number) => {
      if(options.multiPacket && body.length>1){
        const middle=Math.floor(body.length/2);
        const parts=[body.subarray(0,middle),body.subarray(middle)];
        const packets=parts.map((part,i)=>{const h=Buffer.alloc(8);h[0]=type;h[1]=i===1?1:0;h.writeUInt16BE(part.length+8,2);h[6]=i+1;return Buffer.concat([h,part]);});
        for(const bytes of packets){socket.write(bytes.subarray(0,3));socket.write(bytes.subarray(3));}return;
      }
      const header = Buffer.alloc(8); header[0] = type; header[1] = 1; header.writeUInt16BE(body.length+8,2); header[6] = 1;
      const data = Buffer.concat([header,body]);
      if (options.fragmented) { socket.write(data.subarray(0,3)); setImmediate(() => socket.write(data.subarray(3))); }
      else socket.write(data);
    };
    socket.on('error', () => {});
    socket.on('close', () => { sockets.delete(socket); tls?.destroy(); pair?.socket2.destroy(); });
    socket.on('data', data => {
      if (raw) { pair?.socket2.write(data); return; }
      buffer = Buffer.concat([buffer,data]);
      while (buffer.length >= 8 && !raw) {
        const length = buffer.readUInt16BE(2);
        if (length < 8) { socket.destroy(); return; }
        if (buffer.length < length) break;
        const type = buffer[0]!, body = buffer.subarray(8,length);
        session.types.push(type); buffer = buffer.subarray(length);
        if (first) {
          first = false;
          if (options.disconnect) { socket.destroy(); return; }
          if (options.silent) return;
          if (options.flood) {
            const incomplete = Buffer.alloc(8192); incomplete[0] = 4; incomplete.writeUInt16BE(incomplete.length,2);
            socket.write(Buffer.concat(Array.from({ length: 40 }, () => incomplete))); return;
          }
          // ENCRYPT_ON, independent minimum PRELOGIN table with one option.
          packet(options.response ?? Buffer.from([1,0,6,0,1,255,1]), options.packetType ?? 4);
          if (options.stallTls || options.disconnectTls) return;
          pair = new Pair();
          tls = new TLSSocket(pair.socket1,{ isServer: true, secureContext: createSecureContext({
            cert: options.cert, key: options.key, minVersion: options.minVersion ?? 'TLSv1.2',
            maxVersion: options.minVersion ?? 'TLSv1.2' }) });
          tls.on('error', () => {});
          tls.on('secure', () => { session.secure = true; });
          tls.on('data', bytes => { session.applicationBytes += bytes.length; });
          pair.socket2.on('error', () => {});
          pair.socket2.on('data', encrypted => {
            if (raw) socket.write(encrypted);
            else if(options.invalidHandshakeHeader){const header=Buffer.alloc(8);header[0]=4;header.writeUInt16BE(7,2);socket.write(header);}
            else packet(options.handshakeResponse??encrypted,options.handshakePacketType??4);
          });
        } else {
          if (options.disconnectTls) { socket.destroy(); return; }
          // Client handshake fragments are PRELOGIN, never LOGIN7/SQL/RPC.
          pair?.socket2.write(body);
          // TLS server has sent Finished when its secure event fires. The client
          // will switch to raw TLS after receiving that TDS-wrapped response.
          if (session.secure) raw = true;
        }
      }
      if (raw && buffer.length) { pair?.socket2.write(buffer); buffer = Buffer.alloc(0); }
    });
  });
  await new Promise<void>(resolve => server.listen(0,'127.0.0.1',resolve));
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('FIXTURE_LISTEN_FAILED');
  return { port: address.port, sessions, stop: async () => {
    for (const socket of sockets) socket.destroy();
    await new Promise<void>(resolve => server.close(() => resolve()));
  } };
}
