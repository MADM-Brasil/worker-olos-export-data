import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { randomUUID } from 'node:crypto';
import { mkdtemp, writeFile, readFile, rm, mkdir } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import sql from 'mssql';
import type { IsolatedEndpoint } from '../../src/olos/tds/source.js';
const exec = promisify(execFile);
export const sqlImage = 'mcr.microsoft.com/mssql/server@sha256:cf8f0ab2ebe61feec2eed2629061a07d247f791c2a8ddda74a4ac9f06a291718';
/** Cria seu próprio SQL Server Developer. Sem hosts, credenciais ou URLs de ambiente. Sem fallback. */
export async function isolatedSqlServer(options: { selfSignedFallback?: boolean } = {}) {
  let stage = 'certificate';
  let id: string | undefined, admin: sql.ConnectionPool | undefined;
  const directory = await mkdtemp(join(tmpdir(),'olos-tds-tls-'));
  const password = `Isolated!7${randomUUID()}`;
  const stop = async () => { await admin?.close(); if (id) await exec('docker',['rm','-f',id]); await rm(directory,{ recursive: true, force: true }); };
  try {
    await mkdir(join(directory,'test-tls'));
    await exec('openssl',['req','-x509','-newkey','rsa:2048','-nodes','-sha256','-days','2','-subj','/CN=phase3d-test-ca',
      '-keyout',join(directory,'ca.key'),'-out',join(directory,'ca.crt')]);
    await exec('openssl',['req','-new','-newkey','rsa:2048','-nodes','-subj','/CN=localhost',
      '-keyout',join(directory,'test-tls/server.key'),'-out',join(directory,'server.csr')]);
    await writeFile(join(directory,'extensions.cnf'),'subjectAltName=DNS:localhost,IP:127.0.0.1\nextendedKeyUsage=serverAuth\n');
    await exec('openssl',['x509','-req','-in',join(directory,'server.csr'),'-CA',join(directory,'ca.crt'),'-CAkey',join(directory,'ca.key'),
      '-CAcreateserial','-out',join(directory,'test-tls/server.crt'),'-days','2','-sha256','-extfile',join(directory,'extensions.cnf')]);
    if (options.selfSignedFallback) await exec('openssl',['req','-x509','-new','-key',join(directory,'test-tls/server.key'),
      '-out',join(directory,'test-tls/server.crt'),'-days','2','-sha256','-subj','/CN=SSL_Self_Signed_Fallback','-addext','extendedKeyUsage=serverAuth']);
    await writeFile(join(directory,'mssql.conf'),'[network]\nforceencryption=1\ntlscert=/var/opt/mssql/test-tls/server.crt\ntlskey=/var/opt/mssql/test-tls/server.key\ntlsprotocols=1.2\n');
    stage = 'create';
    const container = await exec('docker',['create','--label','worker-olos-export-data.test=phase3d','--user','0',
      '--memory','3g','--cpus','2','-p','127.0.0.1::1433','-e','ACCEPT_EULA=Y','-e','MSSQL_PID=Developer','-e','MSSQL_SA_PASSWORD',
      '--entrypoint','/bin/bash',sqlImage,'-c','chown -R 10001:0 /var/opt/mssql/test-tls; chmod 700 /var/opt/mssql/test-tls; chmod 600 /var/opt/mssql/test-tls/server.key; exec runuser -u mssql -- /opt/mssql/bin/sqlservr'],
      { env: { ...process.env, MSSQL_SA_PASSWORD: password }, timeout: 120000 });
    id = container.stdout.trim(); if (!/^[a-f0-9]{64}$/.test(id)) throw new Error('INVALID_CONTAINER');
    stage = 'copy';
    await exec('docker',['cp',join(directory,'test-tls'),`${id}:/var/opt/mssql/test-tls`]);
    await exec('docker',['cp',join(directory,'mssql.conf'),`${id}:/var/opt/mssql/mssql.conf`]);
    stage = 'start'; await exec('docker',['start',id]);
    stage = 'port'; const port = Number((await exec('docker',['inspect','--format','{{(index (index .NetworkSettings.Ports "1433/tcp") 0).HostPort}}',id])).stdout.trim());
    if (!Number.isInteger(port) || port < 1024 || port > 65535) throw new Error('INVALID_PORT');
    const ca = await readFile(join(directory,'ca.crt'),'utf8');
    // Administrative fixture setup stays verified even when the leaf is self-signed.
    const adminCa = options.selfSignedFallback ? await readFile(join(directory,'test-tls/server.crt'),'utf8') : ca;
    const config: sql.config = { server: '127.0.0.1',port,user: 'sa',password,database: 'master',connectionTimeout: 1000,requestTimeout: 60000,
      options: { encrypt: true,trustServerCertificate: false,serverName: options.selfSignedFallback ? 'SSL_Self_Signed_Fallback' : 'localhost',cryptoCredentialsDetails: { ca: adminCa, minVersion: 'TLSv1.2' },useUTC: true }, pool: { min: 0,max: 1 } };
    stage = 'connect'; let ready = false;
    for (let i=0; i<100; i++) {
      admin = new sql.ConnectionPool(config);
      try { await admin.connect(); ready = true; break; } catch { await admin.close(); const state = (await exec('docker',['inspect','--format','{{.State.Running}}',id])).stdout.trim(); if (state !== 'true') break; await new Promise(r => setTimeout(r,300)); }
    }
    if (!ready || !admin) throw new Error('ISOLATED_SQLSERVER_UNAVAILABLE');
    await admin.request().batch('CREATE DATABASE phase3d_fixture'); await admin.close();
    admin = await new sql.ConnectionPool({ ...config,database: 'phase3d_fixture' }).connect();
    const endpoint: IsolatedEndpoint = { server: '127.0.0.1',port,database: 'phase3d_fixture',user: 'sa',password,ca };
    const version = (await admin.request().query('SELECT CAST(SERVERPROPERTY(\'ProductVersion\') AS varchar(128)) AS version')).recordset[0].version as string;
    return { admin,endpoint,version,stop,
      /** Somente a instância criada neste helper, para ensaiar perda efetiva da rede/processo. */
      disconnect: async () => { await exec('docker',['stop','--time','0',id!]); } };
  } catch {
    if (id) { const logs = await exec('docker',['logs',id]).catch(() => ({ stdout: '',stderr: '' })); await writeFile('/tmp/phase3d-fixture-diagnostic.log',logs.stdout+logs.stderr,{ mode: 0o600 }); }
    await stop(); throw new Error(`Fixture stage: ${stage}. `+'SQL Server isolado indisponível; nenhuma conexão externa ou simulação substitui este teste');
  }
}
