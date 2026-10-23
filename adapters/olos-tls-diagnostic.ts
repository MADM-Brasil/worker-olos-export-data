import { open, lstat, realpath, link, unlink } from 'node:fs/promises';
import { constants } from 'node:fs';
import { randomUUID } from 'node:crypto';
import { pathToFileURL } from 'node:url';
import { resolve, dirname, basename, join, isAbsolute } from 'node:path';
import { diagnoseTdsTls, type TlsDiagnosticOptions } from '../src/diagnostics/tds-tls.js';

import { loadOlosPinApproval } from '../src/security/olos-certificate-pin.js';

export function parseTlsArguments(args: readonly string[], env: NodeJS.ProcessEnv): { options: TlsDiagnosticOptions; caFile?: string; pinFile?: string; exportFile?: string } {
  if (env.SYNC_ENABLED !== 'false' || env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new Error('INVALID_CONFIGURATION');
  const flags = new Map<string,string>();
  for (let i = 0; i < args.length; i += 2) {
    const key = args[i], value = args[i + 1];
    if (!key || !value || !['--authorize','--mode','--server-name','--ca-file','--pin-approval-file','--export-certificate-pem'].includes(key) || flags.has(key)) throw new Error('INVALID_CONFIGURATION');
    flags.set(key,value);
  }
  if (flags.get('--authorize') !== 'TDS_TLS_METADATA_ONLY') throw new Error('OPERATIONAL_AUTHORIZATION_REQUIRED');
  const mode = flags.get('--mode') ?? 'verify';
  const serverName = flags.get('--server-name');
  if (!['verify','inspect-unverified','verify-pinned','verify-corporate-pinned'].includes(mode) || !serverName || !/^[a-zA-Z0-9][a-zA-Z0-9.-]{0,252}$/.test(serverName)) throw new Error('INVALID_CONFIGURATION');
  const pinned = mode === 'verify-pinned' || mode === 'verify-corporate-pinned';
  if (pinned !== flags.has('--pin-approval-file') || pinned && flags.has('--ca-file')) throw new Error('INVALID_CONFIGURATION');
  const options: TlsDiagnosticOptions = { host: '10.196.234.11', port: 1433, serverName, mode: mode as TlsDiagnosticOptions['mode'], timeoutMs: 10000 };
  const exportFile = flags.get('--export-certificate-pem');
  if (mode === 'verify-corporate-pinned' && exportFile !== undefined) throw new Error('INVALID_CONFIGURATION');
  if (exportFile !== undefined && (!isAbsolute(exportFile) || resolve(exportFile) !== exportFile)) throw new Error('INVALID_CONFIGURATION');
  return { options, ...(flags.has('--ca-file') ? { caFile: flags.get('--ca-file')! } : {}), ...(flags.has('--pin-approval-file') ? { pinFile: flags.get('--pin-approval-file')! } : {}), ...(exportFile ? { exportFile } : {}) };
}
/** Private, exclusive export. Atomic publication cannot overwrite a file created during TLS. */
async function prepareCertificateExport(path: string) {
  const parent = dirname(path), stat = await lstat(parent);
  if (!stat.isDirectory() || stat.isSymbolicLink() || stat.uid !== process.getuid?.() || stat.mode & 0o077 || await realpath(parent) !== parent) throw Error('PRIVATE_EXPORT_DIRECTORY_REQUIRED');
  try { await lstat(path); throw Error('EXPORT_EXISTS'); }
  catch(error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
  const temporary = join(parent,`.${basename(path)}.${randomUUID()}.tmp`);
  const file = await open(temporary,constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW,0o600);
  return {
    save: async (pem: string) => {
      await file.chmod(0o600); await file.writeFile(pem); await file.sync();
      await link(temporary,path);
      const dir = await open(parent,constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
      try { await dir.sync(); } finally { await dir.close(); }
    },
    close: async () => { try { await file.close(); } finally { await unlink(temporary); } },
  };
}
const help = `Diagnóstico manual TDS PRELOGIN/TLS; nunca envia LOGIN7, SQL ou credenciais.
SYNC_ENABLED=false npm run diagnose:olos-tls -- --authorize TDS_TLS_METADATA_ONLY --server-name <nome-esperado> [--mode verify|inspect-unverified|verify-pinned|verify-corporate-pinned] [--ca-file /caminho/ca-publica.pem | --pin-approval-file /caminho/privado/aprovacao.json] [--export-certificate-pem /diretorio/privado/certificado.pem]
Destino fixo: 10.196.234.11:1433. Uma conexão, timeout total 10s, sem retry.
verify-pinned: exige --pin-approval-file /caminho/privado/aprovacao.json; autentica o certificado por pin aprovado, sem LOGIN7/SQL.
verify-corporate-pinned: reutiliza a aprovação OLOS_ATTEMPTS_TDS_ACQUISITION e o PinnedMessageIO corporativo; saída somente code, stage e fingerprintSha256 remoto (ou null). Não exporta certificados.
verify (padrão): exige cadeia confiável, validade e nome compatível.
inspect-unverified: exclusivamente informativo, sem autenticar a identidade do servidor.
Exportação explícita: somente certificado público folha, arquivo novo 0600 em diretório privado existente do usuário; nunca aprova pin e nunca imprime PEM.
Saída: JSON; exit 0=validado, 2=inspeção não validada, 1=falha, 64=configuração/autorização.
Não executar sem aprovação operacional para esta conexão. Não altera os gates Disposition.`;

export async function runTlsCli(args: readonly string[], env: NodeJS.ProcessEnv): Promise<number> {
  if (args.length === 1 && args[0] === '--help') { process.stdout.write(`${help}\n`); return 0; }
  let config: ReturnType<typeof parseTlsArguments>;
  let exporter: Awaited<ReturnType<typeof prepareCertificateExport>> | undefined;
  try {
    config = parseTlsArguments(args, env);
    if (config.pinFile) config.options.pinApproval = await loadOlosPinApproval(config.pinFile);
    if (config.caFile) {
      // Only a public CA bundle is accepted. No dotenv, password, client key or credentials API.
      const file = await open(config.caFile, constants.O_RDONLY | constants.O_NOFOLLOW);
      try {
        const stat = await file.stat();
        if (!stat.isFile() || stat.size > 131072) throw new Error('INVALID_CA_FILE');
        const data = Buffer.alloc(131073);
        const { bytesRead } = await file.read(data, 0, data.length, 0);
        if (bytesRead > 131072) throw new Error('INVALID_CA_FILE');
        config.options.ca = data.subarray(0, bytesRead).toString('utf8');
      } finally { await file.close(); }
    }
    if (config.exportFile) exporter = await prepareCertificateExport(config.exportFile);
  } catch {
    process.stdout.write(`${JSON.stringify(args.includes('verify-corporate-pinned')
      ? {code:'CONFIGURATION_OR_AUTHORIZATION_REJECTED',stage:'AUTHORIZATION_STARTED',fingerprintSha256:null}
      : { code: 'CONFIGURATION_OR_AUTHORIZATION_REJECTED', verified: false })}\n`);
    return 64;
  }
  try {
    const result = await diagnoseTdsTls(config.options,exporter?.save);
    if (config.options.mode === 'verify-corporate-pinned') {
      const diagnostic = result.pinnedDiagnostic;
      process.stdout.write(`${JSON.stringify(diagnostic ?? {code:'DIAGNOSTIC_FAILED',stage:'UNKNOWN',fingerprintSha256:null})}\n`);
      return diagnostic?.code === 'PIN_VERIFIED' ? 0 : 1;
    }
    // Restrict displayed certificate fields; the existing core metadata API retains SAN.
    const certificate = result.certificate && (({subject,issuer,validFrom,validTo,fingerprintSha256}) =>
      ({subject,issuer,validFrom,validTo,fingerprintSha256}))(result.certificate);
    process.stdout.write(`${JSON.stringify({...result,...(certificate ? {certificate} : {})})}\n`);
    return ['VERIFIED','PIN_VERIFIED'].includes(result.code) ? 0 : result.code === 'INSPECTED_UNVERIFIED' ? 2 : 1;
  } finally { await exporter?.close(); }
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  runTlsCli(process.argv.slice(2), process.env).then(code => { process.exitCode = code; }).catch(() => {
    process.stdout.write(`${JSON.stringify(process.argv.includes('verify-corporate-pinned')
      ? {code:'DIAGNOSTIC_FAILED',stage:'UNKNOWN',fingerprintSha256:null}
      : {code:'DIAGNOSTIC_FAILED',verified:false})}\n`); process.exitCode = 1;
  });
}
