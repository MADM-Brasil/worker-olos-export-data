import { open } from 'node:fs/promises';
import { constants } from 'node:fs';
import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';
import { diagnoseTdsTls, type TlsDiagnosticOptions } from '../src/diagnostics/tds-tls.js';

import { loadOlosPinApproval } from '../src/security/olos-certificate-pin.js';

export function parseTlsArguments(args: readonly string[], env: NodeJS.ProcessEnv): { options: TlsDiagnosticOptions; caFile?: string; pinFile?: string } {
  if (env.SYNC_ENABLED !== 'false' || env.NODE_TLS_REJECT_UNAUTHORIZED === '0') throw new Error('INVALID_CONFIGURATION');
  const flags = new Map<string,string>();
  for (let i = 0; i < args.length; i += 2) {
    const key = args[i], value = args[i + 1];
    if (!key || !value || !['--authorize','--mode','--server-name','--ca-file','--pin-approval-file'].includes(key) || flags.has(key)) throw new Error('INVALID_CONFIGURATION');
    flags.set(key,value);
  }
  if (flags.get('--authorize') !== 'TDS_TLS_METADATA_ONLY') throw new Error('OPERATIONAL_AUTHORIZATION_REQUIRED');
  const mode = flags.get('--mode') ?? 'verify';
  const serverName = flags.get('--server-name');
  if (!['verify','inspect-unverified','verify-pinned'].includes(mode) || !serverName || !/^[a-zA-Z0-9][a-zA-Z0-9.-]{0,252}$/.test(serverName)) throw new Error('INVALID_CONFIGURATION');
  if ((mode === 'verify-pinned') !== flags.has('--pin-approval-file') || mode === 'verify-pinned' && flags.has('--ca-file')) throw new Error('INVALID_CONFIGURATION');
  const options: TlsDiagnosticOptions = { host: '10.196.234.11', port: 1433, serverName, mode: mode as TlsDiagnosticOptions['mode'], timeoutMs: 10000 };
  return { options, ...(flags.has('--ca-file') ? { caFile: flags.get('--ca-file')! } : {}), ...(flags.has('--pin-approval-file') ? { pinFile: flags.get('--pin-approval-file')! } : {}) };
}
const help = `Diagnóstico manual TDS PRELOGIN/TLS; nunca envia LOGIN7, SQL ou credenciais.
SYNC_ENABLED=false npm run diagnose:olos-tls -- --authorize TDS_TLS_METADATA_ONLY --server-name <nome-esperado> [--mode verify|inspect-unverified|verify-pinned] [--ca-file /caminho/ca-publica.pem | --pin-approval-file /caminho/privado/aprovacao.json]
Destino fixo: 10.196.234.11:1433. Uma conexão, timeout total 10s, sem retry.
verify-pinned: exige --pin-approval-file /caminho/privado/aprovacao.json; autentica o certificado por pin aprovado, sem LOGIN7/SQL.
verify (padrão): exige cadeia confiável, validade e nome compatível.
inspect-unverified: exclusivamente informativo, sem autenticar a identidade do servidor.
Saída: JSON; exit 0=validado, 2=inspeção não validada, 1=falha, 64=configuração/autorização.
Não executar sem aprovação operacional para esta conexão. Não altera os gates Disposition.`;

export async function runTlsCli(args: readonly string[], env: NodeJS.ProcessEnv): Promise<number> {
  if (args.length === 1 && args[0] === '--help') { process.stdout.write(`${help}\n`); return 0; }
  let config: ReturnType<typeof parseTlsArguments>;
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
  } catch {
    process.stdout.write(`${JSON.stringify({ code: 'CONFIGURATION_OR_AUTHORIZATION_REJECTED', verified: false })}\n`);
    return 64;
  }
  const result = await diagnoseTdsTls(config.options);
  process.stdout.write(`${JSON.stringify(result)}\n`);
  return ['VERIFIED','PIN_VERIFIED'].includes(result.code) ? 0 : result.code === 'INSPECTED_UNVERIFIED' ? 2 : 1;
}
if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  runTlsCli(process.argv.slice(2), process.env).then(code => { process.exitCode = code; }).catch(() => {
    process.stdout.write('{"code":"DIAGNOSTIC_FAILED","verified":false}\n'); process.exitCode = 1;
  });
}
