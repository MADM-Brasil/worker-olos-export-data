import { beforeAll, afterAll, expect, it } from 'vitest';
import { isolatedSqlServer } from './support/sqlserver.js';
import { diagnoseTdsTls } from '../src/diagnostics/tds-tls.js';

let fixture: Awaited<ReturnType<typeof isolatedSqlServer>>;
// Authenticated setup is confined to the fresh disposable Developer container.
// The diagnostic itself has no authentication/database/SQL API or credentials.
beforeAll(async () => { fixture = await isolatedSqlServer(); },120000);
afterAll(async () => { await fixture?.stop(); },30000);

function options() {
  return { host: '127.0.0.1',port: fixture.endpoint.port,serverName: 'localhost',mode: 'verify' as const,ca: fixture.endpoint.ca };
}
it('SQL Server isolado efetivo: PRELOGIN + TLS 1.2 validado, somente metadados do certificado',async () => {
  expect(fixture.version).toMatch(/^16\./);
  const result = await diagnoseTdsTls(options());
  expect(result).toMatchObject({ code: 'VERIFIED',verified: true,chain: 'valid',name: 'match',
    certificate: { subject: 'CN=localhost',issuer: 'CN=phase3d-test-ca' } });
  expect(result.certificate?.fingerprintSha256).toMatch(/^(?:[A-F0-9]{2}:){31}[A-F0-9]{2}$/);
},20000);
it('SQL Server isolado efetivo: cadeia desconhecida bloqueia verify; inspeção continua não validada',async () => {
  const { ca: _,...withoutCa } = options();
  expect(await diagnoseTdsTls(withoutCa)).toMatchObject({ code: 'CERT_UNTRUSTED',verified: false });
  expect(await diagnoseTdsTls({ ...withoutCa,mode: 'inspect-unverified' })).toMatchObject({ code: 'INSPECTED_UNVERIFIED',verified: false,
    certificate: { subject: 'CN=localhost' } });
},30000);
it('SQL Server isolado efetivo: nome DNS incompatível falha sem fallback',async () => {
  expect(await diagnoseTdsTls({ ...options(),serverName: 'wrong.example' })).toMatchObject({ code: 'CERT_NAME_MISMATCH',verified: false });
},20000);
