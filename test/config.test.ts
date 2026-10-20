import { describe, expect, it } from 'vitest';
import { loadConfig } from '../src/config/env.js';
import { postgresOptions, sqlServerOptions } from '../src/db/pools.js';

describe('configuração segura', () => {
  it('inicia sem segredos e bloqueia sincronização', () => {
    const config = loadConfig({});
    expect(config.SYNC_ENABLED).toBe(false);
    expect(config.SYNC_INTERVAL_MS).toBe(60_000);
    expect(config.OLOS_PASSWORD).toBeUndefined();
    expect(config.PG_HOST).toBeUndefined();
    expect(() => loadConfig({ SYNC_ENABLED: 'true' })).toThrow('SYNC_ENABLED');
  });
  it.each([{ SYNC_ENABLED: 'yes' }, { HTTP_PORT: '0' }, { PG_SCHEMA: 'public' },
    { PG_HOST: 'example.invalid' }, { STATUS_TOKEN: 'short' }, { OLOS_POOL_MAX: '10000' },
    { OLOS_ENCRYPT: 'false' }, { OLOS_TRUST_SERVER_CERTIFICATE: 'true' },
    { OLOS_TLS_MIN_VERSION: 'TLSv1' }, { PG_SSL_MODE: 'disable' },
    { DEPENDENCY_RETRY_MAX_MS: '1000' }])('rejeita configuração inválida %o', env => {
    expect(() => loadConfig(env)).toThrow('Configuração inválida');
  });
  it('reconhece false literal e exige exceção TLS explícita', () => {
    const c = loadConfig({ OLOS_ENCRYPT: 'false', ALLOW_INSECURE_DB_TLS: 'true' });
    expect(c.OLOS_ENCRYPT).toBe(false);
    expect(sqlServerOptions(c).options?.encrypt).toBe(false);
  });
  it('não inclui valores secretos em erros de validação', () => {
    const secret = 'private-marker';
    expect(() => loadConfig({ STATUS_TOKEN: secret })).toThrow('STATUS_TOKEN');
    try { loadConfig({ STATUS_TOKEN: secret }); } catch (error) { expect(String(error)).not.toContain(secret); }
  });
  it('configura pools limitados, TLS e sessões PG somente leitura', () => {
    const c = loadConfig({});
    expect(sqlServerOptions(c)).toMatchObject({ pool: { min: 0, max: 2 }, connectionTimeout: 5000,
      requestTimeout: 5000, options: { encrypt: true, trustServerCertificate: false, cryptoCredentialsDetails: { minVersion: 'TLSv1.2' } } });
    expect(postgresOptions(c)).toMatchObject({ max: 3, connectionTimeoutMillis: 5000,
      query_timeout: 5000, statement_timeout: 5000, ssl: { rejectUnauthorized: true } });
    expect(postgresOptions(c).options).toContain('default_transaction_read_only=on');
    expect(postgresOptions(c).options).toContain('search_path=olos_export');
  });
});
