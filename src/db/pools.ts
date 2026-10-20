import sql from 'mssql';
import pg from 'pg';
import type { Logger } from 'pino';
import type { Config } from '../config/env.js';
import type { Dependency } from './dependency.js';

export function sqlServerOptions(c: Config): sql.config {
  return {
    server: c.OLOS_HOST, port: c.OLOS_PORT, database: c.OLOS_DATABASE,
    user: c.OLOS_USER, password: c.OLOS_PASSWORD ?? '',
    connectionTimeout: c.OLOS_CONNECT_TIMEOUT_MS, requestTimeout: c.OLOS_REQUEST_TIMEOUT_MS,
    pool: { max: c.OLOS_POOL_MAX, min: 0, idleTimeoutMillis: c.OLOS_IDLE_TIMEOUT_MS, acquireTimeoutMillis: c.OLOS_CONNECT_TIMEOUT_MS },
    options: {
      encrypt: c.OLOS_ENCRYPT, trustServerCertificate: c.OLOS_TRUST_SERVER_CERTIFICATE,
      useUTC: true, cryptoCredentialsDetails: { minVersion: c.OLOS_TLS_MIN_VERSION },
      appName: 'worker-olos-export-data',
    },
  };
}

export function postgresOptions(c: Config): pg.PoolConfig {
  return {
    host: c.PG_HOST, port: c.PG_PORT, database: c.PG_DATABASE,
    user: c.PG_USER, password: c.PG_PASSWORD,
    max: c.PG_POOL_MAX, min: 0, connectionTimeoutMillis: c.PG_CONNECT_TIMEOUT_MS,
    idleTimeoutMillis: c.PG_IDLE_TIMEOUT_MS, query_timeout: c.PG_QUERY_TIMEOUT_MS,
    statement_timeout: c.PG_QUERY_TIMEOUT_MS,
    application_name: 'worker-olos-export-data',
    // Nenhuma escrita ou DDL é permitida nestas conexões de diagnóstico.
    options: '-c default_transaction_read_only=on -c search_path=olos_export',
    ssl: c.PG_SSL_MODE === 'disable' ? false : { rejectUnauthorized: true, ...(c.PG_SSL_CA ? { ca: c.PG_SSL_CA } : {}) },
  };
}

export function createDependencies(c: Config, logger: Logger): Dependency[] {
  // Pools são exclusivos deste processo; nada de pool global do mssql.
  let olos: sql.ConnectionPool | undefined;
  let postgres: pg.Pool | undefined;
  return [{
    name: 'olos', configured: Boolean(c.OLOS_PASSWORD),
    async probe() {
      if (!c.OLOS_PASSWORD) throw new Error('Dependency unconfigured');
      if (!olos) {
        olos = new sql.ConnectionPool(sqlServerOptions(c));
        olos.on('error', () => logger.warn({ dependency: 'olos', event: 'pool_error' }, 'Falha no pool'));
      }
      try {
        await olos.connect();
        await olos.request().query('SELECT 1 AS healthy');
      } catch {
        await olos.close();
        olos = undefined;
        throw new Error('Dependency unavailable');
      }
    },
    async close() { await olos?.close(); },
  }, {
    name: 'postgres', configured: Boolean(c.PG_HOST && c.PG_USER && c.PG_PASSWORD),
    async probe() {
      if (!c.PG_HOST || !c.PG_USER || !c.PG_PASSWORD) throw new Error('Dependency unconfigured');
      if (!postgres) {
        postgres = new pg.Pool(postgresOptions(c));
        postgres.on('error', () => logger.warn({ dependency: 'postgres', event: 'pool_error' }, 'Falha no pool'));
      }
      await postgres.query('SELECT 1 AS healthy');
    },
    async close() { await postgres?.end(); },
  }];
}
