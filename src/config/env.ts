import { z } from 'zod';

const flag = (fallback: boolean) => z.enum(['true', 'false']).default(String(fallback) as 'true' | 'false').transform(v => v === 'true');
const integer = (fallback: number, max = 300_000) => z.coerce.number().int().min(1).max(max).default(fallback);
const optional = z.preprocess(v => v === '' ? undefined : v, z.string().min(1).optional());

const schema = z.object({
  NODE_ENV: z.enum(['development', 'test', 'production']).default('development'),
  HTTP_HOST: z.string().min(1).default('127.0.0.1'),
  HTTP_PORT: integer(3000, 65535),
  LOG_LEVEL: z.enum(['fatal', 'error', 'warn', 'info', 'debug', 'trace', 'silent']).default('info'),
  STATUS_TOKEN: z.preprocess(v => v === '' ? undefined : v, z.string().min(32).max(256).optional()),
  SYNC_ENABLED: flag(false),
  SYNC_INTERVAL_MS: integer(60_000),
  DEPENDENCY_CHECK_INTERVAL_MS: integer(60_000),
  DEPENDENCY_RETRY_MAX_MS: integer(300_000, 3_600_000),
  SHUTDOWN_TIMEOUT_MS: integer(20_000),
  PERSISTENCE_MAX_RECORDS: integer(1000, 100_000),
  PERSISTENCE_MAX_BYTES: integer(4_194_304, 67_108_864),
  PERSISTENCE_CHUNK_SIZE: integer(200, 100_000),
  PERSISTENCE_LEASE_MS: integer(60_000),
  ALLOW_INSECURE_DB_TLS: flag(false),
  OLOS_HOST: z.string().min(1).default('10.196.234.11'),
  OLOS_PORT: integer(1433, 65535),
  OLOS_DATABASE: z.string().min(1).default('ExportDataV3'),
  OLOS_USER: z.string().min(1).default('OEC'),
  OLOS_PASSWORD: optional,
  OLOS_ENCRYPT: flag(true),
  OLOS_TRUST_SERVER_CERTIFICATE: flag(false),
  OLOS_TLS_MIN_VERSION: z.enum(['TLSv1', 'TLSv1.1', 'TLSv1.2', 'TLSv1.3']).default('TLSv1.2'),
  OLOS_POOL_MAX: integer(2, 20),
  OLOS_CONNECT_TIMEOUT_MS: integer(5000),
  OLOS_REQUEST_TIMEOUT_MS: integer(5000),
  OLOS_IDLE_TIMEOUT_MS: integer(30_000),
  PG_HOST: optional,
  PG_PORT: integer(5432, 65535),
  PG_DATABASE: z.string().min(1).default('hubspot_olos'),
  PG_USER: optional,
  PG_PASSWORD: optional,
  PG_SCHEMA: z.literal('olos_export').default('olos_export'),
  PG_SSL_MODE: z.enum(['verify-full', 'disable']).default('verify-full'),
  PG_SSL_CA: optional,
  PG_POOL_MAX: integer(3, 20),
  PG_CONNECT_TIMEOUT_MS: integer(5000),
  PG_QUERY_TIMEOUT_MS: integer(5000),
  PG_IDLE_TIMEOUT_MS: integer(30_000),
}).superRefine((env, ctx) => {
  if (env.SYNC_ENABLED) ctx.addIssue({ code: 'custom', path: ['SYNC_ENABLED'], message: 'Consumo indisponível nesta fase' });
  if (env.PERSISTENCE_CHUNK_SIZE > env.PERSISTENCE_MAX_RECORDS) ctx.addIssue({ code: 'custom', path: ['PERSISTENCE_CHUNK_SIZE'], message: 'Parcela maior que lote' });
  if (!env.ALLOW_INSECURE_DB_TLS && (!env.OLOS_ENCRYPT || env.OLOS_TRUST_SERVER_CERTIFICATE || ['TLSv1', 'TLSv1.1'].includes(env.OLOS_TLS_MIN_VERSION) || env.PG_SSL_MODE === 'disable')) {
    ctx.addIssue({ code: 'custom', path: ['ALLOW_INSECURE_DB_TLS'], message: 'Exceção TLS exige autorização explícita por configuração' });
  }
  const pgFields = [env.PG_HOST, env.PG_USER, env.PG_PASSWORD];
  if (pgFields.some(Boolean) && !pgFields.every(Boolean)) ctx.addIssue({ code: 'custom', path: ['PG_HOST'], message: 'Configuração PostgreSQL incompleta' });
  if (env.DEPENDENCY_RETRY_MAX_MS < env.DEPENDENCY_CHECK_INTERVAL_MS) ctx.addIssue({ code: 'custom', path: ['DEPENDENCY_RETRY_MAX_MS'], message: 'Máximo deve ser maior ou igual ao intervalo' });
});

export type Config = z.infer<typeof schema>;
export function loadConfig(env: NodeJS.ProcessEnv = process.env): Config {
  const result = schema.safeParse(env);
  // Nunca incluir valores recebidos ou mensagens do Zod no erro público/log.
  if (!result.success) throw new Error(`Configuração inválida: ${[...new Set(result.error.issues.map(i => i.path.join('.')))].join(', ')}`);
  return result.data;
}
