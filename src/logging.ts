import pino, { type DestinationStream } from 'pino';
import type { Config } from './config/env.js';

export function createLogger(config: Config, destination?: DestinationStream) {
  return pino({
    level: config.LOG_LEVEL,
    base: { service: 'worker-olos-export-data' },
    // Também protege logs internos do Fastify em erros de transporte/parser.
    serializers: {
      err: () => ({ type: 'Error' }),
      req: () => ({ redacted: true }),
    },
    redact: {
      paths: ['password', 'token', 'authorization', 'req.headers.authorization', '*.password', '*.token',
        'OLOS_PASSWORD', 'PG_PASSWORD', 'STATUS_TOKEN', 'PG_SSL_CA', 'config', 'env'],
      censor: '[REDACTED]',
    },
  }, destination);
}
