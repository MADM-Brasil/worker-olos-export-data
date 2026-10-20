import 'dotenv/config';
import { buildApp } from './app.js';
import { loadConfig } from './config/env.js';
import { createLogger } from './logging.js';
import type { Config } from './config/env.js';
import type { Dependency } from './db/dependency.js';
import type { WorkerRuntime } from './integration/attempts/runtime-control.js';
import { pathToFileURL } from 'node:url';
import { resolve } from 'node:path';

export async function serveWorker(config:Config,dependencies?:Dependency[],runtime?:WorkerRuntime) {
  const logger = createLogger(config);
  const app = buildApp(config,dependencies,runtime);
  let shutdown: Promise<void> | undefined;
  const close = (signal: string): Promise<void> => {
    if (shutdown) return shutdown;
    shutdown = (async () => {
      logger.info({ event: 'shutdown', signal }, 'Encerrando worker');
      const deadline = setTimeout(() => {
        logger.fatal({ event: 'shutdown_timeout' }, 'Prazo de encerramento excedido');
        process.exit(1);
      }, config.SHUTDOWN_TIMEOUT_MS);
      deadline.unref();
      try { await app.close(); clearTimeout(deadline); }
      catch {
        process.exitCode = 1;
        logger.error({ event: 'shutdown_failed' }, 'Falha no encerramento');
        // Manter watchdog se um driver deixou sockets ativos após falhar no close.
      }
    })();
    return shutdown;
  };
  process.on('SIGTERM', () => { void close('SIGTERM'); });
  process.on('SIGINT', () => { void close('SIGINT'); });
  try { await app.listen({ host: config.HTTP_HOST, port: config.HTTP_PORT }); }
  catch { logger.error({ event: 'startup_failed' }, 'Falha ao iniciar HTTP'); process.exitCode = 1; await close('startup_failure'); }
}

if(process.argv[1] && import.meta.url===pathToFileURL(resolve(process.argv[1])).href)void Promise.resolve().then(()=>serveWorker(loadConfig())).catch(() => {
  // Não imprimir erro bruto: drivers e validação podem carregar dados sensíveis.
  process.stderr.write('{"level":"fatal","event":"configuration_or_startup_failed"}\n');
  process.exitCode = 1;
});
