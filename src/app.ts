import { timingSafeEqual, createHash } from 'node:crypto';
import Fastify, { LogController } from 'fastify';
import type { Config } from './config/env.js';
import { createLogger } from './logging.js';
import { createDependencies } from './db/pools.js';
import type { Dependency } from './db/dependency.js';
import { DependencyMonitor } from './health/monitor.js';
import { AttemptsRuntimeControl,type WorkerRuntime } from './integration/attempts/runtime-control.js';
import { attemptsBlockers } from './integration/attempts/contracts.js';

export function buildApp(config: Config, dependencies?: Dependency[],runtime?:WorkerRuntime) {
  const logger = createLogger(config);
  const app = Fastify({ loggerInstance: logger, logController: new LogController({ disableRequestLogging: true }), bodyLimit: 1024,
    requestTimeout: 5000, connectionTimeout: 5000, keepAliveTimeout: 5000, forceCloseConnections: true });
  const monitor = new DependencyMonitor(dependencies ?? createDependencies(config, logger), logger,
    config.DEPENDENCY_CHECK_INTERVAL_MS, config.DEPENDENCY_RETRY_MAX_MS);
  let stopping = false;
  const control = new AttemptsRuntimeControl(config.SYNC_INTERVAL_MS);
  control.block(['ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED',...attemptsBlockers]);
  const acquisition:WorkerRuntime=runtime ?? {start:async()=>{},stop:()=>control.stop(),snapshot:()=>({...control.snapshot(),sourceConfigured:false,sourceAuthorized:false}),ready:()=>false};
  app.setErrorHandler((_error, _request, reply) => {
    logger.warn({ event: 'http_request_failed' }, 'Requisição rejeitada');
    void reply.code(500).send({ error: 'internal_error' });
  });
  app.setNotFoundHandler((_req, reply) => reply.code(404).send({ error: 'not_found' }));
  app.get('/health/live', (_request, reply) => reply.code(stopping ? 503 : 200).send({ status: stopping ? 'stopping' : 'alive' }));
  app.get('/health/ready', (_request, reply) => reply.code(!stopping && acquisition.ready()?200:503).send({
    status: !stopping && acquisition.ready()?'ready':'not_ready', reason: stopping ? 'stopping' : acquisition.ready()?'acquisition_active':'acquisition_blocked', syncEnabled: !stopping && acquisition.ready(),
    dependencies: monitor.snapshot(),
  }));
  app.get('/status', { onRequest: async (request, reply) => {
    if (!config.STATUS_TOKEN) return reply.code(404).send({ error: 'not_found' });
    const supplied = request.headers.authorization ?? '';
    const hash = (value: string) => createHash('sha256').update(value).digest();
    if (!timingSafeEqual(hash(supplied), hash(`Bearer ${config.STATUS_TOKEN}`))) {
      return reply.code(401).header('www-authenticate', 'Bearer').send({ error: 'unauthorized' });
    }
  } }, () => ({
    stopping, sync: {
      ...acquisition.snapshot(),
    },
    dependencies: monitor.snapshot(),
  }));
  app.addHook('onListen', async () => {
    if (config.ALLOW_INSECURE_DB_TLS) logger.warn({ event: 'insecure_tls_exception' }, 'Exceção TLS explicitamente habilitada');
    void acquisition.start().catch(()=>logger.warn({event:'acquisition_start_blocked'},'Aquisição bloqueada'));
    monitor.start(); // Não bloqueia o listen aguardando redes externas.
  });
  app.addHook('preClose', async () => { stopping = true; });
  app.addHook('onClose', async () => {
    const results=await Promise.allSettled([monitor.stop(),acquisition.stop()]);
    if(results.some(r=>r.status==='rejected'))throw Error('SHUTDOWN_FAILED');
  });
  return app;
}
