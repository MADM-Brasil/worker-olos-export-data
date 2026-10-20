import { describe, expect, it, vi } from 'vitest';
import { buildApp } from '../src/app.js';
import { loadConfig } from '../src/config/env.js';
import type { Dependency } from '../src/db/dependency.js';

const token = 'test-only-status-token-32-characters';
const config = () => loadConfig({ LOG_LEVEL: 'silent', STATUS_TOKEN: token });

describe('HTTP e inicialização', () => {
  it('liveness funciona; readiness não promete consumo; status exige autenticação', async () => {
    const app = buildApp(config());
    try {
      expect((await app.inject('/health/live')).statusCode).toBe(200);
      const ready = await app.inject('/health/ready');
      expect(ready.statusCode).toBe(503);
      expect(ready.json()).toMatchObject({ syncEnabled: false, reason: 'acquisition_blocked' });
      expect((await app.inject('/status')).statusCode).toBe(401);
      expect((await app.inject({ url: '/status', headers: { authorization: 'Bearer wrong' } })).statusCode).toBe(401);
      const status = await app.inject({ url: '/status', headers: { authorization: `Bearer ${token}` } });
      expect(status.statusCode).toBe(200);
      expect(status.json().dependencies).toMatchObject({ olos: { state: 'unconfigured' }, postgres: { state: 'unconfigured' } });
      expect(status.body).not.toContain(token);
      expect(status.body).not.toContain('10.196.234.11');
    } finally { await app.close(); }
  });
  it('desabilita status sem token', async () => {
    const app = buildApp(loadConfig({ LOG_LEVEL: 'silent' }));
    try { expect((await app.inject('/status')).statusCode).toBe(404); }
    finally { await app.close(); }
  });
  it('abre listener antes de probe terminar e encerra pools após probe', async () => {
    let finish!: () => void;
    const probe = vi.fn(() => new Promise<void>(resolve => { finish = resolve; }));
    const close = vi.fn(async () => {});
    const dependency: Dependency = { name: 'olos', configured: true, probe, close };
    const app = buildApp(config(), [dependency]);
    try {
      await app.listen({ host: '127.0.0.1', port: 0 });
      expect(probe).toHaveBeenCalledTimes(1);
      expect((await app.inject('/health/live')).statusCode).toBe(200);
      const closing = app.close();
      expect(close).not.toHaveBeenCalled();
      finish();
      await closing;
      expect(close).toHaveBeenCalledTimes(1);
    } finally { finish?.(); await app.close(); }
  });
  it('dependência inacessível não impede HTTP e aparece down no diagnóstico', async () => {
    const dependency: Dependency = { name: 'olos', configured: true,
      probe: vi.fn(async () => { throw new Error('sensitive-driver-message'); }), close: vi.fn(async () => {}) };
    const app = buildApp(config(), [dependency]);
    try {
      await app.listen({ host: '127.0.0.1', port: 0 });
      const status = await app.inject({ url: '/status', headers: { authorization: `Bearer ${token}` } });
      expect(status.json().dependencies.olos.state).toBe('down');
      expect(status.body).not.toContain('sensitive-driver-message');
      expect((await app.inject('/health/live')).statusCode).toBe(200);
      expect((await app.inject('/health/ready')).statusCode).toBe(503);
    } finally { await app.close(); }
  });
  it('bancos saudáveis também não habilitam consumo na Fase 1', async () => {
    const app = buildApp(config(), ['olos', 'postgres'].map(name => ({ name, configured: true,
      probe: async () => {}, close: async () => {} })) as Dependency[]);
    try {
      await app.listen({ host: '127.0.0.1', port: 0 });
      const status = await app.inject({ url: '/status', headers: { authorization: `Bearer ${token}` } });
      expect(status.json().dependencies.olos.state).toBe('up');
      expect(status.json().dependencies.postgres.state).toBe('up');
      expect((await app.inject('/health/ready')).statusCode).toBe(503);
    } finally { await app.close(); }
  });
});
