import { expect, it } from 'vitest';
import { loadConfig } from '../src/config/env.js';
import { configuredPersistenceLimits, defaultLimits } from '../src/persistence/types.js';

it('limites da persistência são centralizados e configuráveis sem habilitar sync', () => {
  expect(configuredPersistenceLimits(loadConfig({}))).toEqual(defaultLimits);
  const config = loadConfig({ PERSISTENCE_MAX_RECORDS: '10', PERSISTENCE_CHUNK_SIZE: '2', PERSISTENCE_MAX_BYTES: '1024', PERSISTENCE_LEASE_MS: '30000' });
  expect(configuredPersistenceLimits(config)).toEqual({ maxRecords: 10,chunkSize: 2,maxBytes: 1024,leaseMs: 30000 });
  expect(config.SYNC_ENABLED).toBe(false);
  expect(() => loadConfig({ PERSISTENCE_MAX_RECORDS: '1' })).toThrow('PERSISTENCE_CHUNK_SIZE');
  expect(() => loadConfig({ PERSISTENCE_LEASE_MS: '0' })).toThrow('PERSISTENCE_LEASE_MS');
});
