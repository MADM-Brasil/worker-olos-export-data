import { createHmac } from 'node:crypto';
import { z } from 'zod';
import { canonical } from '../src/persistence/validation.js';
import { exceptionalSourceSchema, exceptionalTlsMode, exceptionalTlsRisk } from '../src/olos/tds/disposition-exception.js';

export const exceptionalConfigSchema = z.object({ source: exceptionalSourceSchema.omit({ password: true }),
  collationHex: z.string().regex(/^[a-f0-9]{10}$/).optional() }).strict();
export type ExceptionalConfig = z.infer<typeof exceptionalConfigSchema>;
export interface BindingState { testId: string; container: string; port: number; password: string; snapshotDate: string; }
/** Bind the entire configuration and credential without persisting/logging the Olos password. */
export function exceptionalConfigurationDigest(state: BindingState, config: ExceptionalConfig, password: unknown): string {
  if (typeof password !== 'string' || !password || !state.password) throw new Error('CONTROLLED_INTEGRATION_BLOCKED');
  return createHmac('sha256',state.password).update(canonical({ version: 'phase4-exceptional-tls/v1',config,password,
    destination: { testId: state.testId,container: state.container,port: state.port,snapshotDate: state.snapshotDate,
      host: '127.0.0.1',database: 'phase4_worker_fixture',image: 'postgres:16-bookworm' },
    sourceAction: "ReadData('Disposition')",tlsMode: exceptionalTlsMode,encrypt: true,trustServerCertificate: true,minVersion: 'TLSv1.2',retries: 0 })).digest('hex');
}
export function dispositionTlsMode(flags: Readonly<Record<string,string>>): 'verified' | typeof exceptionalTlsMode {
  const mode = flags['--tls-mode'] ?? 'verified';
  if (mode !== 'verified' && mode !== exceptionalTlsMode ||
    mode === exceptionalTlsMode && flags['--tls-risk-confirmation'] !== exceptionalTlsRisk ||
    mode === 'verified' && (flags['--tls-risk-confirmation'] !== undefined || flags['--operator'] !== undefined)) throw new Error('CONTROLLED_INTEGRATION_BLOCKED');
  return mode;
}
