import { expect, it } from 'vitest';
import { createLogger } from '../src/logging.js';
import { loadConfig } from '../src/config/env.js';

it('omite credenciais, URL, IP e erro bruto mesmo em logs internos', () => {
  let output = '';
  const logger = createLogger(loadConfig({}), { write: chunk => { output += chunk; } });
  const secret = 'sensitive-test-marker';
  logger.error({
    err: new Error(secret), req: { url: `/?token=${secret}`, ip: secret, headers: { authorization: secret } },
    password: secret, token: secret, authorization: secret, nested: { password: secret },
    OLOS_PASSWORD: secret, PG_PASSWORD: secret, STATUS_TOKEN: secret, config: { anything: secret }, env: { anything: secret },
  }, 'Falha');
  expect(output).not.toContain(secret);
  expect(JSON.parse(output)).toMatchObject({ err: { type: 'Error' }, req: { redacted: true }, password: '[REDACTED]' });
  expect(output).not.toContain('stack');
});
