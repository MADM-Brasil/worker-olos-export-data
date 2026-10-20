import { readFile, readdir } from 'node:fs/promises';
import { expect, it } from 'vitest';
import { buildApp } from '../src/app.js';
import { loadConfig } from '../src/config/env.js';

it('boot não executa persistência, migrations ou procedures; sync=true permanece proibido', async () => {
  const app = buildApp(loadConfig({ LOG_LEVEL: 'silent' }), []);
  try {
    await app.listen({ host: '127.0.0.1', port: 0 });
    expect((await app.inject('/health/live')).statusCode).toBe(200);
    expect((await app.inject('/health/ready')).statusCode).toBe(503);
    expect(() => loadConfig({ SYNC_ENABLED: 'true' })).toThrow('SYNC_ENABLED');
  } finally { await app.close(); }
  // Garantia arquitetural: não existe runner nem leitura de SQL/filesystem no código de runtime.
  async function sources(url: URL): Promise<string[]> {
    const files: string[] = [];
    for (const file of await readdir(url, { withFileTypes: true })) {
      const path = new URL(file.name, url);
      if (file.isDirectory()) files.push(...await sources(new URL(`${file.name}/`, url)));
      else if (file.name.endsWith('.ts')) files.push(await readFile(path,'utf8'));
    }
    return files;
  }
  const source = (await sources(new URL('../src/', import.meta.url))).join('\n');
  expect(source).not.toMatch(/from ['"](?:node:)?(?:fs|child_process)/);
  expect(source).not.toMatch(/\.execute\s*\(/);
  expect(source).not.toMatch(/\b(?:CREATE|ALTER|DROP|TRUNCATE)\s+(?:SCHEMA|TABLE|ROLE|FUNCTION)\b/i);
  for (const file of ['app.ts','main.ts']) expect(await readFile(new URL(`../src/${file}`,import.meta.url),'utf8')).not.toContain('/persistence/');
  const pkg = JSON.parse(await readFile(new URL('../package.json',import.meta.url),'utf8'));
  for (const script of ['prestart','postinstall','migrate']) expect(pkg.scripts).not.toHaveProperty(script);
});
