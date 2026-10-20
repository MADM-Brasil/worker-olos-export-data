import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { mkdtemp, rm, readFile, writeFile, stat, symlink } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { LocalSpool, type StorageFaults } from '../adapters/local-spool.js';
import { spoolLimits } from '../src/large-batch/config.js';
import { decodeWireRow } from '../src/large-batch/wire.js';
import { largePlan, rawRow, spoolIdentity } from './support/large-batch-fixture.js';
import type { SpoolManifest } from '../src/large-batch/types.js';

let root: string;
beforeEach(async () => { root = await mkdtemp(join(tmpdir(),'olos-spool-test-')); });
afterEach(async () => { await rm(root,{ recursive: true, force: true }); });
const limits = () => spoolLimits({ minFreeBytes: 0, bufferBytes: 1024 });
async function sealed(repo: LocalSpool) {
  const identity = spoolIdentity(), writer = await repo.create(identity);
  await writer.append(decodeWireRow(identity.plan,rawRow(identity.plan)));
  await writer.append(decodeWireRow(identity.plan,rawRow(identity.plan,2)));
  return writer.seal();
}
async function drain(repo: LocalSpool,m: SpoolManifest) { const rows = []; for await (const row of repo.records(m)) rows.push(row); return rows; }
describe('spool POSIX opcional, privado e durável por selo', () => {
  it('fsync/rename produz resposta completa verificável e mantém os dados depois da confirmação', async () => {
    const repo = await LocalSpool.open(root,limits()), m = await sealed(repo);
    expect(m).toMatchObject({ sealed: true, state: 'sealed', rows: 2 }); expect(m.sha256).toMatch(/^[a-f0-9]{64}$/);
    expect((await drain(repo,m))).toHaveLength(2);
    expect((await stat(join(root,'batches',m.identity.batchId,'records.ready'))).mode & 0o777).toBe(0o600);
    await repo.mark(m,'committed'); expect(await repo.active()).toBeNull();
    expect((await drain(repo,m))).toHaveLength(2); // retenção; nenhum delete automático do payload
    expect(repo.metrics.peakBufferedBytes).toBeLessThanOrEqual(limits().bufferBytes);
  });
  it('dois processos/instâncias não criam dois lotes no mesmo volume', async () => {
    const a = await LocalSpool.open(root,limits()), b = await LocalSpool.open(root,limits());
    const writer = await a.create(spoolIdentity());
    await expect(b.create(spoolIdentity())).rejects.toMatchObject({ code: 'BUSY' }); await writer.interrupt();
    const restarted = await LocalSpool.open(root,limits()); expect((await restarted.active())?.state).toBe('recovery_required');
  });
  it('prefixo interrompido conserva identidade e jamais é considerado lote pronto', async () => {
    const repo = await LocalSpool.open(root,limits()), id = spoolIdentity(), writer = await repo.create(id);
    await writer.append(decodeWireRow(id.plan,rawRow(id.plan))); await writer.interrupt();
    const m = (await repo.active())!; expect(m.identity).toEqual(id); expect(m.sealed).toBe(false);
    await expect(drain(repo,m)).rejects.toMatchObject({ code: 'INCOMPLETE' });
  });
  it.each(['write','sync','rename'] as const)('falha %s antes do selo não promove resposta parcial', async operation => {
    let armed = false;
    const faults: StorageFaults = { async before(op) { if (armed && op === operation) throw Object.assign(new Error('private disk detail'),{ code: 'ENOSPC' }); } };
    const repo = await LocalSpool.open(root,limits(),faults), id = spoolIdentity(), writer = await repo.create(id);
    await writer.append(decodeWireRow(id.plan,rawRow(id.plan))); armed = true;
    await expect(writer.seal()).rejects.toMatchObject({ code: 'DISK_FULL' });
    await writer.interrupt();
    const reopened = await LocalSpool.open(root,limits()); expect((await reopened.active())?.sealed).toBe(false);
  });
  it('quota de disco inclui retenção e bloqueia antes de continuar recebendo', async () => {
    const repo = await LocalSpool.open(root,spoolLimits({ maxDiskBytes: 256 * 1024, minFreeBytes: 0, bufferBytes: 1024 }));
    const id = spoolIdentity(), writer = await repo.create(id), row = decodeWireRow(id.plan,rawRow(id.plan));
    await expect((async () => { for (let i = 0; i < 1000; i++) await writer.append(row); })()).rejects.toMatchObject({ code: 'QUOTA_EXCEEDED' });
    await writer.interrupt(); expect((await repo.active())?.sealed).toBe(false);
  });
  it('limite de registros e de linha rejeita excesso sem truncamento', async () => {
    const repo = await LocalSpool.open(root,spoolLimits({ minFreeBytes: 0, maxRows: 1 })), id = spoolIdentity(), writer = await repo.create(id);
    const row = decodeWireRow(id.plan,rawRow(id.plan)); await writer.append(row);
    await expect(writer.append(row)).rejects.toMatchObject({ code: 'QUOTA_EXCEEDED' }); await writer.interrupt();
  });
  it('detecta alteração e truncamento, inclusive alteração que conserva o tamanho do arquivo', async () => {
    const repo = await LocalSpool.open(root,limits()), m = await sealed(repo), path = join(root,'batches',m.identity.batchId,'records.ready');
    const body = await readFile(path); body[body.indexOf(Buffer.from('"X"'))+1] = 89; await writeFile(path,body);
    await expect(drain(repo,m)).rejects.toMatchObject({ code: 'CORRUPT' });
    await writeFile(path,body.subarray(0,body.length-1)); await expect(drain(repo,m)).rejects.toMatchObject({ code: 'CORRUPT' });
  });
  it('não libera claim de uma identidade usando outra identidade', async () => {
    const repo = await LocalSpool.open(root,limits()), m = await sealed(repo);
    await expect(repo.mark({ ...m, identity: spoolIdentity() },'committed')).rejects.toMatchObject({ code: 'BUSY' });
    expect((await repo.active())?.identity.batchId).toBe(m.identity.batchId);
  });
  it('rejeita symlinks e payload com senha mesmo em uso direto do adaptador', async () => {
    const alias = `${root}-alias`; await symlink(root,alias);
    try { await expect(LocalSpool.open(alias,limits())).rejects.toMatchObject({ code: 'CORRUPT' }); }
    finally { await rm(alias); }
    const repo = await LocalSpool.open(root,limits()), id = spoolIdentity(), writer = await repo.create(id);
    await expect(writer.append({ ...decodeWireRow(id.plan,rawRow(id.plan)), Password: 'never store' })).rejects.toMatchObject({ code: 'CORRUPT' });
    await writer.interrupt();
    expect((await readFile(join(root,'batches',id.batchId,'records.partial'),'utf8'))).not.toContain('never store');
  });
});
