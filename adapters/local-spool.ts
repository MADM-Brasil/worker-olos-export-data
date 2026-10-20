import * as fs from 'node:fs/promises';
import { constants } from 'node:fs';
import { createHash, randomUUID } from 'node:crypto';
import { isAbsolute, join, resolve } from 'node:path';
import type { SpoolIdentity, SpoolLimits, SpoolManifest, SpoolRecord, SpoolRepository, SpoolWriter } from '../src/large-batch/types.js';

type Operation = 'write' | 'sync' | 'rename' | 'manifest' | 'read';
export interface StorageFaults { before(operation: Operation): Promise<void>; }
class StorageError extends Error {
  constructor(readonly code: 'BUSY' | 'STORAGE_FAILED' | 'DISK_FULL' | 'QUOTA_EXCEEDED' | 'CORRUPT' | 'INCOMPLETE') { super(code); }
}
const uuid = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/i;
function safe(error: unknown): StorageError {
  if (error instanceof StorageError) return error;
  const code = typeof error === 'object' && error !== null && 'code' in error ? error.code : '';
  return new StorageError(code === 'ENOSPC' || code === 'EDQUOT' ? 'DISK_FULL' : code === 'EEXIST' ? 'BUSY' : 'STORAGE_FAILED');
}
/** Adaptador POSIX opcional. Não é importado pelo núcleo/boot; root explícito e privado, sem SQL. */
export class LocalSpool implements SpoolRepository {
  readonly metrics = { peakBufferedBytes: 0, writes: 0, syncs: 0, bytes: 0, rows: 0 };
  private used = 0;
  private constructor(readonly root: string, readonly limits: SpoolLimits, private readonly faults?: StorageFaults) {}
  static async open(root: string, limits: SpoolLimits, faults?: StorageFaults): Promise<LocalSpool> {
    if (!isAbsolute(root) || resolve(root) !== root) throw new StorageError('CORRUPT');
    if (Object.values(limits).some(n => !Number.isSafeInteger(n) || n < 0) || limits.maxRows < 1 || limits.maxRows > 200000 ||
        limits.bufferBytes < 1024 || limits.bufferBytes > 1024 ** 2 || limits.maxRecordBytes < 64 || limits.maxRecordBytes > 8 * 1024 ** 2 ||
        limits.maxDiskBytes < 4096 || limits.maxDiskBytes > 16 * 1024 ** 3 || limits.maxRecordBytes > limits.chunkBytes) throw new StorageError('CORRUPT');
    limits = Object.freeze({ ...limits });
    const repo = new LocalSpool(root,limits,faults);
    try {
      await fs.mkdir(root,{ mode: 0o700, recursive: true });
      if (await fs.realpath(root) !== root || (await fs.lstat(root)).mode & 0o077) throw new StorageError('CORRUPT');
      await fs.mkdir(join(root,'batches'),{ mode: 0o700 }).catch(e => { if (e.code !== 'EEXIST') throw e; });
      const dir = await fs.lstat(join(root,'batches'));
      if (!dir.isDirectory() || dir.isSymbolicLink() || dir.mode & 0o077) throw new StorageError('CORRUPT');
      repo.used = await repo.size(root);
      await repo.space(0);
      return repo;
    } catch (e) { throw safe(e); }
  }
  private async size(path: string, depth = 0): Promise<number> {
    if (depth > 2) throw new StorageError('CORRUPT');
    let bytes = 0;
    const names = await fs.readdir(path);
    if (names.length > 10000) throw new StorageError('QUOTA_EXCEEDED');
    for (const name of names) {
      const p = join(path,name), stat = await fs.lstat(p);
      if (stat.isSymbolicLink()) throw new StorageError('CORRUPT');
      bytes += stat.isDirectory() ? await this.size(p,depth + 1) : stat.size;
    }
    return bytes;
  }
  private path(id: string): string {
    if (!uuid.test(id)) throw new StorageError('CORRUPT');
    return join(this.root,'batches',id);
  }
  private async hook(operation: Operation): Promise<void> { await this.faults?.before(operation); }
  private async syncDir(dir: string): Promise<void> {
    const handle = await fs.open(dir,constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW);
    try { await this.hook('sync'); await handle.sync(); this.metrics.syncs++; } finally { await handle.close(); }
  }
  private async space(bytes: number): Promise<void> {
    // Reserva para metadados/rename. Sem apagar arquivos antigos ou lotes incompletos.
    if (this.used + bytes + 131072 > this.limits.maxDiskBytes) throw new StorageError('QUOTA_EXCEEDED');
    const s = await fs.statfs(this.root);
    if (s.bavail * s.bsize < bytes + this.limits.minFreeBytes + 131072) throw new StorageError('DISK_FULL');
  }
  private async writeAll(handle: fs.FileHandle, data: Buffer): Promise<void> {
    let offset = 0;
    while (offset < data.length) {
      await this.hook('write');
      const { bytesWritten } = await handle.write(data,offset,data.length-offset);
      if (bytesWritten < 1) throw new StorageError('STORAGE_FAILED');
      offset += bytesWritten;
    }
    this.metrics.writes++;
  }
  private async readJson(path: string): Promise<unknown> {
    const handle = await fs.open(path,constants.O_RDONLY | constants.O_NOFOLLOW);
    try {
      const stat = await handle.stat();
      if (!stat.isFile() || stat.size > 131072 || stat.mode & 0o077) throw new StorageError('CORRUPT');
      return JSON.parse(await handle.readFile('utf8')) as unknown;
    } finally { await handle.close(); }
  }
  private async owner(id: string): Promise<void> {
    const active = await this.readJson(join(this.root,'active.json')) as { batchId?: string };
    if (active.batchId !== id) throw new StorageError('BUSY');
  }
  private async manifest(manifest: SpoolManifest): Promise<void> {
    const dir = this.path(manifest.identity.batchId);
    await this.owner(manifest.identity.batchId);
    await this.hook('manifest');
    const path = join(dir,`${randomUUID()}.tmp`), data = Buffer.from(JSON.stringify(manifest));
    if (data.length > 65536) throw new StorageError('QUOTA_EXCEEDED');
    await this.space(data.length);
    const handle = await fs.open(path,constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW,0o600);
    try { await this.writeAll(handle,data); await this.hook('sync'); await handle.sync(); } finally { await handle.close(); }
    await this.hook('rename'); await fs.rename(path,join(dir,'manifest.json')); await this.syncDir(dir);
  }
  async active(): Promise<SpoolManifest | null> {
    try {
      let active: { batchId?: string; kind?: string };
      try { active = await this.readJson(join(this.root,'active.json')) as typeof active; }
      catch (e) { if (typeof e === 'object' && e !== null && 'code' in e && e.code === 'ENOENT') return null; throw e; }
      if(active.kind==='tds-capture')throw new StorageError('INCOMPLETE');
      if (!active.batchId) throw new StorageError('CORRUPT');
      const m = await this.readJson(join(this.path(active.batchId),'manifest.json')) as SpoolManifest;
      if (m.identity?.batchId !== active.batchId || m.identity.format !== 'olos-spool/v1' ||
        !['receiving','sealed','persisting','uncertain','recovery_required','committed'].includes(m.state) ||
        !Number.isSafeInteger(m.rows) || m.rows < 0 || m.rows > this.limits.maxRows ||
        !Number.isSafeInteger(m.bytes) || m.bytes < 0 || m.bytes > this.limits.maxDiskBytes ||
        typeof m.sealed !== 'boolean' || m.sealed && !/^[a-f0-9]{64}$/.test(m.sha256 ?? '')) throw new StorageError('CORRUPT');
      return m;
    } catch (e) { throw safe(e); }
  }
  async create(identity: SpoolIdentity): Promise<SpoolWriter> {
    const id = identity.batchId, dir = this.path(id);
    if (!uuid.test(identity.runId) || !uuid.test(identity.streamId)) throw new StorageError('CORRUPT');
    try {
      try { await fs.lstat(join(this.root,'active-capture.json')); throw new StorageError('BUSY'); }
      catch (e) { if (!(e !== null && typeof e === 'object' && 'code' in e && e.code === 'ENOENT')) throw e; }
      try { await fs.lstat(join(this.root,'transition.lock')); throw new StorageError('BUSY'); }
      catch (e) { if (!(e !== null && typeof e === 'object' && 'code' in e && e.code === 'ENOENT')) throw e; }
      this.used = await this.size(this.root); await this.space(65536);
      const lock = await fs.open(join(this.root,'active.json'),constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW,0o600);
      try { await this.writeAll(lock,Buffer.from(JSON.stringify({ batchId: id, runId: identity.runId, table: identity.plan.run.table }))); await this.hook('sync'); await lock.sync(); }
      finally { await lock.close(); }
      await this.syncDir(this.root);
      await fs.mkdir(dir,{ mode: 0o700 }); await this.syncDir(join(this.root,'batches'));
      let m: SpoolManifest = { identity: structuredClone(identity), state: 'receiving', sealed: false, rows: 0, bytes: 0, sha256: null };
      await this.manifest(m);
      const file = await fs.open(join(dir,'records.partial'),constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW,0o600);
      const hash = createHash('sha256');
      let buffers: Buffer[] = [], buffered = 0, sinceSync = 0, rows = 0, bytes = 0, closed = false, pending = false;
      const flush = async () => {
        if (!buffered) return;
        const data = Buffer.concat(buffers,buffered);
        await this.space(data.length); await this.writeAll(file,data);
        this.used += data.length; sinceSync += data.length; hash.update(data); buffers = []; buffered = 0;
        if (sinceSync >= this.limits.syncEveryBytes) { await this.hook('sync'); await file.sync(); this.metrics.syncs++; sinceSync = 0; }
      };
      return {
        append: async record => {
          if (closed || pending) throw new StorageError('BUSY'); pending = true;
          try {
            if (!record || Object.getPrototypeOf(record) !== Object.prototype ||
              Object.keys(record).some(k => /password|passwd|senha/.test(k.toLowerCase().replace(/[^a-z]/g,'')) || !Object.hasOwn(identity.plan.contract.fields,k)) ||
              Object.keys(identity.plan.contract.fields).some(k => !Object.hasOwn(record,k)) ||
              Object.values(record).some(v => v !== null && typeof v !== 'string' && typeof v !== 'boolean')) throw new StorageError('CORRUPT');
            const data = Buffer.from(`${JSON.stringify(record)}\n`);
            if (rows >= this.limits.maxRows || data.length > this.limits.maxRecordBytes) throw new StorageError('QUOTA_EXCEEDED');
            if (buffered + data.length > this.limits.bufferBytes) await flush();
            buffers.push(data); buffered += data.length; rows++; bytes += data.length;
            this.metrics.rows++; this.metrics.bytes += data.length;
            this.metrics.peakBufferedBytes = Math.max(this.metrics.peakBufferedBytes,buffered);
            if (buffered >= this.limits.bufferBytes) await flush();
          } catch (e) { throw safe(e); } finally { pending = false; }
        },
        seal: async () => {
          if (closed || pending) throw new StorageError('BUSY'); pending = true;
          try {
            await flush(); await this.hook('sync'); await file.sync(); this.metrics.syncs++;
            await file.close(); closed = true;
            await this.hook('rename'); await fs.rename(join(dir,'records.partial'),join(dir,'records.ready')); await this.syncDir(dir);
            m = { ...m, state: 'sealed', sealed: true, rows, bytes, sha256: hash.digest('hex') };
            await this.manifest(m);
            return m;
          } catch (e) { throw safe(e); } finally { pending = false; }
        },
        interrupt: async () => {
          if (!closed) { try { await file.close(); } finally { closed = true; } }
          buffers = []; buffered = 0;
          // Não promover um arquivo só porque existe: sem selo, a resposta remota pode ser parcial.
          try { await this.manifest({ ...m, state: 'recovery_required' }); } catch { /* intent inicial permanece se disco falhou */ }
        },
      };
    } catch (e) { throw safe(e); }
  }
  /** Quarantine of exact TDS response bytes. Never an approved ingestion batch/checkpoint. */
  async createResponseCapture(attemptId: string) {
    const dir = this.path(attemptId);
    if (await this.active()) throw new StorageError('BUSY');
    this.used = await this.size(this.root); await this.space(65536);
    // Same atomic claim as normal batches: capture and ingestion cannot win independently.
    const primary=await fs.open(join(this.root,'active.json'),constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|constants.O_NOFOLLOW,0o600);
    try{await this.writeAll(primary,Buffer.from(JSON.stringify({batchId:attemptId,kind:'tds-capture'})));await primary.sync();}
    finally{await primary.close();}
    await this.syncDir(this.root);
    const claim = await fs.open(join(this.root,'active-capture.json'),constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|constants.O_NOFOLLOW,0o600);
    try { await this.writeAll(claim,Buffer.from(JSON.stringify({attemptId}))); await claim.sync(); }
    finally { await claim.close(); }
    await this.syncDir(this.root);
    await fs.mkdir(dir,{mode:0o700}); await this.syncDir(join(this.root,'batches'));
    const file = await fs.open(join(dir,'response.partial'),constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|constants.O_NOFOLLOW,0o600);
    const hash = createHash('sha256'); let bytes = 0, closed = false, pending = false, sinceSync = 0;
    const descriptor = async (state: 'sealed'|'incomplete', detail: Readonly<Record<string,unknown>>) => {
      const data = Buffer.from(JSON.stringify({...detail,format:'olos-tds-response/v1',attemptId,state,bytes,
        contractApproved:false,businessDelivered:false,checkpointAdvanced:false}));
      if (data.length > 65536) throw new StorageError('QUOTA_EXCEEDED');
      await this.space(data.length);
      const f = await fs.open(join(dir,'response.json'),constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|constants.O_NOFOLLOW,0o600);
      try { await this.writeAll(f,data); await f.sync(); } finally { await f.close(); }
      await this.syncDir(dir);
    };
    return {
      append: async (chunk: Buffer) => {
        if (closed || pending) throw new StorageError('BUSY'); pending = true;
        try {
          if (!Buffer.isBuffer(chunk) || chunk.length > 16*1024**2) throw new StorageError('QUOTA_EXCEEDED');
          await this.space(chunk.length); await this.writeAll(file,chunk); hash.update(chunk);
          this.used += chunk.length; bytes += chunk.length; sinceSync += chunk.length;
          if (sinceSync >= this.limits.syncEveryBytes) { await file.sync(); sinceSync=0; }
        } catch(e) { throw safe(e); } finally { pending=false; }
      },
      seal: async (detail: Readonly<Record<string,unknown>>) => {
        if (closed || pending) throw new StorageError('BUSY'); pending=true;
        try {
          await this.hook('sync'); await file.sync(); await file.close(); closed=true;
          await this.hook('rename'); await fs.rename(join(dir,'response.partial'),join(dir,'response.ready')); await this.syncDir(dir);
          const sha256=hash.digest('hex'); await descriptor('sealed',{...detail,sha256});
          // Keep the exclusive claim: approval/local replay is required before ANY new acquisition.
          return {bytes,sha256};
        } catch(e) { throw safe(e); } finally { pending=false; }
      },
      interrupt: async () => {
        if (!closed) { try { await file.sync(); } finally { await file.close(); closed=true; } }
        try { await descriptor('incomplete',{}); } catch { /* durable remote intent and capture claim remain */ }
      },
    };
  }
  async responseCapture(attemptId:string) {
    const m=await this.readJson(join(this.path(attemptId),'response.json')) as {
      format:string;attemptId:string;state:string;bytes:number;sha256:string;received:number;fields:unknown;
    };
    if(m.format!=='olos-tds-response/v1' || m.attemptId!==attemptId || m.state!=='sealed' ||
      !Number.isSafeInteger(m.bytes) || m.bytes<1 || m.bytes>this.limits.maxDiskBytes || !/^[a-f0-9]{64}$/.test(m.sha256) ||
      !Number.isSafeInteger(m.received) || m.received<0 || m.received>this.limits.maxRows || !Array.isArray(m.fields))throw new StorageError('INCOMPLETE');
    return m;
  }
  async *responseChunks(attemptId:string):AsyncGenerator<Buffer> {
    const m=await this.responseCapture(attemptId);
    const f=await fs.open(join(this.path(attemptId),'response.ready'),constants.O_RDONLY|constants.O_NOFOLLOW);
    const hash=createHash('sha256');let bytes=0;
    try{
      const s=await f.stat();if(!s.isFile() || s.mode&0o077 || s.size!==m.bytes)throw new StorageError('CORRUPT');
      while(true){const b=Buffer.alloc(this.limits.bufferBytes),r=await f.read(b,0,b.length,null);if(!r.bytesRead)break;
        const chunk=b.subarray(0,r.bytesRead);bytes+=chunk.length;hash.update(chunk);yield chunk;
      }
      if(bytes!==m.bytes || hash.digest('hex')!==m.sha256)throw new StorageError('CORRUPT');
    }finally{await f.close();}
  }
  async *records(m: SpoolManifest): AsyncIterable<SpoolRecord> {
    if (!m.sealed || !m.sha256) throw new StorageError('INCOMPLETE');
    const file = await fs.open(join(this.path(m.identity.batchId),'records.ready'),constants.O_RDONLY | constants.O_NOFOLLOW);
    const hash = createHash('sha256'), decoder = new TextDecoder('utf-8',{ fatal: true, ignoreBOM: true });
    let pending = Buffer.alloc(0), count = 0, bytes = 0;
    try {
      const stat = await file.stat();
      if (stat.size !== m.bytes || stat.mode & 0o077) throw new StorageError('CORRUPT');
      const buffer = Buffer.alloc(this.limits.bufferBytes);
      while (true) {
        await this.hook('read');
        const { bytesRead } = await file.read(buffer,0,buffer.length,null);
        if (!bytesRead) break;
        const chunk = buffer.subarray(0,bytesRead); hash.update(chunk); bytes += bytesRead;
        pending = Buffer.concat([pending,chunk]);
        let offset: number;
        while ((offset = pending.indexOf(10)) !== -1) {
          if (offset + 1 > this.limits.maxRecordBytes || ++count > this.limits.maxRows) throw new StorageError('CORRUPT');
          yield JSON.parse(decoder.decode(pending.subarray(0,offset))) as SpoolRecord;
          pending = pending.subarray(offset + 1);
        }
        if (pending.length > this.limits.maxRecordBytes) throw new StorageError('CORRUPT');
      }
      if (pending.length || count !== m.rows || bytes !== m.bytes || hash.digest('hex') !== m.sha256) throw new StorageError('CORRUPT');
    } catch (e) { throw safe(e); } finally { await file.close(); }
  }
  async mark(m: SpoolManifest, state: 'persisting' | 'uncertain' | 'committed'): Promise<SpoolManifest> {
    let transition: fs.FileHandle | undefined;
    try {
      transition = await fs.open(join(this.root,'transition.lock'),constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW,0o600);
      await this.owner(m.identity.batchId);
      const current = await this.active();
      if (!current?.sealed || current.identity.batchId !== m.identity.batchId || current.sha256 !== m.sha256) throw new StorageError('CORRUPT');
      if (current.state === 'committed' && state !== 'committed') return current;
      const next = { ...current, state };
      await this.manifest(next);
      if (state === 'committed') { await fs.unlink(join(this.root,'active.json')); await this.syncDir(this.root); }
      return next;
    } catch (e) { throw safe(e); }
    finally { if (transition) { await transition.close(); await fs.unlink(join(this.root,'transition.lock')); } }
  }
}
