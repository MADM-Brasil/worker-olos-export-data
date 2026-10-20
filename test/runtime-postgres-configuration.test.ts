import {afterEach,beforeEach,expect,it,vi} from 'vitest';
import {mkdtemp,mkdir,writeFile,chmod,rm,symlink} from 'node:fs/promises';
import {join} from 'node:path';
import {tmpdir} from 'node:os';
import {Socket} from 'node:net';
import type {PoolConfig} from 'pg';
import {createLocalRuntimePools,runtimePostgresTarget} from '../src/integration/attempts/pools.js';

const state=vi.hoisted(()=>({home:''}));
vi.mock('node:os',async original=>({...await original<typeof import('node:os')>(),homedir:()=>state.home}));
import {localSettings} from '../adapters/local-worker.js';
const passwords={olos_ingest_login:'synthetic-ingest',olos_project_login:'synthetic-project',olos_receiver_login:'synthetic-receiver'};
let dir:string;
beforeEach(async()=>{
  state.home=await mkdtemp(join(tmpdir(),'olos-runtime-config-'));
  dir=join(state.home,'.config','olos-worker');await mkdir(dir,{recursive:true,mode:0o700});
  await writeFile(join(dir,'worker-local.env'),'SYNC_ENABLED=false\n',{mode:0o600});
  await writeFile(join(dir,'postgres-credentials.json'),JSON.stringify(passwords),{mode:0o600});
  await writeFile(join(dir,'http-status-token'),'synthetic-status-token-at-least-32-characters',{mode:0o600});
  vi.spyOn(Socket.prototype,'connect').mockImplementation(()=>{throw Error('NO_NETWORK_EXPECTED');});
});
afterEach(async()=>{expect(Socket.prototype.connect).not.toHaveBeenCalled();vi.restoreAllMocks();await chmod(dir,0o700);await rm(state.home,{recursive:true,force:true});});

it('preserves the local default and configures all three Docker pools without an administrative login',async()=>{
  expect((await localSettings({})).postgres).toEqual({host:'127.0.0.1',port:5434,database:'olos_worker'});
  const settings=await localSettings({WORKER_PG_HOST:'postgres',WORKER_PG_PORT:'5432',WORKER_PG_DATABASE:'olos_worker',PG_USER:'olos_admin',PG_PASSWORD:'ignored-admin'});
  expect(settings.postgres).toEqual({host:'postgres',port:5432,database:'olos_worker'});
  const pools=createLocalRuntimePools(settings.passwords,()=>{},settings.postgres);
  try{
    for(const [role,user] of [['ingest','olos_ingest_login'],['project','olos_project_login'],['receiver','olos_receiver_login']] as const){
      const options=(pools[role] as unknown as {options:PoolConfig}).options;
      expect(options).toMatchObject({host:'postgres',port:5432,database:'olos_worker',user,password:passwords[user],ssl:false});
      expect(JSON.stringify(options)).not.toContain('ignored-admin');
    }
  }finally{await Promise.all(Object.values(pools).map(p=>p.end()));}
});
it('accepts the existing private local env and uses dedicated runtime overrides only for the destination',async()=>{
  await writeFile(join(dir,'worker-local.env'),'SYNC_ENABLED=false\nPG_HOST=127.0.0.1\nPG_PORT=5434\nPG_DATABASE=olos_worker\n');
  expect((await localSettings({})).postgres.port).toBe(5434);
  expect((await localSettings({WORKER_PG_HOST:'postgres',WORKER_PG_PORT:'5432'})).postgres.host).toBe('postgres');
});
it('reads pre-provisioned files in a readonly private directory without generating a token',async()=>{
  await chmod(dir,0o500);
  const settings=await localSettings({WORKER_PG_HOST:'postgres',WORKER_PG_PORT:'5432'});
  expect(settings.passwords).toEqual(passwords);expect(settings.config.SYNC_ENABLED).toBe(false);
});
it('requires a pre-provisioned status token in production instead of writing to the readonly mount',async()=>{
  await rm(join(dir,'http-status-token'));
  await expect(localSettings({NODE_ENV:'production'})).rejects.toThrow('PRIVATE_STATUS_TOKEN_REQUIRED');
});
it.each([{WORKER_PG_PORT:'0'},{WORKER_PG_PORT:'65536'},{WORKER_PG_PORT:'NaN'},{WORKER_PG_DATABASE:'another_database'},{WORKER_PG_HOST:'postgres://secret@host'}])('rejects an invalid target without including its value',async env=>{
  await expect(localSettings(env)).rejects.toThrow('RUNTIME_POSTGRES_CONFIGURATION_INVALID');
});
it.each([{...passwords,olos_admin:'never-print-admin'}, {...passwords,olos_project_login:passwords.olos_ingest_login}, {...passwords,olos_receiver_login:''}])('rejects unknown/admin credentials, reused or empty passwords with a sanitized error',async input=>{
  await writeFile(join(dir,'postgres-credentials.json'),JSON.stringify(input));
  await expect(localSettings({})).rejects.toThrow(/^PRIVATE_RUNTIME_CREDENTIALS_INVALID$/);
});
it('rejects insecure modes and symlink credential files',async()=>{
  const path=join(dir,'postgres-credentials.json');await chmod(path,0o644);
  await expect(localSettings({})).rejects.toThrow('PRIVATE_RUNTIME_CREDENTIALS_INVALID');
  await rm(path);const target=join(state.home,'credentials.json');await writeFile(target,JSON.stringify(passwords),{mode:0o600});await symlink(target,path);
  await expect(localSettings({})).rejects.toThrow('PRIVATE_RUNTIME_CREDENTIALS_INVALID');
});
it('keeps sync and login selection guarded',async()=>{
  await expect(localSettings({SYNC_ENABLED:'true'})).rejects.toThrow('LOCAL_CONFIGURATION_REJECTED');
  expect(()=>runtimePostgresTarget({host:'postgres',port:5432,database:'olos_worker',user:'olos_admin'})).toThrow('RUNTIME_POSTGRES_CONFIGURATION_INVALID');
});
