import { open,lstat,realpath } from 'node:fs/promises';
import {constants} from 'node:fs';
import {homedir} from 'node:os';
import {join,resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {randomBytes} from 'node:crypto';
import {parse} from 'dotenv';
import {z} from 'zod';
import {loadConfig} from '../src/config/env.js';
import {serveWorker} from '../src/main.js';
import {createLocalRuntimePools,runtimePostgresTarget} from '../src/integration/attempts/pools.js';
import {AttemptsService} from '../src/integration/attempts/service.js';

const secretSchema=z.object({olos_ingest_login:z.string().min(1),olos_project_login:z.string().min(1),olos_receiver_login:z.string().min(1)}).strict()
  .refine(value=>new Set(Object.values(value)).size===3);
async function secret(path:string){
  const s=await lstat(path);if(!s.isFile() || s.isSymbolicLink() || s.uid!==process.getuid?.() || s.mode&0o077 || await realpath(path)!==path || s.size>32768)throw Error('PRIVATE_CONFIGURATION_REQUIRED');
  const f=await open(path,constants.O_RDONLY|constants.O_NOFOLLOW);try{return await f.readFile('utf8');}finally{await f.close();}
}
export async function localSettings(runtimeEnv:NodeJS.ProcessEnv=process.env){
  const dir=join(homedir(),'.config','olos-worker');
  const parent=await lstat(dir);if(!parent.isDirectory() || parent.uid!==process.getuid?.() || parent.mode&0o022)throw Error('PRIVATE_CONFIGURATION_REQUIRED');
  const env=parse(await secret(join(dir,'worker-local.env')));
  if(env.SYNC_ENABLED!=='false' || runtimeEnv.SYNC_ENABLED!==undefined && runtimeEnv.SYNC_ENABLED!=='false')throw Error('LOCAL_CONFIGURATION_REJECTED');
  // Dedicated variables do not configure the generic boot diagnostic or accept an admin login.
  const postgres=runtimePostgresTarget({host:runtimeEnv.WORKER_PG_HOST??env.PG_HOST??'127.0.0.1',
    port:runtimeEnv.WORKER_PG_PORT??env.PG_PORT??'5434',database:runtimeEnv.WORKER_PG_DATABASE??env.PG_DATABASE??'olos_worker'});
  let passwords:z.infer<typeof secretSchema>;
  try{passwords=secretSchema.parse(JSON.parse(await secret(join(dir,'postgres-credentials.json'))));}
  catch{throw Error('PRIVATE_RUNTIME_CREDENTIALS_INVALID');}
  // Ignore generic insecure-TLS flags in the legacy env: they never reach SQL Server options.
  const selected:NodeJS.ProcessEnv={SYNC_ENABLED:'false'};
  for(const k of ['HTTP_HOST','HTTP_PORT','LOG_LEVEL','SYNC_INTERVAL_MS','SHUTDOWN_TIMEOUT_MS','STATUS_TOKEN'])if(env[k]!==undefined)selected[k]=env[k];
  selected.HTTP_HOST ??='127.0.0.1';selected.HTTP_PORT ??='3100';
  if(selected.HTTP_HOST!=='127.0.0.1')throw Error('LOCAL_HTTP_LOOPBACK_REQUIRED');
  const tokenPath=join(dir,'http-status-token');
  if(!selected.STATUS_TOKEN){
    try{selected.STATUS_TOKEN=(await secret(tokenPath)).trim();}
    catch(e){if(!(e && typeof e==='object' && 'code' in e && e.code==='ENOENT'))throw e;
      if(runtimeEnv.NODE_ENV==='production')throw Error('PRIVATE_STATUS_TOKEN_REQUIRED');
      const token=randomBytes(32).toString('hex');let f;
      try{f=await open(tokenPath,constants.O_WRONLY|constants.O_CREAT|constants.O_EXCL|constants.O_NOFOLLOW,0o600);await f.writeFile(token);await f.sync();}
      catch(error){if(!(error && typeof error==='object' && 'code' in error && error.code==='EEXIST'))throw error;}
      finally{await f?.close();}selected.STATUS_TOKEN=(await secret(tokenPath)).trim();
    }
  }
  return {config:loadConfig(selected),passwords,postgres};
}
export async function localCommand(action:string){
  const {config,passwords,postgres}=await localSettings();
  if(action==='status'){
    const r=await fetch(`http://127.0.0.1:${config.HTTP_PORT}/status`,{headers:{authorization:`Bearer ${config.STATUS_TOKEN}`},signal:AbortSignal.timeout(5000)});
    if(!r.ok)throw Error('LOCAL_STATUS_UNAVAILABLE');process.stdout.write(`${JSON.stringify(await r.json())}\n`);return;
  }
  if(action!=='start' && action!=='check')throw Error('LOCAL_COMMAND_REJECTED');
  const pools=createLocalRuntimePools(passwords,()=>process.stderr.write('{"event":"local_postgres_pool_error"}\n'),postgres);
  const runtime=new AttemptsService(pools,config.SYNC_INTERVAL_MS);
  if(action==='check'){try{await runtime.probe();process.stdout.write(`${JSON.stringify(runtime.snapshot())}\n`);}finally{await runtime.stop();}return;}
  await serveWorker(config,[{name:'postgres',configured:true,probe:()=>runtime.probe(),close:async()=>{}},{name:'olos',configured:false,probe:async()=>{throw Error('SOURCE_BLOCKED');},close:async()=>{}}],runtime);
}
if(process.argv[1] && import.meta.url===pathToFileURL(resolve(process.argv[1])).href){
  if(process.argv.length>3){process.stderr.write('{"event":"local_command_rejected"}\n');process.exitCode=1;}
  else void localCommand(process.argv[2] ?? 'start').catch(()=>{process.stderr.write('{"event":"local_worker_failed","sourceExecuted":false}\n');process.exitCode=1;});
}
