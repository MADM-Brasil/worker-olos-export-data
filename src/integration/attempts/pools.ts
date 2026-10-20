import pg from 'pg';
import type { RuntimePools } from './service.js';
export const runtimeLogins=['olos_ingest_login','olos_project_login','olos_receiver_login'] as const;
export function createLocalRuntimePools(passwords:Readonly<Record<typeof runtimeLogins[number],string>>,onError:()=>void):RuntimePools {
  const make=(user:typeof runtimeLogins[number])=>{
    const p=new pg.Pool({host:'127.0.0.1',port:5434,database:'olos_worker',user,password:passwords[user],ssl:false,
      max:user==='olos_ingest_login'?3:2,connectionTimeoutMillis:5000,idleTimeoutMillis:30000,query_timeout:15000,statement_timeout:15000,
      application_name:'olos-local-worker',options:'-c search_path=olos_export'});
    p.on('error',onError);return p;
  };
  return {ingest:make(runtimeLogins[0]),project:make(runtimeLogins[1]),receiver:make(runtimeLogins[2])};
}
