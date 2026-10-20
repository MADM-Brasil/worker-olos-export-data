import pg from 'pg';
import {z} from 'zod';
import type { RuntimePools } from './service.js';
export const runtimeLogins=['olos_ingest_login','olos_project_login','olos_receiver_login'] as const;
const targetSchema=z.object({host:z.string().min(1).max(253).regex(/^[a-zA-Z0-9.-]+$/).default('127.0.0.1'),
  port:z.coerce.number().int().min(1).max(65535).default(5434),database:z.literal('olos_worker').default('olos_worker')}).strict();
export type RuntimePostgresTarget=z.infer<typeof targetSchema>;
export function runtimePostgresTarget(input:unknown={}):RuntimePostgresTarget {
  const result=targetSchema.safeParse(input);
  if(!result.success)throw Error('RUNTIME_POSTGRES_CONFIGURATION_INVALID');
  return result.data;
}
export function createLocalRuntimePools(passwords:Readonly<Record<typeof runtimeLogins[number],string>>,onError:()=>void,target:RuntimePostgresTarget=runtimePostgresTarget()):RuntimePools {
  const destination=runtimePostgresTarget(target);
  const make=(user:typeof runtimeLogins[number])=>{
    const p=new pg.Pool({...destination,user,password:passwords[user],ssl:false,
      max:user==='olos_ingest_login'?3:2,connectionTimeoutMillis:5000,idleTimeoutMillis:30000,query_timeout:15000,statement_timeout:15000,
      application_name:'olos-local-worker',options:'-c search_path=olos_export'});
    p.on('error',onError);return p;
  };
  return {ingest:make(runtimeLogins[0]),project:make(runtimeLogins[1]),receiver:make(runtimeLogins[2])};
}
