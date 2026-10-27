import type { Pool,PoolClient } from 'pg';
import { AttemptsRuntimeControl,type WorkerRuntime,type CycleCounts } from './runtime-control.js';
import { attemptsBlockers } from './contracts.js';
import {assertAttemptsDestinationSchema} from './destination-schema.js';
import {setTimeout as delay} from 'node:timers/promises';
import {annotateFailure,failureDetails,logWorkerFailure,preserveFailure,type FailureContext} from '../../diagnostics/worker-failure.js';

const STATISTICS_INTERVAL_MS=10*60*1000;
const emptyAggregates=()=>({totalCalls:null,withCpf:null,withEmail:null,withAgentName:null,pendingDeliveries:null,confirmedDeliveries:null,checkpoints:null,ingestionTotals:null});

export interface RuntimePools {ingest:Pool;project:Pool;receiver:Pool;}
export interface LocalDeliveryWork {initialize(signal:AbortSignal):Promise<void>;step(signal:AbortSignal):Promise<boolean>;}
export class AttemptsService implements WorkerRuntime {
  private readonly control:AttemptsRuntimeControl;
  private database:'unknown'|'up'|'down'='unknown';
  private schemaReady=false;
  private aggregates:Record<string,unknown>=emptyAggregates();
  private statisticsTask:Promise<void>|undefined;
  private statisticsAttemptedAt:number|null=null;
  private statisticsError:ReturnType<typeof failureDetails>|null=null;
  private refreshTask:Promise<void>|undefined;
  private starting:Promise<void>|undefined;
  private closing:Promise<void>|undefined;
  private lock:PoolClient|undefined;
  private stopping=false;
  private deliveryTask:Promise<void>|undefined;
  private checkedAt:string|null=null;
  private startupFailure:ReturnType<typeof logWorkerFailure>|null=null;
  private readonly abort=new AbortController();
  constructor(readonly pools:RuntimePools,private readonly intervalMs=60000,
    private readonly authorizedCycle?: (signal:AbortSignal)=>Promise<CycleCounts>,private readonly sourceKind='isolated-fixture',private readonly operationalSnapshot?:()=>Record<string,unknown>,private readonly deliveryWork?:LocalDeliveryWork) {
    this.control=new AttemptsRuntimeControl(intervalMs);
    this.control.block(['ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED',...attemptsBlockers]);
  }
  async probe():Promise<void>{
    if(!this.refreshTask)this.refreshTask=this.refresh().finally(()=>{this.refreshTask=undefined;});return this.refreshTask;
  }
  private fail(error:unknown,context:FailureContext){
    if(this.stopping)return;
    this.control.requireReconciliation(error,context);this.abort.abort();
  }
  private async refresh(){
    let connected=false;
    try {
      for(const [pool,login] of [[this.pools.ingest,'olos_ingest_login'],[this.pools.project,'olos_project_login'],[this.pools.receiver,'olos_receiver_login']] as const){
        const c=await pool.connect();try{await c.query('BEGIN READ ONLY');
          const r=(await c.query("SELECT session_user AS login,current_setting('is_superuser') AS superuser")).rows[0];
          if(r.login!==login || r.superuser!=='off')throw Error('ROLE_REJECTED');await c.query('COMMIT');
        }catch(e){await c.query('ROLLBACK').catch(()=>{});throw e;}finally{c.release();}
      }
      connected=true;
      this.database='up';
      const required:[Pool,string[],string][]=[
        [this.pools.ingest,['streams','runs','source_locks','batches','checkpoints','attempts_raw_data'],'SELECT,INSERT,UPDATE'],
        [this.pools.ingest,['large_batch_receipts','large_batch_keys','projection_batches','projection_outbox','audit_events'],'SELECT,INSERT'],
        [this.pools.project,['projection_outbox'],'SELECT'],[this.pools.project,['projection_events'],'SELECT,INSERT'],
        [this.pools.receiver,['delivery_receipts'],'SELECT,INSERT']
      ];
      for(const [pool,tables,privileges] of required)for(const table of tables){
        const r=(await pool.query('SELECT bool_and(has_table_privilege(current_user,$1,p)) AS allowed FROM unnest($2::text[]) AS p',[`olos_export.${table}`,privileges.split(',')])).rows[0];
        if(!r.allowed)throw Error('PRIVILEGE_REJECTED');
      }
      const projectColumns=['state','generation','lease_owner','lease_until','attempts','available_at','attempt_id','request_payload','request_digest','last_code','confirmed_at'];
      if(!(await this.pools.project.query("SELECT bool_and(has_column_privilege(current_user,'olos_export.projection_outbox',c,'UPDATE')) AS allowed FROM unnest($1::text[]) AS c",[projectColumns])).rows[0].allowed)throw Error('PRIVILEGE_REJECTED');
      const c=await this.pools.receiver.connect();
      try{await c.query('BEGIN READ ONLY');
        await assertAttemptsDestinationSchema(c);
        await c.query('COMMIT');
      }catch(e){await c.query('ROLLBACK').catch(()=>{});throw e;}finally{c.release();}
      this.schemaReady=true;
      if(!this.authorizedCycle && !this.stopping)this.control.block(['ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED',...attemptsBlockers]);
    }catch(error) {this.schemaReady=false;this.database=connected?'up':'down';
      const failure=annotateFailure(preserveFailure(Error('POSTGRES_RUNTIME_NOT_READY'),error),{flow:'probe',stage:'PROBE'});
      if(this.authorizedCycle && this.control.snapshot().armed)this.fail(failure,{flow:'probe',stage:'PROBE'});throw failure;}
    // Observability must neither delay readiness nor stop acquisition. Failures also observe the cooldown.
    if(!this.stopping && !this.statisticsTask && (this.statisticsAttemptedAt===null || Date.now()-this.statisticsAttemptedAt>=STATISTICS_INTERVAL_MS)){
      this.statisticsAttemptedAt=Date.now();
      this.statisticsTask=this.refreshStatistics().finally(()=>{this.statisticsTask=undefined;});
    }
  }
  private async refreshStatistics(){
    try{
      const calls=(await this.pools.receiver.query(`SELECT count(*)::text AS "totalCalls",count(cpf)::text AS "withCpf",count(email)::text AS "withEmail",count(nome_agente)::text AS "withAgentName" FROM raw.olos_calls`)).rows[0];
      const outbox=(await this.pools.project.query(`SELECT count(*) FILTER(WHERE state<>'confirmed')::text AS "pendingDeliveries",count(*) FILTER(WHERE state='confirmed')::text AS "confirmedDeliveries" FROM olos_export.projection_outbox`)).rows[0];
      const checkpoints=(await this.pools.ingest.query(`SELECT c.version::text,c.last_batch_id FROM olos_export.checkpoints c JOIN olos_export.streams s ON s.id=c.stream_id WHERE s.source_table='AttemptsRawData' ORDER BY s.id`)).rows;
      const totals=(await this.pools.ingest.query(`SELECT coalesce(sum(received_count),0)::text AS received,coalesce(sum(persisted_count),0)::text AS persisted,
        coalesce(sum(duplicate_count),0)::text AS duplicates,count(*) FILTER(WHERE state IN ('read_intent','persisting','needs_reconciliation'))::text AS unresolved
        FROM olos_export.runs WHERE source_table='AttemptsRawData'`)).rows[0];
      // Publish only a complete sample; never mix new counts with values from an older attempt.
      this.aggregates={...calls,...outbox,checkpoints,ingestionTotals:totals};this.checkedAt=new Date().toISOString();this.statisticsError=null;
    }catch(error){this.aggregates=emptyAggregates();this.statisticsError=failureDetails(error);}
  }
  /** Administrative safety check: exact existing predicates, independent of cached telemetry. */
  async checkRecoveryPending():Promise<boolean>{
    const runs=(await this.pools.ingest.query(`SELECT EXISTS(SELECT 1 FROM olos_export.runs WHERE source_table='AttemptsRawData' AND state IN ('read_intent','persisting','needs_reconciliation')) AS pending`)).rows[0];
    const outbox=(await this.pools.project.query(`SELECT EXISTS(SELECT 1 FROM olos_export.projection_outbox WHERE state<>'confirmed') AS pending`)).rows[0];
    return this.control.snapshot().state==='needs_reconciliation' || runs.pending || outbox.pending;
  }
  start():Promise<void>{
    if(this.starting)return this.starting;
    this.starting=(async()=>{
      let stage:FailureContext['stage']='PROBE';
      try {await this.probe();if(this.stopping || !this.authorizedCycle)return;
        stage='INSTANCE_LOCK';
        const lock=await this.pools.ingest.connect();this.lock=lock;
        lock.on('error',error=>this.fail(error,{flow:'instance',stage:'INSTANCE_LOCK'}));
        if(!(await lock.query('SELECT pg_try_advisory_lock(740219,1) AS acquired')).rows[0].acquired)throw Error('INSTANCE_BUSY');
        stage='INITIALIZE';try{await this.deliveryWork?.initialize(this.abort.signal);}catch(error){throw annotateFailure(error,{flow:'initialization',stage:'INITIALIZE'});}
        if(this.abort.signal.aborted || this.stopping)throw Error('INSTANCE_LOST');
        this.control.start(async signal=>{
          if(this.abort.signal.aborted || this.stopping)throw Error('INSTANCE_LOST');
          const combined=AbortSignal.any([signal,this.abort.signal]);
          try{await this.probe();return await this.authorizedCycle!(combined);}catch(error){this.fail(error,{flow:'acquisition',stage:'ACQUIRE'});throw error;}finally{await this.probe().catch(()=>{});}
        });
        if(this.deliveryWork)this.deliveryTask=this.deliver();
      }catch(error) {if(!this.stopping)this.startupFailure=logWorkerFailure('worker_start_failed',error,{flow:'initialization',stage});
        this.abort.abort();this.control.block(['POSTGRES_SCHEMA_ROLE_OR_INSTANCE_NOT_READY',...(!this.authorizedCycle?['ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED',...attemptsBlockers]:[])]);}
    })();return this.starting;
  }
  private async deliver(){
    try{while(!this.abort.signal.aborted && !this.stopping){
      const progressed=await this.deliveryWork!.step(this.abort.signal);
      if(!progressed)await delay(250,undefined,{signal:this.abort.signal});
    }}catch(error){this.fail(error,{flow:'delivery',stage:'DELIVER'});}
  }
  ready(){const s=this.control.snapshot();return !this.stopping && !this.abort.signal.aborted && this.schemaReady && s.enabled;}
  snapshotNextExecutionAt(){return this.control.snapshot().nextExecutionAt;}
  snapshotDatabaseState(){return this.database;}
  snapshotBaseArmed(){return this.control.snapshot().armed;}
  snapshot(){
    const statisticsAvailable=this.checkedAt!==null && this.statisticsError===null && !this.statisticsTask && this.statisticsAttemptedAt!==null && Date.now()-this.statisticsAttemptedAt<STATISTICS_INTERVAL_MS;
    const aggregates=statisticsAvailable?this.aggregates:emptyAggregates();
    const totals=aggregates.ingestionTotals as {unresolved:string}|null;
    return {...this.control.snapshot(),lastFailure:this.control.snapshot().lastFailure??this.startupFailure,enabled:this.ready(),countsScope:'ingestion_last_completed_cycle',sourceConfigured:Boolean(this.authorizedCycle),sourceAuthorized:Boolean(this.authorizedCycle),
    sourceKind:this.authorizedCycle?this.sourceKind:'corporate-blocked',databaseState:this.database,schemaReady:this.schemaReady,statisticsAvailable,statisticsCheckedAt:this.checkedAt,
    statisticsStatus:this.statisticsTask?'refreshing':this.statisticsError?'unavailable':statisticsAvailable?'cached':this.checkedAt?'stale':'unavailable',
    statisticsAttemptedAt:this.statisticsAttemptedAt===null?null:new Date(this.statisticsAttemptedAt).toISOString(),statisticsRefreshIntervalMs:STATISTICS_INTERVAL_MS,statisticsError:this.statisticsError,
    recoveryPending:this.control.snapshot().state==='needs_reconciliation'?true:statisticsAvailable?Boolean(totals && totals.unresolved!=='0') || Number(aggregates.pendingDeliveries)>0:null,...aggregates,...this.operationalSnapshot?.()};}
  stop():Promise<void>{this.closing ??=this.close();return this.closing;}
  private async close(){this.stopping=true;this.abort.abort();await this.starting;await this.control.stop();await this.deliveryTask;await this.refreshTask?.catch(()=>{});
    await this.statisticsTask;
    if(this.lock){await this.lock.query('SELECT pg_advisory_unlock(740219,1)').catch(()=>{});this.lock.release(true);this.lock=undefined;}
    const results=await Promise.allSettled(Object.values(this.pools).map(p=>p.end()));if(results.some(r=>r.status==='rejected'))throw Error('RUNTIME_POOL_CLOSE_FAILED');
  }
}
