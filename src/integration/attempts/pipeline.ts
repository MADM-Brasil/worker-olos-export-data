import {assertCorporateBinding} from './corporate.js';
import type { Pool } from 'pg';
import { LargeBatchCoordinator } from '../../large-batch/coordinator.js';
import type { LargePlan,RawSource,SpoolRepository,SpoolLimits } from '../../large-batch/types.js';
import { PostgresPersistence } from '../../persistence/store.js';
import { PostgresLargeBatchPersistence } from '../../persistence/large-batch.js';
import { isolatedTdsDelivery } from '../../offline/tds-delivery.js';
import type { OfflineProfile,Catalogs } from '../../offline/model.js';
import type { CycleCounts } from './runtime-control.js';
import { AttemptsService,type RuntimePools } from './service.js';
import {projectionClaimBatchSize} from '../../projection/outbox.js';
import {isTdsResponseReplay} from '../../olos/tds/source.js';
import {assertUuid} from '../../persistence/validation.js';

export interface AttemptsBinding {
  // Corporate bindings require registration by the reviewed factory; SELECT adapters remain unavailable.
  kind:'isolated-fixture'|'corporate-local';tlsVerified?:true;recoveryValidated?:true;
  plan:()=>LargePlan;source:()=>RawSource;
  deferCheckpointUntilDelivery?:boolean;
  configurationDigest?:string;
  acquisitionScopeDigest?:string;
  beforeAcquire?:(plan:LargePlan,signal:AbortSignal)=>Promise<void>;
  afterAcquire?:(result:CycleCounts)=>Promise<void>;
  destinationId:string;mappingVersion:string;sourceNamespace:string;
  profile:OfflineProfile;catalogs:Catalogs;
}
export function createAttemptsPipeline(ingest:Pool,project:Pool,receiver:Pool,spool:SpoolRepository,limits:SpoolLimits,binding:AttemptsBinding,independentDelivery=false){
  if(binding.kind==='corporate-local')assertCorporateBinding(binding);
  else if(binding.kind!=='isolated-fixture' || binding.tlsVerified!==true || binding.recoveryValidated!==true)throw Error('ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED');
  if(binding.profile.tmaPolicy!=='disabled' || binding.profile.phoneMatchKey!=='normalized')throw Error('DESTINATION_MAPPING_NOT_APPROVED');
  const control=new PostgresPersistence(ingest,{leaseMs:300000});
  const writer=new PostgresLargeBatchPersistence(ingest,limits,300000,{sourceNamespace:binding.sourceNamespace,destinationId:binding.destinationId,mappingVersion:binding.mappingVersion},binding.deferCheckpointUntilDelivery===true,independentDelivery);
  const coordinator=new LargeBatchCoordinator(independentDelivery?{startRun:control.startRun.bind(control),checkpoint:writer.ingestionCheckpoint.bind(writer),renew:control.renew.bind(control),recoverExpired:control.recoverExpired.bind(control)}:control,spool,writer,limits);
  const delivery=isolatedTdsDelivery(project,receiver,binding.destinationId,binding.profile,binding.catalogs,new Date().toISOString().slice(0,-1),true);
  const empty:CycleCounts={received:0,persisted:0,duplicates:0,rejected:0};
  async function drain(signal:AbortSignal){
    const uncertain=(await project.query<{delivery_id:string}>(`SELECT delivery_id FROM olos_export.projection_outbox WHERE destination_id=$1 AND state='uncertain'`,[binding.destinationId])).rows;
    for(const row of uncertain){if(signal.aborted)throw Error('STOPPED');await delivery.dispatcher.reconcile(row.delivery_id);}
    const before=(await project.query(`SELECT count(*)::int AS n FROM olos_export.projection_outbox WHERE destination_id=$1 AND state<>'confirmed'`,[binding.destinationId])).rows[0].n;
    for(let delivered=0;delivered<limits.maxRows;){
      const count=await delivery.dispatcher.runBatch(signal,Math.min(projectionClaimBatchSize,limits.maxRows-delivered));
      if(!count)break;delivered+=count;
    }
    const pending=(await project.query(`SELECT 1 FROM olos_export.projection_outbox WHERE destination_id=$1 AND state<>'confirmed' LIMIT 1`,[binding.destinationId])).rowCount;
    if(pending)throw Error('DELIVERY_REQUIRES_RECONCILIATION');return before>0;
  }
  async function reconcileLocal(batchId:string,signal:AbortSignal){
    if(!independentDelivery || signal.aborted)throw Error('STOPPED');assertUuid(batchId);
    const uncertain=(await project.query<{delivery_id:string}>("SELECT delivery_id FROM olos_export.projection_outbox WHERE destination_id=$1 AND batch_id=$2 AND state='uncertain'",[binding.destinationId,batchId])).rows;
    for(const row of uncertain){if(signal.aborted)throw Error('STOPPED');if(!await delivery.dispatcher.reconcile(row.delivery_id))throw Error('DELIVERY_REQUIRES_RECONCILIATION');}
    if((await project.query("SELECT 1 FROM olos_export.projection_outbox WHERE destination_id=$1 AND batch_id=$2 AND state IN ('rejected','uncertain') LIMIT 1",[binding.destinationId,batchId])).rowCount)throw Error('DELIVERY_REQUIRES_RECONCILIATION');
  }
  return {coordinator,writer,reconcileLocal,async materializeLocal(signal:AbortSignal):Promise<CycleCounts>{
    if(!independentDelivery || signal.aborted)throw Error('LOCAL_REPLAY_REQUIRED');
    if(await spool.active()){
      const recovered=await coordinator.recover(signal);if(!recovered.result)throw Error('RECOVERY_REQUIRED');return recovered.result;
    }
    const source=binding.source();
    if(!isTdsResponseReplay(source) && !(binding.kind==='isolated-fixture' && source.kind==='synthetic'))throw Error('LOCAL_REPLAY_REQUIRED');
    return coordinator.receive(binding.plan(),source,signal);
  },async deliverStep(batchId:string,signal:AbortSignal){
    await reconcileLocal(batchId,signal);
    const count=await delivery.dispatcher.runBatch(signal,projectionClaimBatchSize,batchId);
    const pending=(await project.query("SELECT 1 FROM olos_export.projection_outbox WHERE destination_id=$1 AND batch_id=$2 AND state<>'confirmed' LIMIT 1",[binding.destinationId,batchId])).rowCount;
    if((await project.query("SELECT 1 FROM olos_export.projection_outbox WHERE destination_id=$1 AND batch_id=$2 AND state IN ('rejected','uncertain') LIMIT 1",[binding.destinationId,batchId])).rowCount)throw Error('DELIVERY_REQUIRES_RECONCILIATION');
    return {count,complete:!pending};
  },async recoverLocal(signal:AbortSignal):Promise<CycleCounts>{
    const r=await coordinator.recover(signal);if(r.manifest && !r.result)throw Error('RECOVERY_REQUIRED');await drain(signal);return r.result??empty;
  },async cycle(signal:AbortSignal):Promise<CycleCounts>{
    if(signal.aborted)throw Error('STOPPED');
    // Local recovery gets its own cycle; never consume remotely to replace uncertain data.
    if(await spool.active()){
      const r=await coordinator.recover(signal);if(!r.result)throw Error('RECOVERY_REQUIRED');await drain(signal);return r.result;
    }
    const orphan=(await ingest.query("SELECT 1 FROM olos_export.runs WHERE source_table='AttemptsRawData' AND state IN ('read_intent','persisting','needs_reconciliation') LIMIT 1")).rowCount;
    if(orphan)throw Error('RECOVERY_AFTER_EXPIRED_LEASE_OR_HARD_CRASH_REQUIRES_REVIEW');
    if(await drain(signal))return empty;
    const plan=binding.plan();if(plan.run.table!=='AttemptsRawData')throw Error('SOURCE_TABLE_BLOCKED');
    if(binding.kind==='corporate-local'){
      assertCorporateBinding(binding);
      // Detect known local refusals before recording a remote attempt; coordinator rechecks against races.
      await writer.preflight(plan);
    }
    await binding.beforeAcquire?.(plan,signal);
    const result=await coordinator.receive(plan,binding.source(),signal);
    await drain(signal);await binding.afterAcquire?.(result);return {...result,acquisitionPerformed:true};
  }};
}
export function createAutomatedAttemptsService(pools:RuntimePools,spool:SpoolRepository,limits:SpoolLimits,binding:AttemptsBinding,intervalMs=60000){
  const pipeline=createAttemptsPipeline(pools.ingest,pools.project,pools.receiver,spool,limits,binding);
  return new AttemptsService(pools,intervalMs,signal=>pipeline.cycle(signal),binding.kind);
}
