import { randomUUID } from 'node:crypto';
import { destinationVersion, type Candidate } from './contract.js';
import { ProjectionOutbox, projectionClaimBatchSize, type DeliveryClaim, type DeliveryMessage, type Receipt } from './outbox.js';
import type { ProjectionResult } from './transform.js';
import {PersistenceError} from '../persistence/types.js';
import {annotateFailure,failureDetails,preserveFailure,type FailureContext} from '../diagnostics/worker-failure.js';

/** Only a confirmed rollback of the claim transaction, before authorization or destination I/O. */
export function isRecoverableClaimFailure(error:unknown){
  const detail=failureDetails(error);
  return error instanceof PersistenceError && error.code==='BUSY' && error.outcome==='not_committed' &&
    detail.stage==='CLAIM' && detail.technicalCode==='55P03';
}

// Somente o adaptador que conhece o transporte pode classificar ausência de commit.
// Erro genérico ou pós-envio sempre é incerto.
export class TargetDeliveryError extends Error {
  constructor(readonly outcome: 'not_committed' | 'unknown') { super(outcome==='unknown' ? 'TARGET_UNKNOWN' : 'TARGET_NOT_COMMITTED'); }
}
export interface DeliveryRequest {
  readonly deliveryId: string; readonly destinationId: string; readonly requestDigest: string;
  readonly contractVersion: string; readonly mappingVersion: string; readonly row: Readonly<Candidate>;
}
export interface DeliveryTarget {
  preflight(row: Readonly<Candidate>): 'allowed' | 'blocked' | 'rejected';
  deliver(request: DeliveryRequest): Promise<'confirmed' | 'rejected'>;
  deliverBatch?: (requests: readonly DeliveryRequest[]) => Promise<readonly ('confirmed'|'rejected')[]>;
  // Recibo deve estar no mesmo commit da escrita de negócio e vinculado ao digest.
  receipt(deliveryId: string, requestDigest: string): Promise<Receipt>;
}
export class ProjectionDispatcher {
  constructor(private readonly outbox: ProjectionOutbox, private readonly target: DeliveryTarget,
    private readonly project: (message: DeliveryMessage) => ProjectionResult, private readonly owner = randomUUID()) {}
  async runOnce(): Promise<'empty' | 'pending' | 'confirmed' | 'rejected' | 'uncertain'> {
    const claim = await this.outbox.claim(this.owner);
    if (!claim) return 'empty';
    return this.dispatch(claim);
  }
  /** Bounded SQL grouping inside the caller's instance lock; never concurrent imports.
   * Await the authorized group before returning, including during shutdown.
   * Unstarted claims retain their durable lease and can be reclaimed after expiry.
   */
  async runBatch(signal: AbortSignal, limit = projectionClaimBatchSize, batchId?: string): Promise<number> {
    let stage:FailureContext['stage']='CLAIM',currentBatch=batchId;
    try{
    if (signal.aborted) throw Error('STOPPED');
    if(!Number.isSafeInteger(limit) || limit<1 || limit>projectionClaimBatchSize)throw Error('VALIDATION_FAILED');
    if(!this.target.deliverBatch){
      if(batchId!==undefined)throw Error('BATCH_TARGET_REQUIRED');
      stage='DELIVER';
      const state=await this.runOnce();
      if(state!=='empty' && state!=='confirmed')throw Error('DELIVERY_REQUIRES_RECONCILIATION');
      if(signal.aborted)throw Error('STOPPED');
      return state==='empty'?0:1;
    }
    const claims = await this.outbox.claimBatch(this.owner,limit,batchId);
    if(claims.length && this.target.deliverBatch){
      currentBatch=claims[0]!.message.batch_id;stage='PREPARE';
      const items: {claim:DeliveryClaim;row:Readonly<Candidate>}[]=[];
      for(const claim of claims){
        if(signal.aborted)throw Error('STOPPED');
        const row=await this.prepare(claim,true);
        if(typeof row==='string')throw Error('DELIVERY_REQUIRES_RECONCILIATION');
        items.push({claim,row});
      }
      if(signal.aborted)throw Error('STOPPED');
      // Once authorized, finish the bounded transport and settlement even on SIGTERM.
      // An ambiguous authorization commit must never reach the destination.
      stage='AUTHORIZE';const authorized=await this.outbox.authorizeBatch(items);
      let states: ('confirmed'|'rejected'|'pending'|'uncertain')[];
      let targetError:unknown;stage='DESTINATION';
      try{
        const result=await this.target.deliverBatch(authorized.map((a,i)=>({deliveryId:a.id,
          destinationId:claims[i]!.message.destination_id,requestDigest:a.requestDigest,
          contractVersion:claims[i]!.message.destination_version,mappingVersion:claims[i]!.message.mapping_version,row:a.row})));
        if(result.length!==claims.length || result.some(s=>s!=='confirmed' && s!=='rejected'))throw Error('TARGET_UNKNOWN');
        states=[...result];
      }catch(error){targetError=error;states=claims.map(()=>error instanceof TargetDeliveryError && error.outcome==='not_committed'?'pending':'uncertain');}
      stage='SETTLE';
      await this.outbox.settleBatch(claims.map((claim,i)=>({claim,attemptId:authorized[i]!.attemptId,state:states[i]!})));
      if(states.some(s=>s!=='confirmed')){
        stage='DESTINATION';throw preserveFailure(Error('DELIVERY_REQUIRES_RECONCILIATION'),targetError??Error('PROJECTION_REJECTED'));
      }
      if(signal.aborted)throw Error('STOPPED');
      return claims.length;
    }
    if (signal.aborted) throw Error('STOPPED');
    return 0;
    }catch(error){throw annotateFailure(error,{flow:'delivery',stage,batchId:currentBatch});}
  }
  private async prepare(claim: DeliveryClaim,strict=false): Promise<Readonly<Candidate> | 'pending' | 'rejected'> {
    const refused=(state:'pending'|'rejected',code:string,cause?:unknown)=>{
      if(strict)throw preserveFailure(Error('DELIVERY_REQUIRES_RECONCILIATION'),cause??Error(code));return state;
    };
    if (claim.message.destination_version!==destinationVersion) { await this.outbox.defer(claim); return refused('pending','DESTINATION_VERSION_MISMATCH'); }
    let row: Readonly<Candidate>;
    if (claim.message.request_payload !== null) row = claim.message.request_payload;
    else {
      let result: ProjectionResult;
      try { result = this.project(claim.message); }
      catch(error) { await this.outbox.defer(claim,true); return refused('rejected','PROJECTION_REJECTED',error); }
      if (result.contractVersion!==destinationVersion || result.mappingVersion!==claim.message.mapping_version || result.status!=='ready' || !result.row) {
        await this.outbox.defer(claim,result.status==='rejected'); return refused(result.status==='rejected' ? 'rejected' : 'pending','PROJECTION_REJECTED');
      }
      row = result.row;
    }
    const preflight = this.target.preflight(row);
    if (preflight!=='allowed') { await this.outbox.defer(claim,preflight==='rejected',preflight==='blocked'); return refused(preflight==='rejected' ? 'rejected' : 'pending',preflight==='blocked'?'DESTINATION_BLOCKED':'PROJECTION_REJECTED'); }
    return row;
  }
  private async dispatch(claim: DeliveryClaim): Promise<'pending' | 'confirmed' | 'rejected' | 'uncertain'> {
    const row=await this.prepare(claim);
    if(typeof row==='string')return row;
    let authorized: Awaited<ReturnType<ProjectionOutbox['authorize']>>;
    try { authorized = await this.outbox.authorize(claim,row); }
    catch { return 'uncertain'; } // Não chamar destino se nem o commit da autorização foi confirmado.
    let state: 'confirmed' | 'rejected' | 'pending' | 'uncertain';
    try {
      state = await this.target.deliver({ deliveryId: claim.id,destinationId: claim.message.destination_id,
        requestDigest: authorized.requestDigest,contractVersion: claim.message.destination_version,
        mappingVersion: claim.message.mapping_version,row: authorized.row });
      if (state!=='confirmed' && state!=='rejected') state='uncertain';
    } catch (error) { state=error instanceof TargetDeliveryError && error.outcome==='not_committed' ? 'pending' : 'uncertain'; }
    // Sem timeout que solte transporte ainda pendente. O envio original deve terminar.
    try { await this.outbox.settle(claim,authorized.attemptId,state); }
    catch { return 'uncertain'; } // Ack local perdido: conciliar por recibo, não reenviar.
    return state;
  }
  async reconcile(id: string): Promise<boolean> {
    const pending = await this.outbox.uncertain(id);
    if (!pending) return false;
    let receipt: Receipt;
    try { receipt=await this.target.receipt(id,pending.requestDigest); } catch { receipt='unknown'; }
    return this.outbox.reconcile(id,pending.requestDigest,receipt);
  }
}
