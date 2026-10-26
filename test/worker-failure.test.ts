import {afterEach,it,expect,vi} from 'vitest';
import {randomUUID} from 'node:crypto';
import type {Pool,PoolClient} from 'pg';
import {annotateFailure,failureDetails,logWorkerFailure,preserveFailure} from '../src/diagnostics/worker-failure.js';
import {PersistenceError} from '../src/persistence/types.js';
import {transaction} from '../src/persistence/transaction.js';
import {isRecoverableClaimFailure} from '../src/projection/dispatcher.js';
import {AttemptsRuntimeControl} from '../src/integration/attempts/runtime-control.js';
afterEach(()=>{vi.restoreAllMocks();vi.useRealTimers();});

it('structured diagnostics retain safe codes, stage and IDs through wrappers, never driver values or stacks',()=>{
 const batchId=randomUUID(),captureId=randomUUID(),secret='password=secret CPF=12345678901 email=private@example.test';
 const driver=Object.assign(Error(secret),{code:'23514',detail:secret,query:secret});
 const persistence=preserveFailure(new PersistenceError('PERSISTENCE_FAILED'),driver);
 const wrapped=preserveFailure(Error('DELIVERY_REQUIRES_RECONCILIATION'),persistence);
 annotateFailure(wrapped,{stage:'DESTINATION',batchId});annotateFailure(wrapped,{flow:'delivery',stage:'DELIVER',captureId});
 const output=vi.spyOn(process.stderr,'write').mockReturnValue(true);
 const logged=logWorkerFailure('worker_needs_reconciliation',wrapped);
 expect(logged).toMatchObject({flow:'delivery',stage:'DESTINATION',batchId,captureId,code:'DELIVERY_REQUIRES_RECONCILIATION',technicalCode:'23514',technicalMessage:'PostgreSQL check constraint violation',retryRemote:false});
 expect(output).toHaveBeenCalledOnce();expect(JSON.stringify(logged)).not.toContain(secret);
 expect(logged).not.toHaveProperty('stack');expect(logged).not.toHaveProperty('detail');expect(logged).not.toHaveProperty('query');
 expect(failureDetails(Error(secret),{captureId:secret})).toEqual({code:'EXECUTION_FAILED',message:'Failure details omitted'});
});
it('recognizes the exact client timeout without trusting arbitrary messages or codes',()=>{
 expect(failureDetails(Error('Query read timeout'))).toMatchObject({code:'PG_QUERY_TIMEOUT'});
 for(const code of ['12345678901','TOKEN','password=secret'])expect(failureDetails(Object.assign(Error('secret'),{code}))).toMatchObject({code:'EXECUTION_FAILED'});
});
it.each(['rollback','rollback-lost','commit-lost'] as const)('only confirmed claim rollback can be isolated (%s)',async point=>{
 const driver=Object.assign(Error('sensitive query'),{code:'55P03'});
 const query=vi.fn(async(sql:string)=>{if(sql==='ROLLBACK' && point==='rollback-lost' || sql==='COMMIT' && point==='commit-lost')throw driver;return {rows:[]};});
 const client={query,release:vi.fn()} as unknown as PoolClient;
 const pool={connect:vi.fn(async()=>client)} as unknown as Pool;
 let error:unknown;try{await transaction(pool,async()=>{if(point!=='commit-lost')throw driver;});}catch(e){error=e;}
 annotateFailure(error,{flow:'delivery',stage:'CLAIM',batchId:randomUUID()});
 expect(failureDetails(error)).toMatchObject({technicalCode:'55P03'});
 expect(isRecoverableClaimFailure(error)).toBe(point==='rollback');
 if(point==='commit-lost')expect(error).toMatchObject({code:'COMMIT_UNKNOWN',outcome:'unknown'});
 else expect(error).toMatchObject({code:point==='rollback'?'BUSY':'PERSISTENCE_FAILED',outcome:'not_committed'});
 expect(client.release).toHaveBeenCalledWith(point!=='rollback');
});
it.each(['AUTHORIZE','SETTLE','CHECKPOINT','MATERIALIZE'] as const)('BUSY at %s cannot bypass global stop',stage=>{
 const error=preserveFailure(new PersistenceError('BUSY'),{code:'55P03'});annotateFailure(error,{stage});
 expect(isRecoverableClaimFailure(error)).toBe(false);
 expect(isRecoverableClaimFailure(annotateFailure(new PersistenceError('LEASE_LOST'),{stage:'CLAIM'}))).toBe(false);
});
it('the first failure remains visible after cancellation cascades and no new acquisition runs',async()=>{
 vi.useFakeTimers();const output=vi.spyOn(process.stderr,'write').mockReturnValue(true),control=new AttemptsRuntimeControl(120000),batchId=randomUUID();
 let calls=0;
 try{control.start(async()=>{calls++;throw annotateFailure(new PersistenceError('CHECKPOINT_CONFLICT'),{flow:'delivery',stage:'CHECKPOINT',batchId});});
  await vi.advanceTimersByTimeAsync(120000);const first=control.snapshot().lastFailure;
  control.requireReconciliation(Error('STOPPED'),{flow:'acquisition',stage:'ACQUIRE'});
  await vi.advanceTimersByTimeAsync(360000);
  expect(calls).toBe(1);expect(control.snapshot().lastFailure).toEqual(first);
  expect(first).toMatchObject({code:'CHECKPOINT_CONFLICT',flow:'delivery',stage:'CHECKPOINT',batchId});expect(output).toHaveBeenCalledOnce();
 }finally{await control.stop();}
});
