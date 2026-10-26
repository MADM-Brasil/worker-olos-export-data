import {randomUUID} from 'node:crypto';
import type {Pool,PoolClient} from 'pg';
import {z} from 'zod';
import type {SpoolManifest,SpoolRepository} from '../large-batch/types.js';
import {manifestDigest} from './large-batch.js';
import {canonical,digest,assertContract} from './validation.js';
import {PersistenceError,type RunHandle,type SourceContract} from './types.js';
import {transaction} from './transaction.js';

export const localReconciliationApprovalSchema=z.object({
 version:z.literal('olos-local-ingestion-reconciliation/v1'),authorizationId:z.string().uuid(),
 captureId:z.string().uuid(),batchId:z.string().uuid(),runId:z.string().uuid(),recoveryCaseId:z.string().uuid(),
 manifestDigest:z.string().regex(/^[a-f0-9]{64}$/),expectedGeneration:z.string().max(19).regex(/^[0-9]+$/),
 operatorReference:z.string().regex(/^[A-Za-z0-9_.:/-]{3,128}$/),
 authorizedAt:z.string().datetime(),expiresAt:z.string().datetime(),
 decision:z.literal('AUTHORIZE_VERIFIED_LOCAL_REPLAY'),remoteReplayAllowed:z.literal(false)
}).strict();
export type LocalReconciliationApproval=z.infer<typeof localReconciliationApprovalSchema>;
const refuse=():never=>{throw new PersistenceError('RECOVERY_REQUIRED');};

/** Source-row lock is the transaction completion barrier, not receipt absence alone.
 * Only the initial (version zero) sealed AttemptsRawData ingestion is supported.
 * The open original case blocks normal startRun throughout preparation/replay. */
export async function reconcileLocalIngestion(pool:Pick<Pool,'connect'>,spool:SpoolRepository,m:SpoolManifest,input:unknown,signal:AbortSignal):Promise<RunHandle>{
 const a=localReconciliationApprovalSchema.parse(input),i=m.identity;
 if(signal.aborted || !m.sealed || !['uncertain','persisting','sealed','committed'].includes(m.state) ||
   i.plan.run.table!=='AttemptsRawData' || i.expected.version!=='0' || i.expected.cursor!==null ||
   a.batchId!==i.batchId || a.runId!==i.runId || a.manifestDigest!==manifestDigest(m) ||
   Date.parse(a.authorizedAt)>Date.now() || Date.parse(a.expiresAt)<=Date.now() ||
   Date.parse(a.expiresAt)-Date.parse(a.authorizedAt)>86400000)refuse();
 // Exhaust the verified local file BEFORE changing any database identity. Replay revalidates it again.
 for await(const _ of spool.records(m)){if(signal.aborted)refuse();}
 return transaction(pool,async c=>{
  const lock=(await c.query(`SELECT *,lease_until>clock_timestamp() AS live FROM olos_export.source_locks
    WHERE source_table='AttemptsRawData' FOR UPDATE NOWAIT`)).rows[0];
  if(!lock)refuse();
  const original=(await c.query(`SELECT r.*,s.read_type,s.export_date::text FROM olos_export.runs r
    JOIN olos_export.streams s ON s.id=r.stream_id WHERE r.id=$1 FOR UPDATE OF r NOWAIT`,[i.runId])).rows[0];
  const recovery=(await c.query('SELECT * FROM olos_export.recovery_cases WHERE id=$1 FOR UPDATE NOWAIT',[a.recoveryCaseId])).rows[0];
  const prior=(await c.query('SELECT * FROM olos_export.local_ingestion_reconciliations WHERE authorization_id=$1',[a.authorizationId])).rows[0];
  if(!original || original.stream_id!==i.streamId || original.source_table!==i.plan.run.table ||
    original.contract_version!==i.plan.run.contractVersion || original.fencing_token!==i.fencingToken ||
    original.read_type!==i.plan.run.readType || original.export_date!==i.plan.run.exportDate ||
    canonical(original.pk_range)!==canonical(i.plan.run.pkRange) || recovery?.run_id!==i.runId)refuse();
  if(prior){
   if(prior.authorization_digest!==digest(a) || prior.manifest_digest!==manifestDigest(m))refuse();
   const receipt=(await c.query(`SELECT b.state,x.manifest_digest FROM olos_export.batches b
     JOIN olos_export.large_batch_receipts x ON x.batch_id=b.id WHERE b.id=$1 AND b.run_id=$2`,[i.batchId,i.runId])).rows[0];
   // Lost COMMIT acknowledgment: a matching durable result may be read, never re-ingested.
   if(receipt){if(receipt.state!=='committed' || receipt.manifest_digest!==manifestDigest(m) || recovery.state!=='resolved' || original.state!=='succeeded')refuse();}
   else if(recovery.state!=='open' || lock.owner_run_id!==prior.recovery_run_id || lock.generation!==prior.fencing_token || !lock.live)refuse();
   return {runId:prior.recovery_run_id as string,streamId:i.streamId,fencingToken:prior.fencing_token as string,signal};
  }
  if(lock.generation!==a.expectedGeneration || BigInt(lock.generation)<=BigInt(i.fencingToken) ||
    recovery.state!=='open' || recovery.reason!=='expired_read_intent' || original.state!=='needs_reconciliation' ||
    ['received_count','persisted_count','duplicate_count','rejected_count'].some(k=>original[k]!=='0'))refuse();
  const chain=(await c.query('SELECT * FROM olos_export.local_ingestion_reconciliations WHERE original_run_id=$1',[i.runId])).rows;
  if(chain.some(r=>r.original_batch_id!==i.batchId || r.manifest_digest!==manifestDigest(m) || r.capture_id!==a.captureId || r.case_id!==a.recoveryCaseId))refuse();
  const previous=chain.map(r=>r.recovery_run_id as string);
  const allowed=[i.runId,...previous];
  // A successor's lock is never stolen; an expired local recovery may be explicitly re-reconciled.
  if(lock.owner_run_id && (!previous.includes(lock.owner_run_id) || lock.live))refuse();
  const runs=(await c.query(`SELECT * FROM olos_export.runs WHERE id=ANY($1::uuid[]) FOR UPDATE NOWAIT`,[previous])).rows;
  if(runs.some(r=>!['read_intent','needs_reconciliation','failed'].includes(r.state) || ['received_count','persisted_count','duplicate_count','rejected_count'].some(k=>r[k]!=='0')))refuse();
  if((await c.query(`SELECT 1 FROM olos_export.runs WHERE source_table='AttemptsRawData'
    AND state IN ('read_intent','persisting','needs_reconciliation') AND NOT(id=ANY($1::uuid[])) LIMIT 1`,[allowed])).rowCount)refuse();
  if((await c.query(`SELECT 1 FROM olos_export.recovery_cases r JOIN olos_export.runs u ON u.id=r.run_id
    WHERE u.source_table='AttemptsRawData' AND r.state='open' AND NOT(u.id=ANY($1::uuid[])) LIMIT 1`,[allowed])).rowCount)refuse();
  const cp=(await c.query('SELECT * FROM olos_export.checkpoints WHERE stream_id=$1 FOR UPDATE NOWAIT',[i.streamId])).rows[0];
  if(!cp || cp.version!=='0' || cp.cursor_value!==null || cp.last_batch_id!==null || cp.last_success_at!==null)throw new PersistenceError('CHECKPOINT_CONFLICT');
  // Check all provenance roots and their descendants, including receipts/outbox/audit/envelopes.
  // FKs + atomic ingestion mean delivery cannot precede a committed projection batch.
  const effects=(await c.query(`SELECT
    EXISTS(SELECT 1 FROM olos_export.batches WHERE id=$1 OR run_id=ANY($2::uuid[]) OR stream_id=$3) OR
    EXISTS(SELECT 1 FROM olos_export.large_batch_receipts WHERE batch_id=$1) OR
    EXISTS(SELECT 1 FROM olos_export.large_batch_keys WHERE batch_id=$1) OR
    EXISTS(SELECT 1 FROM olos_export.attempts_raw_data WHERE last_batch_id=$1) OR
    EXISTS(SELECT 1 FROM olos_export.projection_batches WHERE batch_id=$1) OR
    EXISTS(SELECT 1 FROM olos_export.projection_outbox WHERE batch_id=$1) OR
    EXISTS(SELECT 1 FROM olos_export.projection_events e JOIN olos_export.projection_outbox o ON o.delivery_id=e.delivery_id WHERE o.batch_id=$1) OR
    EXISTS(SELECT 1 FROM olos_export.delivery_receipts d JOIN olos_export.projection_outbox o ON o.delivery_id=d.delivery_id AND o.destination_id=d.destination_id WHERE o.batch_id=$1) OR
    EXISTS(SELECT 1 FROM olos_export.audit_events WHERE (run_id=ANY($2::uuid[]) OR batch_id=$1) AND event='batch_committed') OR
    EXISTS(SELECT 1 FROM olos_export.sync_errors WHERE run_id=ANY($2::uuid[]) AND (batch_id IS NOT NULL OR received_count<>0 OR rejected_count<>0)) AS present`,[i.batchId,allowed,i.streamId])).rows[0];
  if(effects.present)refuse();
  const contract=(await c.query<SourceContract>('SELECT * FROM olos_export.source_contracts WHERE source_table=$1 AND version=$2',[i.plan.run.table,i.plan.contract.version])).rows[0];
  if(!contract || canonical(contract)!==canonical(i.plan.contract))refuse();assertContract(contract!);
  if(signal.aborted || Date.parse(a.expiresAt)<=Date.now())refuse();
  const runId=randomUUID(),token=(BigInt(lock.generation)+1n).toString();
  const proof={protocol:'source-lock-barrier/v1',generation:lock.generation,checkpoint:{version:'0',cursor:null,lastBatchId:null},effectsAbsent:true,originalRunId:i.runId,previousRecoveryRuns:previous,manifestDigest:manifestDigest(m)};
  // All proof checks completed. Proof and new local identity commit atomically.
  await c.query(`INSERT INTO olos_export.runs(id,stream_id,source_table,contract_version,fencing_token,state,pk_range)
    VALUES ($1,$2,'AttemptsRawData',$3,$4,'read_intent',$5::jsonb)`,[runId,i.streamId,i.plan.run.contractVersion,token,JSON.stringify(i.plan.run.pkRange)]);
  await c.query(`INSERT INTO olos_export.local_ingestion_reconciliations(authorization_id,authorization_digest,capture_id,case_id,original_run_id,original_batch_id,
    manifest_digest,original_manifest,recovery_run_id,previous_generation,fencing_token,proof,operator_reference,authorized_at,expires_at)
    VALUES ($1,$2,$3,$4,$5,$6,$7,$8::jsonb,$9,$10,$11,$12::jsonb,$13,$14,$15)`,
    [a.authorizationId,digest(a),a.captureId,a.recoveryCaseId,i.runId,i.batchId,manifestDigest(m),JSON.stringify(m),runId,lock.generation,token,JSON.stringify(proof),a.operatorReference,a.authorizedAt,a.expiresAt]);
  await c.query(`UPDATE olos_export.runs SET state='failed',finished_at=clock_timestamp() WHERE id=ANY($1::uuid[])`,[previous]);
  await c.query(`UPDATE olos_export.source_locks SET generation=$1,owner_run_id=$2,lease_until=clock_timestamp()+interval '5 minutes' WHERE source_table='AttemptsRawData'`,[token,runId]);
  await c.query(`INSERT INTO olos_export.recovery_attempts(id,case_id,action,result,evidence_digest) VALUES ($1,$2,'manual_review','evidence_recorded',$3)`,[randomUUID(),a.recoveryCaseId,digest(proof)]);
  await c.query('UPDATE olos_export.recovery_cases SET attempts=attempts+1 WHERE id=$1',[a.recoveryCaseId]);
  return {runId,streamId:i.streamId,fencingToken:token,signal};
 });
}

/** Checked before receipt lookup and again under the materialization source lock. */
export async function assertLocalReconciliation(c:PoolClient,m:SpoolManifest,h:RunHandle){
 const row=(await c.query(`SELECT x.*,r.state AS original_state,k.state AS case_state,(x.expires_at>clock_timestamp()) AS approval_valid FROM olos_export.local_ingestion_reconciliations x
   JOIN olos_export.runs r ON r.id=x.original_run_id JOIN olos_export.recovery_cases k ON k.id=x.case_id
   WHERE x.recovery_run_id=$1 AND x.fencing_token=$2`,[h.runId,h.fencingToken])).rows[0];
 if(!row || row.original_run_id!==m.identity.runId || row.original_batch_id!==m.identity.batchId || row.manifest_digest!==manifestDigest(m) ||
   (row.case_state==='open' && !row.approval_valid) || !['needs_reconciliation','succeeded'].includes(row.original_state) || !['open','resolved'].includes(row.case_state))refuse();
 return row;
}
