import {z} from 'zod';
import type {Pool,PoolClient} from 'pg';
import type {SpoolManifest} from '../large-batch/types.js';
import {manifestDigest} from './large-batch.js';
import {canonical,digest} from './validation.js';
import {PersistenceError} from './types.js';
import {transaction} from './transaction.js';

const sha=z.string().regex(/^[a-f0-9]{64}$/),version=z.string().max(19).regex(/^(0|[1-9][0-9]*)$/);
export const administrativeCaptureDiscardSchema=z.object({
 version:z.literal('olos-administrative-capture-discard/v1'),authorizationId:z.string().uuid(),captureId:z.string().uuid(),
 batchId:z.string().uuid(),runId:z.string().uuid(),recoveryCaseId:z.string().uuid(),streamId:z.string().uuid(),
 manifestDigest:sha,expectedGeneration:version,operatorReference:z.string().regex(/^[A-Za-z0-9_.:/-]{3,128}$/),
 reason:z.literal('OPERATOR_DECLINED_HISTORICAL_IMPORT'),authorizedAt:z.string().datetime(),expiresAt:z.string().datetime(),
 decision:z.literal('AUTHORIZE_ADMINISTRATIVE_CAPTURE_DISCARD'),remoteReplayAllowed:z.literal(false)
}).strict();
export type AdministrativeCaptureDiscardApproval=z.infer<typeof administrativeCaptureDiscardSchema>;
export function validateAdministrativeCaptureDiscard(input:unknown,now=Date.now()){
 const a=administrativeCaptureDiscardSchema.parse(input),from=Date.parse(a.authorizedAt),to=Date.parse(a.expiresAt);
 if(from>now || to<=now || to<=from || to-from>3600000)throw Error('ADMINISTRATIVE_DISCARD_AUTHORIZATION_INVALID');
 return a;
}
export const discardLocalEvidenceSchema=z.object({captureId:z.string().uuid(),configurationDigest:sha,captureSha256:sha,
 intentDigest:sha,reviewDigest:sha,importStartedDigest:sha}).strict();
export type DiscardLocalEvidence=z.infer<typeof discardLocalEvidenceSchema>;
const proofSchema=z.object({protocol:z.literal('olos-discard-proof/v1'),manifestDigest:sha,local:discardLocalEvidenceSchema,
 checkpoint:z.object({version,cursor:z.string().nullable(),lastBatchId:z.string().uuid().nullable()}).strict(),
 priorBatchIds:z.array(z.string().uuid()),effectsAbsent:z.literal(true)}).strict();
const refuse=():never=>{throw new PersistenceError('RECOVERY_REQUIRED');};

/** Under source lock: receipt absence is insufficient. Every protocol provenance root must be absent.
 * FKs and immutable batches/outbox prevent indirect delivery effects without these roots. */
async function assertNoEffects(c:PoolClient,m:SpoolManifest){
 const i=m.identity;
 const effects=(await c.query(`SELECT
  EXISTS(SELECT 1 FROM olos_export.batches WHERE id=$1 OR run_id=$2) OR
  EXISTS(SELECT 1 FROM olos_export.large_batch_receipts WHERE batch_id=$1) OR
  EXISTS(SELECT 1 FROM olos_export.large_batch_keys WHERE batch_id=$1) OR
  EXISTS(SELECT 1 FROM olos_export.attempts_raw_data WHERE last_batch_id=$1) OR
  EXISTS(SELECT 1 FROM olos_export.projection_batches WHERE batch_id=$1) OR
  EXISTS(SELECT 1 FROM olos_export.projection_outbox WHERE batch_id=$1) OR
  EXISTS(SELECT 1 FROM olos_export.projection_events e JOIN olos_export.projection_outbox o USING(delivery_id) WHERE o.batch_id=$1) OR
  EXISTS(SELECT 1 FROM olos_export.delivery_receipts d JOIN olos_export.projection_outbox o USING(delivery_id,destination_id) WHERE o.batch_id=$1) OR
  EXISTS(SELECT 1 FROM olos_export.audit_events WHERE (run_id=$2 OR batch_id=$1) AND (event<>'reconciliation_required' OR batch_id IS NOT NULL OR received_count<>0 OR persisted_count<>0 OR duplicate_count<>0)) OR
  EXISTS(SELECT 1 FROM olos_export.sync_errors WHERE run_id=$2) OR
  EXISTS(SELECT 1 FROM olos_export.local_ingestion_reconciliations WHERE original_run_id=$2 OR recovery_run_id=$2 OR original_batch_id=$1)
  AS present`,[i.batchId,i.runId])).rows[0];
 if(effects.present)refuse();
}

/** Read/lock the real contiguous ingestion history, never advance a checkpoint. */
async function position(c:PoolClient,m:SpoolManifest){
 const cp=(await c.query('SELECT * FROM olos_export.checkpoints WHERE stream_id=$1 FOR UPDATE NOWAIT',[m.identity.streamId])).rows[0];
 if(!cp)refuse();
 const batches=(await c.query(`SELECT b.*,r.state AS run_state,x.manifest_digest,x.data_digest,x.record_count,x.format_version,
  (SELECT count(*)::int FROM olos_export.projection_batches p WHERE p.batch_id=b.id) AS projections,
  (SELECT count(*)::int FROM olos_export.projection_batches p WHERE p.batch_id=b.id
    AND p.source_table=b.source_table AND p.source_contract_version=r.contract_version
    AND p.destination_id='local.olos_worker.raw.olos_calls' AND p.source_namespace='olos.ExportDataV3.AttemptsRawData'
    AND p.mapping_version='local-approved-16/v1' AND p.row_count=(SELECT count(*) FROM olos_export.projection_outbox o WHERE o.batch_id=p.batch_id AND o.destination_id=p.destination_id)) AS valid_projections,
  EXISTS(SELECT 1 FROM olos_export.projection_outbox o WHERE o.batch_id=b.id AND o.state<>'confirmed') AS pending
  FROM olos_export.batches b JOIN olos_export.runs r ON r.id=b.run_id
  LEFT JOIN olos_export.large_batch_receipts x ON x.batch_id=b.id
  WHERE b.stream_id=$1 ORDER BY b.expected_version,b.id FOR SHARE OF b`,[m.identity.streamId])).rows;
 let next=0n,cursor:string|null=null;
 for(const b of batches){
  if(b.expected_version!==String(next) || b.previous_cursor!==cursor || b.state!=='committed' || b.run_state!=='succeeded' ||
    b.digest!==b.manifest_digest || !sha.safeParse(b.data_digest).success || b.format_version!=='olos-spool/v1' ||
    b.record_count!==b.received_count || b.received_count!==b.persisted_count+b.duplicate_count || b.rejected_count!==0 ||
    b.projections!==1 || b.valid_projections!==1 || (BigInt(b.expected_version)<BigInt(cp.version) && b.pending))refuse();
  next++;cursor=b.next_cursor;
 }
 const checkpointIndex=BigInt(cp.version);
 if(checkpointIndex>next || checkpointIndex<0n)refuse();
 const last=checkpointIndex===0n?undefined:batches[Number(checkpointIndex)-1];
 if(cp.last_batch_id!==(last?.id??null) || cp.cursor_value!==(last?.next_cursor??null) ||
   (checkpointIndex===0n ? cp.last_success_at!==null : cp.last_success_at===null))refuse();
 return {checkpoint:{version:cp.version as string,cursor:cp.cursor_value as string|null,lastBatchId:cp.last_batch_id as string|null},
  batches,tip:{version:String(next),cursor}};
}

function bound(m:SpoolManifest,a:AdministrativeCaptureDiscardApproval,local:DiscardLocalEvidence){
 const i=m.identity;
 if(!m.sealed || m.state!=='uncertain' || !sha.safeParse(m.sha256).success || !Number.isSafeInteger(m.rows) || m.rows<0 || m.rows>200000 ||
  a.captureId!==local.captureId || a.batchId!==i.batchId || a.runId!==i.runId || a.streamId!==i.streamId || a.manifestDigest!==manifestDigest(m) ||
  i.plan.run.table!=='AttemptsRawData' || !version.safeParse(i.expected.version).success)refuse();
}
async function state(c:PoolClient,m:SpoolManifest,a:AdministrativeCaptureDiscardApproval,confirmed:boolean){
 const i=m.identity;
 const run=(await c.query(`SELECT r.*,s.read_type,s.export_date::text FROM olos_export.runs r JOIN olos_export.streams s ON s.id=r.stream_id
  WHERE r.id=$1 FOR UPDATE OF r NOWAIT`,[i.runId])).rows[0];
 const recovery=(await c.query('SELECT * FROM olos_export.recovery_cases WHERE id=$1 FOR UPDATE NOWAIT',[a.recoveryCaseId])).rows[0];
 if(!run || run.stream_id!==i.streamId || run.source_table!==i.plan.run.table || run.fencing_token!==i.fencingToken ||
  run.contract_version!==i.plan.run.contractVersion || run.read_type!==i.plan.run.readType || run.export_date!==i.plan.run.exportDate ||
  canonical(run.pk_range)!==canonical(i.plan.run.pkRange) || run.state!==(confirmed?'failed':'needs_reconciliation') ||
  ['received_count','persisted_count','duplicate_count','rejected_count'].some(k=>run[k]!=='0') || (confirmed && !run.finished_at) ||
  !recovery || recovery.run_id!==i.runId || recovery.reason!=='expired_read_intent' || recovery.state!==(confirmed?'resolved':'open') ||
  (confirmed ? !recovery.resolved_at : recovery.resolved_at!==null))refuse();
 await assertNoEffects(c,m);
}
function evidence(row:Record<string,any>,m:SpoolManifest,local:DiscardLocalEvidence){
 const a=administrativeCaptureDiscardSchema.parse(row.authorization_document),p=proofSchema.parse(row.proof);
 bound(m,a,local);
 if(row.configuration_digest!==local.configurationDigest || row.authorization_digest!==digest(a) || row.proof_digest!==digest(p) || row.manifest_digest!==manifestDigest(m) ||
  canonical(row.original_manifest)!==canonical(m) || p.manifestDigest!==manifestDigest(m) || canonical(p.local)!==canonical(local) ||
  row.capture_id!==a.captureId || row.batch_id!==a.batchId || row.original_run_id!==a.runId || row.stream_id!==a.streamId ||
  row.recovery_case_id!==a.recoveryCaseId || row.authorization_id!==a.authorizationId || row.expected_generation!==a.expectedGeneration ||
  row.operator_reference!==a.operatorReference || row.reason!==a.reason || BigInt(a.expectedGeneration)<=BigInt(m.identity.fencingToken))refuse();
 // Expiry blocks a new transition, not recognition of an already committed decision.
 validateAdministrativeCaptureDiscard(a,new Date(row.discarded_at as string).getTime());
 return {a,p};
}
async function sourceLock(c:PoolClient){
 const lock=(await c.query("SELECT * FROM olos_export.source_locks WHERE source_table='AttemptsRawData' FOR UPDATE NOWAIT")).rows[0];
 if(!lock)refuse();return lock;
}

export async function discardAdministrativeCapture(pool:Pick<Pool,'connect'>,m:SpoolManifest,input:unknown,signal:AbortSignal,localInput:DiscardLocalEvidence){
 const a=administrativeCaptureDiscardSchema.parse(input),local=discardLocalEvidenceSchema.parse(localInput);bound(m,a,local);
 if(signal.aborted)refuse();
 return transaction(pool,async c=>{
  const lock=await sourceLock(c);
  // Source-row lock serializes writers; append-only evidence requires SELECT, never FOR UPDATE.
  const existing=(await c.query(`SELECT * FROM olos_export.administrative_capture_discards
   WHERE capture_id=$1 OR batch_id=$2 OR original_run_id=$3 OR authorization_id=$4 OR recovery_case_id=$5`,
   [a.captureId,a.batchId,a.runId,a.authorizationId,a.recoveryCaseId])).rows;
  if(existing.length){
   if(existing.length!==1)refuse();const {a:stored}=evidence(existing[0]!,m,local);
   if(digest(stored)!==digest(a))refuse();await state(c,m,a,true);
   if(BigInt(lock.generation)<BigInt(a.expectedGeneration) || signal.aborted)refuse();
   return {discarded:true,alreadyDiscarded:true,captureId:a.captureId,batchId:a.batchId,authorizationId:a.authorizationId};
  }
  validateAdministrativeCaptureDiscard(a);
  if(lock.owner_run_id!==null || lock.lease_until!==null || lock.generation!==a.expectedGeneration || BigInt(lock.generation)<=BigInt(m.identity.fencingToken))refuse();
  await state(c,m,a,false);
  if((await c.query(`SELECT 1 FROM olos_export.runs WHERE source_table='AttemptsRawData' AND id<>$1 AND state IN ('read_intent','persisting','needs_reconciliation')
   UNION ALL SELECT 1 FROM olos_export.recovery_cases c JOIN olos_export.runs r ON r.id=c.run_id
   WHERE r.source_table='AttemptsRawData' AND c.state='open' AND c.id<>$2 LIMIT 1`,[a.runId,a.recoveryCaseId])).rowCount)refuse();
  const pos=await position(c,m);
  if(pos.tip.version!==m.identity.expected.version || pos.tip.cursor!==m.identity.expected.cursor)refuse();
  const proof={protocol:'olos-discard-proof/v1',manifestDigest:manifestDigest(m),local,checkpoint:pos.checkpoint,priorBatchIds:pos.batches.map(b=>b.id as string),effectsAbsent:true};
  if(signal.aborted)refuse();validateAdministrativeCaptureDiscard(a);
  if(!(await c.query('SELECT $1::timestamptz<=clock_timestamp() AND clock_timestamp()<$2::timestamptz AS valid',[a.authorizedAt,a.expiresAt])).rows[0].valid)refuse();
  // No lock/token/checkpoint/receipt changes. Authorization, proof and terminal states commit together.
  await c.query(`INSERT INTO olos_export.administrative_capture_discards(capture_id,batch_id,original_run_id,recovery_case_id,stream_id,
   manifest_digest,original_manifest,authorization_id,authorization_digest,authorization_document,expected_generation,operator_reference,reason,proof,proof_digest,configuration_digest)
   VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8,$9,$10::jsonb,$11,$12,$13,$14::jsonb,$15,$16)`,
   [a.captureId,a.batchId,a.runId,a.recoveryCaseId,a.streamId,a.manifestDigest,JSON.stringify(m),a.authorizationId,digest(a),JSON.stringify(a),a.expectedGeneration,a.operatorReference,a.reason,JSON.stringify(proof),digest(proof),local.configurationDigest]);
  await c.query("UPDATE olos_export.runs SET state='failed',finished_at=clock_timestamp() WHERE id=$1",[a.runId]);
  await c.query("UPDATE olos_export.recovery_cases SET state='resolved',resolved_at=clock_timestamp() WHERE id=$1",[a.recoveryCaseId]);
  if(signal.aborted)refuse();validateAdministrativeCaptureDiscard(a);
  return {discarded:true,alreadyDiscarded:false,captureId:a.captureId,batchId:a.batchId,authorizationId:a.authorizationId};
 });
}

/** Startup recognition requires the durable ledger and exact local evidence, not a filesystem flag. */
export async function assertAdministrativeDiscard(pool:Pick<Pool,'connect'>,m:SpoolManifest,local:DiscardLocalEvidence){
 return transaction(pool,async c=>{
  const lock=await sourceLock(c);
  const row=(await c.query('SELECT * FROM olos_export.administrative_capture_discards WHERE capture_id=$1',[local.captureId])).rows[0];
  if(!row)refuse();const {a,p}=evidence(row,m,local);await state(c,m,a,true);
  if(BigInt(lock.generation)<BigInt(a.expectedGeneration))refuse();
  const pos=await position(c,m);
  if(BigInt(pos.checkpoint.version)<BigInt(p.checkpoint.version) || pos.batches.length<p.priorBatchIds.length ||
    p.priorBatchIds.some((id,index)=>pos.batches[index]?.id!==id) || String(p.priorBatchIds.length)!==m.identity.expected.version)refuse();
  return {captureId:a.captureId,authorizationId:a.authorizationId};
 });
}
