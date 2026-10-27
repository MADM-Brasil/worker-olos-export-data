import type {Pool} from 'pg';
import {z} from 'zod';
import type {SpoolManifest} from '../large-batch/types.js';
import {canonical,digest} from './validation.js';
import {PersistenceError} from './types.js';
import {transaction} from './transaction.js';

const sha=z.string().regex(/^[a-f0-9]{64}$/);
export const notDispatchedEvidenceSchema=z.object({
 protocol:z.literal('olos-not-dispatched-closure/v1'),captureId:z.string().uuid(),configurationDigest:sha,
 intentDigest:sha,markerDigest:sha,diagnosticDigest:sha,proofDigest:sha,remoteStateDigest:sha,responseDigest:sha,
 manifestDigest:sha.nullable(),operator:z.string().min(1),approvalReference:z.string().min(1)
}).strict();
export type NotDispatchedEvidence=z.infer<typeof notDispatchedEvidenceSchema>;
const refuse=():never=>{throw new PersistenceError('RECOVERY_REQUIRED');};

/** The adapter validates private evidence and exact empty spool bytes under the instance lock.
 * Revalidation runs inside the transaction, including immediately before COMMIT.
 * No remote source, recovery, checkpoint update or delivery operation is available here. */
export async function closeNotDispatchedRun(pool:Pick<Pool,'connect'>,manifest:SpoolManifest|null,
 input:NotDispatchedEvidence,assertEvidence:()=>void,verifyOnly=false){
 const evidence=notDispatchedEvidenceSchema.parse(input);
 assertEvidence();
 if(manifest){
  const i=manifest.identity;
  if(!z.string().uuid().safeParse(i.runId).success || !z.string().uuid().safeParse(i.streamId).success ||
   !z.string().uuid().safeParse(i.batchId).success || !/^[1-9][0-9]{0,18}$/.test(i.fencingToken) ||
   i.format!=='olos-spool/v1' || i.plan.run.table!=='AttemptsRawData' || manifest.sealed || manifest.rows!==0 || manifest.bytes!==0 ||
   manifest.sha256!==null || !['receiving','recovery_required'].includes(manifest.state) || evidence.manifestDigest!==digest(manifest))refuse();
 }else if(evidence.manifestDigest!==null)refuse();
 return transaction(pool,async c=>{
  // Same order as startRun/recoverExpired/materialization. NOWAIT refuses competing writers.
  const lock=(await c.query("SELECT *,lease_until<=clock_timestamp() AS expired FROM olos_export.source_locks WHERE source_table='AttemptsRawData' FOR UPDATE NOWAIT")).rows[0];
  if(!lock)refuse();
  const available=(await c.query("SELECT to_regclass('olos_export.not_dispatched_closures') AS name")).rows[0].name;
  if(!manifest){
   if(available && (await c.query('SELECT 1 FROM olos_export.not_dispatched_closures WHERE capture_id=$1',[evidence.captureId])).rowCount)refuse();
   // Bootstrap normally has no run. An orphan cannot be guessed from a source, time or token.
   if(!verifyOnly && (lock.owner_run_id!==null || (await c.query("SELECT 1 FROM olos_export.runs WHERE source_table='AttemptsRawData' AND state IN ('read_intent','persisting','needs_reconciliation') LIMIT 1")).rowCount))refuse();
   assertEvidence();return null;
  }
  if(!available)throw new PersistenceError('SCHEMA_NOT_READY');
  const i=manifest.identity;
  const prior=(await c.query('SELECT * FROM olos_export.not_dispatched_closures WHERE capture_id=$1 OR run_id=$2 OR batch_id=$3',[evidence.captureId,i.runId,i.batchId])).rows;
  const confirmed=prior.length>0;
  if(verifyOnly && !confirmed)refuse();
  const released=String(BigInt(i.fencingToken)+1n);
  if(confirmed){
   const p=prior[0]!;
   if(prior.length!==1 || p.capture_id!==evidence.captureId || p.run_id!==i.runId || p.batch_id!==i.batchId || p.stream_id!==i.streamId ||
    p.previous_generation!==i.fencingToken || p.released_generation!==released || p.evidence_digest!==digest(evidence) ||
    canonical(p.evidence)!==canonical(evidence) || !p.closed_at || BigInt(lock.generation)<BigInt(released) || lock.owner_run_id===i.runId)refuse();
  }else if(lock.owner_run_id!==i.runId || lock.generation!==i.fencingToken || lock.expired!==true)refuse();
  const run=(await c.query(`SELECT r.*,s.read_type,s.export_date::text FROM olos_export.runs r JOIN olos_export.streams s ON s.id=r.stream_id
   WHERE r.id=$1 FOR UPDATE OF r NOWAIT`,[i.runId])).rows[0];
  if(!run || run.stream_id!==i.streamId || run.source_table!==i.plan.run.table || run.contract_version!==i.plan.run.contractVersion ||
   run.read_type!==i.plan.run.readType || run.export_date!==i.plan.run.exportDate || canonical(run.pk_range)!==canonical(i.plan.run.pkRange) ||
   run.fencing_token!==i.fencingToken || run.state!==(confirmed?'failed':'read_intent') ||
   (confirmed?!run.finished_at:run.finished_at!==null) || ['received_count','persisted_count','duplicate_count','rejected_count'].some(k=>run[k]!=='0'))refuse();
  // Run FOR UPDATE also excludes concurrent FK inserts. Every business/delivery effect has a
  // batch provenance root; explicit checks include orphan-like batch identities in local ledgers.
  if((await c.query(`SELECT
   EXISTS(SELECT 1 FROM olos_export.batches WHERE run_id=$1 OR id=$2) OR
   EXISTS(SELECT 1 FROM olos_export.sync_errors WHERE run_id=$1) OR
   EXISTS(SELECT 1 FROM olos_export.recovery_cases WHERE run_id=$1) OR
   EXISTS(SELECT 1 FROM olos_export.audit_events WHERE run_id=$1 OR batch_id=$2) OR
   EXISTS(SELECT 1 FROM olos_export.large_batch_receipts WHERE batch_id=$2) OR
   EXISTS(SELECT 1 FROM olos_export.large_batch_keys WHERE batch_id=$2) OR
   EXISTS(SELECT 1 FROM olos_export.attempts_raw_data WHERE last_batch_id=$2) OR
   EXISTS(SELECT 1 FROM olos_export.projection_batches WHERE batch_id=$2) OR
   EXISTS(SELECT 1 FROM olos_export.projection_outbox WHERE batch_id=$2) OR
   EXISTS(SELECT 1 FROM olos_export.local_ingestion_reconciliations WHERE original_run_id=$1 OR recovery_run_id=$1 OR original_batch_id=$2 OR capture_id=$3) OR
   EXISTS(SELECT 1 FROM olos_export.administrative_capture_discards WHERE original_run_id=$1 OR batch_id=$2 OR capture_id=$3)
   AS present`,[i.runId,i.batchId,evidence.captureId])).rows[0].present)refuse();
  if(!confirmed){
   if((await c.query(`SELECT 1 FROM olos_export.runs WHERE source_table='AttemptsRawData' AND id<>$1 AND state IN ('read_intent','persisting','needs_reconciliation')
    UNION ALL SELECT 1 FROM olos_export.recovery_cases c JOIN olos_export.runs r ON r.id=c.run_id WHERE r.source_table='AttemptsRawData' AND c.state='open' LIMIT 1`,[i.runId])).rowCount)refuse();
   if(!(await c.query('SELECT 1 FROM olos_export.checkpoints WHERE stream_id=$1 FOR UPDATE NOWAIT',[i.streamId])).rowCount)refuse();
   assertEvidence();
   await c.query(`INSERT INTO olos_export.not_dispatched_closures(capture_id,run_id,batch_id,stream_id,previous_generation,released_generation,evidence,evidence_digest)
    VALUES($1,$2,$3,$4,$5,$6,$7::jsonb,$8)`,[evidence.captureId,i.runId,i.batchId,i.streamId,i.fencingToken,released,JSON.stringify(evidence),digest(evidence)]);
   await c.query("UPDATE olos_export.runs SET state='failed',finished_at=clock_timestamp() WHERE id=$1",[i.runId]);
   const updated=await c.query(`UPDATE olos_export.source_locks SET generation=$3,owner_run_id=NULL,lease_until=NULL
    WHERE source_table='AttemptsRawData' AND owner_run_id=$1 AND generation=$2 AND lease_until<=clock_timestamp()`,[i.runId,i.fencingToken,released]);
   if(updated.rowCount!==1)refuse();
  }
  assertEvidence();
  return {runId:i.runId,runState:'failed' as const,alreadyReconciled:confirmed,releasedGeneration:released};
 });
}
