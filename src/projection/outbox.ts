import { randomUUID } from 'node:crypto';
import type { Pool, PoolClient } from 'pg';
import { transaction } from '../persistence/transaction.js';
import { PersistenceError } from '../persistence/types.js';
import { assertUuid, assertVersion } from '../persistence/validation.js';
import { requestDigest as computeRequestDigest } from './request.js';
import { exactFlat, type Scalar } from './transform.js';
import type { Candidate } from './contract.js';

export interface DeliveryMessage {
  readonly delivery_id: string; readonly batch_id: string; readonly destination_id: string;
  readonly source_key: string; readonly source_table: string; readonly source_namespace: string;
  readonly source_contract_version: string; readonly field_definitions: unknown;
  readonly destination_version: string; readonly mapping_version: string;
  readonly payload: Readonly<Record<string, Scalar>>;
  readonly request_payload: Readonly<Candidate> | null; readonly request_digest: string | null;
}
export interface DeliveryClaim { readonly id: string; readonly owner: string; readonly token: string; readonly message: DeliveryMessage; }
export type Receipt = 'confirmed' | 'rejected' | 'absent' | 'unknown';
type State = 'pending' | 'confirmed' | 'rejected' | 'uncertain';
export class ProjectionOutbox {
  constructor(private readonly pool: Pick<Pool,'connect'>, readonly destinationId: string,
    private readonly leaseMs = 60_000, private readonly retryMs = 60_000) {
    if (!/^[A-Za-z0-9._:/-]{1,128}$/.test(destinationId) || !Number.isSafeInteger(leaseMs) || leaseMs<1 || leaseMs>300_000 ||
      !Number.isSafeInteger(retryMs) || retryMs<1 || retryMs>3_600_000) throw new PersistenceError('VALIDATION_FAILED');
  }
  private async event(client: PoolClient, id: string, generation: string, attemptId: string | null, event: string): Promise<void> {
    await client.query('INSERT INTO olos_export.projection_events(id,delivery_id,generation,attempt_id,event) VALUES($1,$2,$3,$4,$5)', [randomUUID(),id,generation,attemptId,event]);
  }
  private identity(claim: DeliveryClaim): void { assertUuid(claim.id); assertUuid(claim.owner); assertVersion(claim.token); }
  async claim(owner: string): Promise<DeliveryClaim | null> {
    assertUuid(owner);
    return transaction(this.pool, async client => {
      const ledger = await client.query<{ version: number }>('SELECT version FROM olos_export.projection_schema_versions ORDER BY version');
      if (ledger.rows.length!==1 || ledger.rows[0]?.version!==1) throw new PersistenceError('SCHEMA_NOT_READY');
      const picked = await client.query<{ delivery_id: string; generation: string }>(`WITH candidate AS (
        SELECT delivery_id FROM olos_export.projection_outbox WHERE destination_id=$1 AND state='pending'
          AND available_at<=clock_timestamp() AND (lease_until IS NULL OR lease_until<=clock_timestamp())
        ORDER BY created_at,delivery_id FOR UPDATE SKIP LOCKED LIMIT 1)
        UPDATE olos_export.projection_outbox o SET generation=generation+1,lease_owner=$2,
          lease_until=clock_timestamp()+($3::double precision*interval '1 millisecond'),attempts=attempts+1
        FROM candidate c WHERE o.delivery_id=c.delivery_id RETURNING o.delivery_id,o.generation`, [this.destinationId,owner,this.leaseMs]);
      const row = picked.rows[0];
      if (!row) return null;
      const message = (await client.query<DeliveryMessage>(`SELECT o.delivery_id,o.batch_id,o.destination_id,o.source_key,o.payload,o.request_payload,o.request_digest,
        b.source_table,b.source_namespace,b.source_contract_version,b.field_definitions,b.destination_version,b.mapping_version
        FROM olos_export.projection_outbox o JOIN olos_export.projection_batches b USING(batch_id,destination_id) WHERE o.delivery_id=$1`, [row.delivery_id])).rows[0]!;
      await this.event(client,row.delivery_id,row.generation,null,'claimed');
      return { id: row.delivery_id,owner,token: row.generation,message };
    });
  }
  async defer(claim: DeliveryClaim, rejected = false, destinationBlocked = false): Promise<void> {
    this.identity(claim);
    await transaction(this.pool, async client => {
      const state = rejected ? 'rejected' : 'pending';
      const result = await client.query(`UPDATE olos_export.projection_outbox SET state=$4,lease_owner=NULL,lease_until=NULL,
        available_at=clock_timestamp()+($5::double precision*interval '1 millisecond'),last_code=$6
        WHERE delivery_id=$1 AND lease_owner=$2 AND generation=$3 AND state='pending' AND lease_until>clock_timestamp()`,
        [claim.id,claim.owner,claim.token,state,this.retryMs,rejected ? 'PROJECTION_REJECTED' : destinationBlocked ? 'DESTINATION_BLOCKED' : 'PROJECTION_PENDING']);
      if (result.rowCount!==1) throw new PersistenceError('LEASE_LOST');
      await this.event(client,claim.id,claim.token,null,state);
    });
  }
  async authorize(claim: DeliveryClaim, row: Readonly<Candidate>): Promise<{ attemptId: string; requestDigest: string; row: Readonly<Candidate> }> {
    this.identity(claim);
    // Snapshot da requisição não pode mudar depois do primeiro envio autorizado.
    const snapshot = exactFlat(row), requestDigest = computeRequestDigest(snapshot,claim.message.destination_id,claim.message.destination_version,claim.message.mapping_version), attemptId = randomUUID();
    return transaction(this.pool, async client => {
      const result = await client.query(`UPDATE olos_export.projection_outbox SET state='uncertain',attempt_id=$4,
        request_payload=$5::jsonb,request_digest=$6,last_code='TARGET_UNKNOWN'
        WHERE delivery_id=$1 AND lease_owner=$2 AND generation=$3 AND state='pending' AND lease_until>clock_timestamp()
          AND (request_digest IS NULL OR request_digest=$6)`,
        [claim.id,claim.owner,claim.token,attemptId,JSON.stringify(snapshot),requestDigest]);
      if (result.rowCount!==1) throw new PersistenceError('LEASE_LOST');
      await this.event(client,claim.id,claim.token,attemptId,'dispatch_authorized');
      return { attemptId,requestDigest,row: snapshot };
    });
  }
  async settle(claim: DeliveryClaim, attemptId: string, state: State): Promise<void> {
    this.identity(claim); assertUuid(attemptId);
    if (!['pending','confirmed','rejected','uncertain'].includes(state)) throw new PersistenceError('VALIDATION_FAILED');
    await transaction(this.pool, async client => {
      const result = await client.query(`UPDATE olos_export.projection_outbox SET state=$5,lease_owner=NULL,lease_until=NULL,
        confirmed_at=CASE WHEN $5='confirmed' THEN clock_timestamp() ELSE confirmed_at END,
        available_at=clock_timestamp()+($6::double precision*interval '1 millisecond'),last_code=$7
        WHERE delivery_id=$1 AND lease_owner=$2 AND generation=$3 AND attempt_id=$4 AND state='uncertain' AND lease_until>clock_timestamp()`,
        [claim.id,claim.owner,claim.token,attemptId,state,this.retryMs,
          state==='confirmed' ? 'CONFIRMED' : state==='pending' ? 'TARGET_NOT_COMMITTED' : state==='rejected' ? 'PROJECTION_REJECTED' : 'TARGET_UNKNOWN']);
      if (result.rowCount!==1) throw new PersistenceError('LEASE_LOST');
      await this.event(client,claim.id,claim.token,attemptId,state);
    });
  }
  async uncertain(id: string): Promise<{ id: string; requestDigest: string } | null> {
    assertUuid(id);
    return transaction(this.pool, async client => {
      const row = (await client.query<{ request_digest: string }>(`SELECT request_digest FROM olos_export.projection_outbox
        WHERE delivery_id=$1 AND destination_id=$2 AND state='uncertain' AND (lease_until IS NULL OR lease_until<=clock_timestamp())`, [id,this.destinationId])).rows[0];
      return row ? { id,requestDigest: row.request_digest } : null;
    });
  }
  async reconcile(id: string, requestDigest: string, receipt: Receipt): Promise<boolean> {
    assertUuid(id);
    if (!/^[0-9a-f]{64}$/.test(requestDigest) || !['confirmed','rejected','absent','unknown'].includes(receipt)) throw new PersistenceError('VALIDATION_FAILED');
    return transaction(this.pool, async client => {
      const row = (await client.query<{ generation: string; attempt_id: string }>(`SELECT generation,attempt_id FROM olos_export.projection_outbox
        WHERE delivery_id=$1 AND destination_id=$2 AND request_digest=$3 AND state='uncertain'
          AND (lease_until IS NULL OR lease_until<=clock_timestamp()) FOR UPDATE`, [id,this.destinationId,requestDigest])).rows[0];
      if (!row) return false;
      const state = receipt==='confirmed' || receipt==='rejected' ? receipt : 'uncertain';
      await client.query(`UPDATE olos_export.projection_outbox SET state=$2,lease_owner=NULL,lease_until=NULL,
        last_code=$3,confirmed_at=CASE WHEN $2='confirmed' THEN clock_timestamp() ELSE confirmed_at END WHERE delivery_id=$1`,
        [id,state,receipt==='confirmed' ? 'CONFIRMED' : receipt==='rejected' ? 'PROJECTION_REJECTED' : receipt==='absent' ? 'RECEIPT_ABSENT' : 'TARGET_UNKNOWN']);
      await this.event(client,id,row.generation,row.attempt_id,receipt==='absent' ? 'receipt_absent' : state);
      return state!=='uncertain';
    });
  }
}
