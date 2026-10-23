import type { Pool, PoolClient } from 'pg';
import { transaction } from '../persistence/transaction.js';
import { PersistenceError } from '../persistence/types.js';
import { assertUuid } from '../persistence/validation.js';
import {projectionClaimBatchSize} from './outbox.js';
import { exactFlat, ProjectionError } from './transform.js';
import { TargetDeliveryError, type DeliveryRequest, type DeliveryTarget } from './dispatcher.js';
import type { Candidate } from './contract.js';
import type { Receipt } from './outbox.js';

export interface TransactionalBusinessWriter {
  preflight(row: Readonly<Candidate>): 'allowed' | 'blocked' | 'rejected';
  // Somente SQL no cliente fornecido; nenhuma rede externa ou commit próprio.
  writeInTransaction(client: PoolClient, request: DeliveryRequest): Promise<void>;
  writeBatchInTransaction?: (client: PoolClient, requests: readonly DeliveryRequest[]) => Promise<void>;
}
export { requestDigest } from './request.js';
import { requestDigest } from './request.js';
export class PostgresDeliveryTarget implements DeliveryTarget {
  readonly deliverBatch?: (requests: readonly DeliveryRequest[]) => Promise<readonly ('confirmed'|'rejected')[]>;
  constructor(private readonly pool: Pick<Pool,'connect'>, private readonly destinationId: string, private readonly writer: TransactionalBusinessWriter) {
    if(writer.writeBatchInTransaction)this.deliverBatch=requests=>this.deliverGroup(requests);
  }
  preflight(row: Readonly<Candidate>) { return this.writer.preflight(row); }
  async deliver(input: DeliveryRequest): Promise<'confirmed' | 'rejected'> {
    const request = Object.freeze({ ...input,row: exactFlat(input.row) });
    assertUuid(request.deliveryId);
    if (request.destinationId!==this.destinationId || request.requestDigest!==requestDigest(request.row,request.destinationId,request.contractVersion,request.mappingVersion)) throw new TargetDeliveryError('not_committed');
    if (this.writer.preflight(request.row)!=='allowed') throw new TargetDeliveryError('not_committed');
    try {
      return await transaction(this.pool, async client => {
        const ledger = await client.query<{ version: number }>('SELECT version FROM olos_export.delivery_schema_versions ORDER BY version');
        if (ledger.rows.length!==1 || ledger.rows[0]?.version!==1) throw new PersistenceError('SCHEMA_NOT_READY');
        const inserted = await client.query(`INSERT INTO olos_export.delivery_receipts(destination_id,delivery_id,request_digest,destination_version,mapping_version,state)
          VALUES($1,$2,$3,$4,$5,'confirmed') ON CONFLICT(destination_id,delivery_id) DO NOTHING RETURNING delivery_id`,
          [this.destinationId,request.deliveryId,request.requestDigest,request.contractVersion,request.mappingVersion]);
        if (!inserted.rowCount) {
          const receipt = (await client.query<{ request_digest: string; state: 'confirmed' | 'rejected'; destination_version: string; mapping_version: string }>(
            'SELECT request_digest,state,destination_version,mapping_version FROM olos_export.delivery_receipts WHERE destination_id=$1 AND delivery_id=$2', [this.destinationId,request.deliveryId])).rows[0]!;
          if (receipt.request_digest!==request.requestDigest || receipt.destination_version!==request.contractVersion || receipt.mapping_version!==request.mappingVersion) throw new ProjectionError('RECEIPT_CONFLICT');
          return receipt.state;
        }
        // A linha de recibo só fica visível no mesmo COMMIT da escrita de negócio.
        await this.writer.writeInTransaction(client,request);
        return 'confirmed';
      });
    } catch (error) {
      throw new TargetDeliveryError(error instanceof PersistenceError && error.outcome==='not_committed' ? 'not_committed' : 'unknown');
    }
  }
  async receipt(deliveryId: string, expectedDigest: string): Promise<Receipt> {
    assertUuid(deliveryId);
    try {
      return await transaction(this.pool, async client => {
        const row = (await client.query<{ request_digest: string; state: 'confirmed' | 'rejected' }>(
          'SELECT request_digest,state FROM olos_export.delivery_receipts WHERE destination_id=$1 AND delivery_id=$2', [this.destinationId,deliveryId])).rows[0];
        return !row ? 'absent' : row.request_digest===expectedDigest ? row.state : 'unknown';
      });
    } catch { return 'unknown'; }
  }
  private async deliverGroup(input:readonly DeliveryRequest[]):Promise<readonly ('confirmed'|'rejected')[]>{
    if(!input.length || input.length>projectionClaimBatchSize || new Set(input.map(r=>r.deliveryId)).size!==input.length)throw new TargetDeliveryError('not_committed');
    const requests=input.map(r=>Object.freeze({...r,row:exactFlat(r.row)}));
    for(const r of requests){
      assertUuid(r.deliveryId);
      if(r.destinationId!==this.destinationId || r.requestDigest!==requestDigest(r.row,r.destinationId,r.contractVersion,r.mappingVersion) || this.writer.preflight(r.row)!=='allowed')throw new TargetDeliveryError('not_committed');
    }
    try{return await transaction(this.pool,async client=>{
      const ledger=await client.query<{version:number}>('SELECT version FROM olos_export.delivery_schema_versions ORDER BY version');
      if(ledger.rows.length!==1 || ledger.rows[0]?.version!==1)throw new PersistenceError('SCHEMA_NOT_READY');
      const inserted=await client.query<{delivery_id:string}>(`INSERT INTO olos_export.delivery_receipts(destination_id,delivery_id,request_digest,destination_version,mapping_version,state)
        SELECT $1,x.id,x.digest,x.version,x.mapping,'confirmed' FROM jsonb_to_recordset($2::jsonb) AS x(id uuid,digest text,version text,mapping text)
        ORDER BY x.id ON CONFLICT(destination_id,delivery_id) DO NOTHING RETURNING delivery_id`,
        [this.destinationId,JSON.stringify(requests.map(r=>({id:r.deliveryId,digest:r.requestDigest,version:r.contractVersion,mapping:r.mappingVersion})))]);
      const receipts=(await client.query<{delivery_id:string;request_digest:string;destination_version:string;mapping_version:string;state:'confirmed'|'rejected'}>(
        'SELECT delivery_id,request_digest,destination_version,mapping_version,state FROM olos_export.delivery_receipts WHERE destination_id=$1 AND delivery_id=ANY($2::uuid[])',
        [this.destinationId,requests.map(r=>r.deliveryId)])).rows;
      const byId=new Map(receipts.map(r=>[r.delivery_id,r]));
      const states=requests.map(r=>{
        const receipt=byId.get(r.deliveryId);
        if(!receipt || receipt.request_digest!==r.requestDigest || receipt.destination_version!==r.contractVersion || receipt.mapping_version!==r.mappingVersion)throw new ProjectionError('RECEIPT_CONFLICT');
        return receipt.state;
      });
      const fresh=new Set(inserted.rows.map(r=>r.delivery_id));
      if(fresh.size)await this.writer.writeBatchInTransaction!(client,requests.filter(r=>fresh.has(r.deliveryId)));
      return states;
    });}catch(error){throw new TargetDeliveryError(error instanceof PersistenceError && error.outcome==='not_committed'?'not_committed':'unknown');}
  }
}
