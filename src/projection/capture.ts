import { randomUUID } from 'node:crypto';
import type { PoolClient } from 'pg';
import { PersistenceError, type SourceContract } from '../persistence/types.js';
import { digest, type PreparedRecord } from '../persistence/validation.js';
import { destinationVersion } from './contract.js';

export interface CaptureConfig {
  readonly sourceNamespace: string;
  readonly destinationId: string;
  readonly mappingVersion: string;
}
export function captureConfig(input: CaptureConfig): Readonly<CaptureConfig> {
  const config = { sourceNamespace: input.sourceNamespace,destinationId: input.destinationId,mappingVersion: input.mappingVersion };
  for (const value of Object.values(config)) if (typeof value !== 'string' || !/^[A-Za-z0-9._:/-]{1,128}$/.test(value)) throw new PersistenceError('VALIDATION_FAILED');
  return Object.freeze(config);
}
function configDigest(c: CaptureConfig): string { return digest({ ...c, destinationVersion }); }
export async function verifyCapture(client: PoolClient, batchId: string, config: CaptureConfig, rows: readonly PreparedRecord[]): Promise<void> {
  const result = await client.query<{ config_digest: string; snapshot_digest: string; row_count: number }>(
    'SELECT config_digest,snapshot_digest,row_count FROM olos_export.projection_batches WHERE batch_id=$1 AND destination_id=$2', [batchId,config.destinationId]);
  const prior = result.rows[0];
  if (!prior || prior.config_digest !== configDigest(config) || prior.snapshot_digest !== digest(rows) || prior.row_count !== rows.length) throw new PersistenceError('PROJECTION_CAPTURE_MISSING');
}
// Somente SQL no cliente da transação de ingestão. Nenhuma chamada de destino neste ponto.
export async function captureBatch(client: PoolClient, batchId: string, c: CaptureConfig, contract: SourceContract, rows: readonly PreparedRecord[]): Promise<void> {
  const ledger = await client.query<{ version: number }>('SELECT version FROM olos_export.projection_schema_versions ORDER BY version');
  if (ledger.rows.length !== 1 || ledger.rows[0]?.version !== 1) throw new PersistenceError('SCHEMA_NOT_READY');
  await client.query(`INSERT INTO olos_export.projection_batches(batch_id,destination_id,source_table,source_namespace,source_contract_version,
    field_definitions,destination_version,mapping_version,config_digest,snapshot_digest,row_count)
    VALUES($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9,$10,$11)`,
    [batchId,c.destinationId,contract.source_table,c.sourceNamespace,contract.version,JSON.stringify(contract.fields),destinationVersion,c.mappingVersion,configDigest(c),digest(rows),rows.length]);
  // Snapshot próprio por observação/chave, inclusive quando o envelope atual não sofreu alteração.
  // UUID gerado uma vez e preservado no retry após commit; rollback não expõe identidade externa.
  for (let offset = 0; offset < rows.length; offset += 200) {
    const chunk = rows.slice(offset,offset+200).map(row => ({ id: randomUUID(), key: row.key, payload: row.payload }));
    await client.query(`INSERT INTO olos_export.projection_outbox(delivery_id,batch_id,destination_id,source_key,payload)
      SELECT x.id,$2,$3,x.key,x.payload FROM jsonb_to_recordset($1::jsonb) AS x(id uuid,key text,payload jsonb)`,
      [JSON.stringify(chunk),batchId,c.destinationId]);
  }
}
