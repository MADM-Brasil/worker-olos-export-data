import { createHash, randomUUID } from 'node:crypto';
import type { PoolClient } from 'pg';
import type { SourceContract } from '../persistence/types.js';
import { canonical, digest, type PreparedRecord } from '../persistence/validation.js';
import { captureConfig, type CaptureConfig } from './capture.js';
import { destinationVersion } from './contract.js';

export class ChunkedProjectionCapture {
  private readonly config: Readonly<CaptureConfig>;
  private readonly hash = createHash('sha256');
  private count = 0;
  private started = false;
  private finished = false;

  constructor(
    private readonly client: PoolClient,
    private readonly batchId: string,
    config: CaptureConfig,
    private readonly contract: SourceContract,
  ) {
    this.config = captureConfig(config);
  }

  async start(): Promise<void> {
    if (this.started) throw new Error('CAPTURE_ALREADY_STARTED');

    const versions = await this.client.query(
      'SELECT version FROM olos_export.projection_schema_versions'
    );

    if (versions.rows.length !== 1 || versions.rows[0].version !== 1) {
      throw new Error('PROJECTION_SCHEMA_NOT_READY');
    }

    await this.client.query(`
      INSERT INTO olos_export.projection_batches(
        batch_id, destination_id, source_table, source_namespace,
        source_contract_version, field_definitions, destination_version,
        mapping_version, config_digest, snapshot_digest, row_count
      )
      VALUES($1,$2,$3,$4,$5,$6::jsonb,$7,$8,$9,$10,0)
    `, [
      this.batchId,
      this.config.destinationId,
      this.contract.source_table,
      this.config.sourceNamespace,
      this.contract.version,
      JSON.stringify(this.contract.fields),
      destinationVersion,
      this.config.mappingVersion,
      digest({ ...this.config, destinationVersion }),
      '0'.repeat(64),
    ]);

    this.started = true;
  }

  async append(rows: readonly PreparedRecord[]): Promise<void> {
    if (!this.started || this.finished) {
      throw new Error('CAPTURE_INVALID_STATE');
    }

    for (let offset = 0; offset < rows.length; offset += 200) {
      const chunk = rows.slice(offset, offset + 200);

      if (!chunk.length) continue;

      await this.client.query(`
        INSERT INTO olos_export.projection_outbox(
          delivery_id,batch_id,destination_id,source_key,payload
        )
        SELECT x.id,$2,$3,x.key,x.payload
        FROM jsonb_to_recordset($1::jsonb)
          AS x(id uuid,key text,payload jsonb)
      `, [
        JSON.stringify(chunk.map(row => ({
          id: randomUUID(),
          key: row.key,
          payload: row.payload,
        }))),
        this.batchId,
        this.config.destinationId,
      ]);

      for (const row of chunk) {
        const value = canonical(row);
        this.hash.update(Buffer.byteLength(value).toString() + ':');
        this.hash.update(value);
        this.count++;
      }
    }
  }

  async finish(): Promise<void> {
    if (!this.started || this.finished) {
      throw new Error('CAPTURE_INVALID_STATE');
    }

    const checksum = this.hash.digest('hex');

    const result = await this.client.query(`
      UPDATE olos_export.projection_batches
      SET snapshot_digest=$3,row_count=$4
      WHERE batch_id=$1 AND destination_id=$2
    `, [
      this.batchId,
      this.config.destinationId,
      checksum,
      this.count,
    ]);

    if (result.rowCount !== 1) {
      throw new Error('CAPTURE_FINALIZATION_FAILED');
    }

    const count = await this.client.query(`
      SELECT count(*)::integer AS total
      FROM olos_export.projection_outbox
      WHERE batch_id=$1 AND destination_id=$2
    `, [this.batchId, this.config.destinationId]);

    if (count.rows[0]?.total !== this.count) {
      throw new Error('CAPTURE_COUNT_MISMATCH');
    }

    this.finished = true;
  }
}
