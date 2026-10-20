import type { Pool } from 'pg';
import { ProjectionOutbox } from '../projection/outbox.js';
import { ProjectionDispatcher } from '../projection/dispatcher.js';
import { PostgresDeliveryTarget } from '../projection/postgres-target.js';
import {
  fields as knownFields,
  destinationVersion,
} from '../projection/contract.js';
import type { FieldResolution } from '../projection/transform.js';
import type { SpoolRecord } from '../large-batch/types.js';
import type { Catalogs, OfflineProfile } from './model.js';
import { projectTdsAttempts } from './tds-projection.js';
import { IsolatedTdsCallsWriter } from './tds-writer.js';

/**
 * Adaptador de homologação para AttemptsRawData.
 *
 * Não cria tabelas, não aprova contratos corporativos
 * e não executa leituras no SQL Server.
 *
 * O chamador deve fornecer pools de um ambiente
 * PostgreSQL isolado e previamente autorizado.
 */
export function isolatedTdsDelivery(
  sourcePool: Pool,
  destinationPool: Pool,
  destinationId: string,
  profile: OfflineProfile,
  catalogs: Catalogs,
  loadedAt: string,
  preserveExisting=false,
) {
  const outbox = new ProjectionOutbox(
    sourcePool,
    destinationId,
    60_000,
    1_000,
  );

  const target = new PostgresDeliveryTarget(
    destinationPool,
    destinationId,
    new IsolatedTdsCallsWriter(profile,preserveExisting),
  );

  const dispatcher = new ProjectionDispatcher(
    outbox,
    target,
    message => {
      if (
        message.source_table !== 'AttemptsRawData' ||
        message.destination_id !== destinationId
      ) {
        throw new Error('UNEXPECTED_TDS_SOURCE');
      }

      const result = projectTdsAttempts(
        message.payload as SpoolRecord,
        catalogs,
        profile,
        loadedAt,
      );

      const resolutions = Object.fromEntries(
        knownFields.map(field => {
          const present = Object.hasOwn(result.row, field);
          const value = present ? result.row[field] : undefined;

          return [
            field,
            {
              state: !present
                ? 'absent'
                : value === null
                  ? 'null'
                  : 'value',
              ...(present
                ? { value: value!, origin: 'local_transform' }
                : {}),
            },
          ];
        }),
      ) as Record<
        typeof knownFields[number],
        FieldResolution
      >;

      return {
        status: 'ready' as const,
        contractVersion: destinationVersion,
        mappingVersion: message.mapping_version,
        original: message.payload,
        fields: resolutions,
        issues: [],
        row: result.row,
      };
    },
  );

  return { outbox, target, dispatcher };
}
