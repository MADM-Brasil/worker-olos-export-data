import { describe, expect, it } from 'vitest';
import { profileSchema } from '../src/offline/model.js';
import { IsolatedTdsCallsWriter } from '../src/offline/tds-writer.js';

const approvedColumns = [
  'internal_id_olos',
  'olos_call_id',
  'data_ligacao',
  'telefone',
  'telefone_match_key',
  'nome_agente',
  'id_campanha_olos',
  'nome_campanha_olos',
  'id_tabulacao_olos',
  'nome_tabulacao_olos',
  'cpf',
  'email',
  'lead_id_origem',
  'tma_origem',
  'razao_desligamento',
  'raw_olos_call_id',
];

describe('entrega TDS isolada', () => {
  it('aceita somente o perfil aprovado de 16 colunas', async () => {
    const profile = profileSchema.parse({
      version: 'offline-homologation/v1',
      approval: 'APPROVE_ISOLATED_OFFLINE_MAPPING_V1',
      identityEvidence: 'LOCAL SYNTHETIC FIXTURE: AttemptsRawDataID only',
      columns: approvedColumns,
      xmlCpfNames: ['CPF'],
      xmlEmailNames: ['Email'],
      phonePolicy: 'digits-only',
      phoneMatchKey: 'normalized',
      timestampPolicy: 'utc-civil',
      tmaPolicy: 'disabled',
    });

    expect(profile.columns).toEqual(approvedColumns);

    const writer = new IsolatedTdsCallsWriter(profile);

    const row: Record<string, string | null> = Object.fromEntries(
      approvedColumns.map(column => [column, null]),
    );

    row.internal_id_olos = '9007199254740993';

    expect(writer.preflight(row)).toBe('allowed');
    for(const field of ['lead_id_origem','tma_origem','razao_desligamento'])expect(writer.preflight({...row,[field]:'1'})).toBe('rejected');

    expect(
      writer.preflight({
        ...row,
        raw_olos_call_id: '9007199254740993',
      }),
    ).toBe('rejected');
  });
});

it.each([false,true])('entrega três chamadas TDS sintéticas com recibos transacionais; checkpoint adiado=%s', async deferCheckpoint => {
  const { isolatedPostgres } = await import('./support/postgres.js');
  const { readFile } = await import('node:fs/promises');
  const pg = (await import('pg')).default;
  const { columnTypes } = await import('../src/offline/model.js');
  const { destinationVersion } = await import('../src/projection/contract.js');
  const { isolatedTdsDelivery } = await import('../src/offline/tds-delivery.js');

  let deliveryManifest:import('../src/large-batch/types.js').SpoolManifest|undefined;
  const source = await isolatedPostgres();
  let destination: Awaited<ReturnType<typeof isolatedPostgres>> | undefined;
  let projector: InstanceType<typeof pg.Pool> | undefined;
  let receiver: InstanceType<typeof pg.Pool> | undefined;

  try {
    destination = await isolatedPostgres();

    await source.admin.query(await readFile(
      new URL('../migrations/003_projection_control.sql', import.meta.url), 'utf8',
    ));
    await source.admin.query(await readFile(
      new URL('../permissions/projection.sql', import.meta.url), 'utf8',
    ));

    await source.admin.query(await readFile(
      new URL('../migrations/005_large_batch_control.sql', import.meta.url), 'utf8',
    ));
    await source.admin.query(await readFile(
      new URL('../permissions/large-batch.sql', import.meta.url), 'utf8',
    ));

    await destination.admin.query(await readFile(
      new URL('../migrations/004_delivery_receipts.sql', import.meta.url), 'utf8',
    ));
    await destination.admin.query(await readFile(
      new URL('../permissions/receiver.sql', import.meta.url), 'utf8',
    ));

    const profile = profileSchema.parse({
      version: 'offline-homologation/v1',
      approval: 'APPROVE_ISOLATED_OFFLINE_MAPPING_V1',
      identityEvidence: 'LOCAL SYNTHETIC FIXTURE: AttemptsRawDataID only',
      columns: approvedColumns,
      xmlCpfNames: ['CPF'],
      xmlEmailNames: ['Email'],
      phonePolicy: 'digits-only',
      phoneMatchKey: 'normalized',
      timestampPolicy: 'utc-civil',
      tmaPolicy: 'disabled',
    });

    await destination.admin.query('CREATE SCHEMA raw');

    await destination.admin.query(
      `CREATE TABLE raw.olos_calls(
        ${profile.columns.map(c =>
          `"${c}" ${columnTypes[c]}${c === 'internal_id_olos' ? ' PRIMARY KEY' : ''}`
        ).join(',')}
      )`,
    );

    await destination.admin.query(
      'GRANT USAGE ON SCHEMA raw TO olos_export_receiver',
    );
    await destination.admin.query(
      'GRANT SELECT,INSERT,UPDATE ON raw.olos_calls TO olos_export_receiver',
    );

    projector = new pg.Pool({
      ...source.connection,
      options: '-c role=olos_export_projector',
    });

    receiver = new pg.Pool({
      ...destination.connection,
      options: '-c role=olos_export_receiver',
    });

    const destinationId = 'raw.olos_calls.fixture';
    const catalogs = {
      agents: new Map([['1592', 'Operador Sintetico']]),
      campaigns: new Map([['10', 'Campanha Teste']]),
      dispositions: new Map([['20', 'Tabulacao Teste']]),
    };

    const { dispatcher } = isolatedTdsDelivery(
      projector,
      receiver,
      destinationId,
      profile,
      catalogs,
      '2026-10-19T00:00:00',
    );

    const { mkdtemp, rm } = await import('node:fs/promises');
    const { join } = await import('node:path');
    const { tmpdir } = await import('node:os');
    const { LocalSpool } = await import('../adapters/local-spool.js');
    const { spoolLimits } = await import('../src/large-batch/config.js');
    const { LargeBatchCoordinator } = await import('../src/large-batch/coordinator.js');
    const { PostgresPersistence } = await import('../src/persistence/store.js');
    const { PostgresLargeBatchPersistence } = await import('../src/persistence/large-batch.js');
    const { largePlan, rawSource } = await import('./support/large-batch-fixture.js');

    const limits = spoolLimits({
      minFreeBytes: 0,
      chunkRows: 2,
      bufferBytes: 1024,
    });

    const plan = largePlan();

    await source.admin.query(
      `INSERT INTO olos_export.source_contracts(
        source_table,version,approved,fields,key_fields,
        layout_evidence,key_evidence
      ) VALUES($1,$2,true,$3::jsonb,$4,$5,$6)`,
      [
        plan.contract.source_table,
        plan.contract.version,
        JSON.stringify(plan.contract.fields),
        plan.contract.key_fields,
        plan.contract.layout_evidence,
        plan.contract.key_evidence,
      ],
    );

    const worker = source.worker();
    const root = await mkdtemp(join(tmpdir(), 'olos-tds-e2e-'));

    try {
      const spool = await LocalSpool.open(root, limits);
      const persistence = new PostgresPersistence(worker);

      const captured = new PostgresLargeBatchPersistence(
        worker,
        limits,
        300000,
        {
          sourceNamespace: 'synthetic-source',
          destinationId,
          mappingVersion: 'fixture-v1',
        },deferCheckpoint,
      );

      const coordinator = new LargeBatchCoordinator(
        persistence,
        spool,
        captured,
        limits,
      );

      const received = await coordinator.receive(
        plan,
        rawSource(3),
        new AbortController().signal,
      );

      expect(received.received).toBe(3);
      expect(received.persisted).toBe(3);
      expect(await spool.active()).toBeNull();

      const counts = await source.admin.query(`
        SELECT
          (SELECT count(*)::int
           FROM olos_export.attempts_raw_data) AS raw_rows,
          (SELECT count(*)::int
           FROM olos_export.projection_outbox) AS deliveries,
          (SELECT version::text
           FROM olos_export.checkpoints LIMIT 1) AS checkpoint
      `);

      expect(counts.rows[0]).toMatchObject({
        raw_rows: 3,
        deliveries: 3,
        checkpoint: deferCheckpoint?'0':'1',
      });
      if(deferCheckpoint){
        const id=(await source.admin.query('SELECT batch_id FROM olos_export.large_batch_receipts')).rows[0].batch_id;
        deliveryManifest=JSON.parse(await readFile(join(root,'batches',id,'manifest.json'),'utf8'));
        await expect(captured.confirmDeliveryCheckpoint(deliveryManifest!)).rejects.toThrow('RECOVERY_REQUIRED');
      }
    } finally {
      await worker.end();
      await rm(root, { recursive: true, force: true });
    }

    for (let i = 0; i < 3; i++) {
      expect(await dispatcher.runOnce()).toBe('confirmed');
    }

    expect(await dispatcher.runOnce()).toBe('empty');

    const calls = await destination.admin.query(
      'SELECT internal_id_olos, raw_olos_call_id FROM raw.olos_calls ORDER BY internal_id_olos',
    );

    expect(calls.rows).toHaveLength(3);
    expect(calls.rows.every(r => r.raw_olos_call_id === null)).toBe(true);

    const receipts = await destination.admin.query(
      'SELECT count(*)::int AS total FROM olos_export.delivery_receipts',
    );

    expect(receipts.rows[0].total).toBe(3);

    const confirmed = await source.admin.query(
      "SELECT count(*)::int AS total FROM olos_export.projection_outbox WHERE state='confirmed'",
    );

    expect(confirmed.rows[0].total).toBe(3);

    const columns = await destination.admin.query(
      `SELECT count(*)::int AS total
       FROM information_schema.columns
       WHERE table_schema='raw' AND table_name='olos_calls'`,
    );

    expect(columns.rows[0].total).toBe(16);
    if(deferCheckpoint){
      const {PostgresLargeBatchPersistence}=await import('../src/persistence/large-batch.js');
      const {spoolLimits}=await import('../src/large-batch/config.js');const worker=source.worker();
      try{
        const finalizer=new PostgresLargeBatchPersistence(worker,spoolLimits(),300000,{sourceNamespace:'synthetic-source',destinationId,mappingVersion:'fixture-v1'},true);
        await finalizer.confirmDeliveryCheckpoint(deliveryManifest!);await finalizer.confirmDeliveryCheckpoint(deliveryManifest!);
        expect((await source.admin.query('SELECT version::text FROM olos_export.checkpoints')).rows[0].version).toBe('1');
      }finally{await worker.end();}
    }


  } finally {
    await projector?.end();
    await receiver?.end();
    await destination?.stop();
    await source.stop();
  }
});
