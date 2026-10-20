import { describe, expect, it } from 'vitest';
import { projectTdsAttempts } from '../src/offline/tds-projection.js';
import { exampleProfile, type Catalogs, type OfflineProfile } from '../src/offline/model.js';

describe('TDS Attempts -> 16 colunas', () => {
  it('preserva identidade e converte timestamp sem alterar a origem', () => {
    const bytes = Buffer.alloc(8);
    bytes.writeInt32LE(0, 0);
    bytes.writeUInt32LE(1, 4);

    const originalDate = `sql-datetime/v1:${bytes.toString('hex')}`;

    const source = {
      AttemptsRawDataID: '123',
      CallId: '456',
      AgentId: '1592',
      StartDate: originalDate,
      EndCall: null,
      EndWrap: null,
      OriginalPhoneNumber: '11999999999',
      CampaignId: '10',
      DispositionId: '20',
      CampaignData: null,
    };

    const catalogs: Catalogs = {
      agents: new Map([['1592', 'Operador Sintetico']]),
      campaigns: new Map([['10', 'Campanha Teste']]),
      dispositions: new Map([['20', 'Tabulacao Teste']]),
    };

    const profile: OfflineProfile = {
      ...exampleProfile,
      columns: [
        ...exampleProfile.columns.filter(c => c !== 'data_carga'),
        'raw_olos_call_id',
      ],
      timestampPolicy: 'utc-civil' as const,
    };

    const result = projectTdsAttempts(
      source,
      catalogs,
      profile,
      '2026-10-19T00:00:00',
    );

    expect(result.row.internal_id_olos).toBe('123');
    expect(result.row.olos_call_id).toBe('456');
    expect(result.row.data_ligacao).toBe('1900-01-01T00:00:00.003333');
    expect(result.row.nome_agente).toBe('Operador Sintetico');
    expect(result.row.raw_olos_call_id).toBeNull();
    expect(Object.keys(result.row)).toHaveLength(16);

    expect(source.StartDate).toBe(originalDate);
  });

  it('rejeita data TDS inválida', () => {
    const catalogs: Catalogs = {
      agents: new Map(),
      campaigns: new Map(),
      dispositions: new Map(),
    };

    expect(() => projectTdsAttempts(
      {
        AttemptsRawDataID: '123',
        StartDate: '2026-10-19T10:00:00',
      },
      catalogs,
      exampleProfile,
      '2026-10-19T00:00:00',
    )).toThrow('INVALID_SQL_DATETIME');
  });
});
