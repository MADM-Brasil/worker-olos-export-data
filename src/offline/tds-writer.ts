import type { PoolClient } from 'pg';
import type { Candidate } from '../projection/contract.js';
import type { DeliveryRequest } from '../projection/dispatcher.js';
import type { TransactionalBusinessWriter } from '../projection/postgres-target.js';
import type { OfflineProfile } from './model.js';

export const APPROVED_COLUMNS = [
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
] as const;

export class IsolatedTdsCallsWriter implements TransactionalBusinessWriter {
  readonly writeBatchInTransaction?: (client:PoolClient,requests:readonly DeliveryRequest[])=>Promise<void>;
  constructor(private readonly profile: OfflineProfile,private readonly preserveExisting=false) {
    // Only the existing insert/preserve policy is batched. Other writers keep their single-row path.
    if(preserveExisting)this.writeBatchInTransaction=async(client,requests)=>{
      if(requests.some(r=>this.preflight(r.row)!=='allowed'))throw Error('ISOLATED_TDS_ROW_REJECTED');
      const columns=this.profile.columns.map(c=>`"${c}"`).join(',');
      await client.query(`INSERT INTO raw.olos_calls (${columns})
        SELECT ${columns} FROM jsonb_populate_recordset(NULL::raw.olos_calls,$1::jsonb)
        ON CONFLICT (internal_id_olos) DO NOTHING`,[JSON.stringify(requests.map(r=>r.row))]);
    };
  }

  preflight(row: Readonly<Candidate>): 'allowed' | 'rejected' {
    const columns = this.profile.columns;

    if (
      columns.length !== APPROVED_COLUMNS.length ||
      columns.some((column, index) => column !== APPROVED_COLUMNS[index]) ||
      !columns.includes('internal_id_olos') ||
      !columns.includes('raw_olos_call_id') ||
      Object.keys(row).length !== 16 ||
      !columns.every(c => Object.hasOwn(row, c)) ||
      typeof row.internal_id_olos !== 'string' ||
      !/^[1-9]\d*$/.test(row.internal_id_olos) ||
      (['raw_olos_call_id','lead_id_origem','tma_origem','razao_desligamento'] as const).some(c=>row[c]!==null)
    ) {
      return 'rejected';
    }

    return 'allowed';
  }

  async writeInTransaction(
    client: PoolClient,
    request: DeliveryRequest,
  ): Promise<void> {
    if (this.preflight(request.row) !== 'allowed') {
      throw new Error('ISOLATED_TDS_ROW_REJECTED');
    }

    const columns = this.profile.columns;
    const updates = columns.filter(c => c !== 'internal_id_olos');

    const assignments = updates.map(c => {
      // Não sobrescrever um nome existente sem política
      // comprovada de posse/atualização pelo catálogo.
      if (c === 'nome_agente') {
        return '"nome_agente"=COALESCE(target."nome_agente",EXCLUDED."nome_agente")';
      }

      return `"${c}"=COALESCE(EXCLUDED."${c}",target."${c}")`;
    });

    const identifiers = columns.map(c => `"${c}"`);

    await client.query(
      `INSERT INTO raw.olos_calls AS target
       (${identifiers.join(',')})
       VALUES (${columns.map((_, i) => `$${i + 1}`).join(',')})
       ON CONFLICT (internal_id_olos)
       ${this.preserveExisting ? 'DO NOTHING' : `DO UPDATE SET ${assignments.join(',')}`}`,
      columns.map(c => request.row[c] ?? null),
    );
  }
}
