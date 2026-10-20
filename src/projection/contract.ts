// Catálogo de evidências, NÃO DDL aprovado. Nullability/defaults/FKs de ambos os modelos são desconhecidos.
const reference1 = {
  raw_olos_call_id: 'BIGINT', olos_call_id: 'TEXT', data_ligacao: 'TIMESTAMP', telefone: 'TEXT',
  telefone_normalizado: 'TEXT', telefone_match_key: 'TEXT', cpf: 'TEXT', cpf_normalizado: 'CHAR(11)',
  email: 'TEXT', nome_agente: 'TEXT', id_campanha_olos: 'TEXT', nome_campanha_olos: 'TEXT', produto_id: 'BIGINT',
  id_tabulacao_olos: 'TEXT', nome_tabulacao_olos: 'TEXT', tma_segundos: 'INTEGER', razao_desligamento: 'TEXT',
  lead_id_banco: 'BIGINT', deal_id_banco: 'BIGINT', hubspot_contact_id: 'TEXT', hubspot_deal_id: 'TEXT',
  created_at: 'TIMESTAMP', updated_at: 'TIMESTAMP',
} as const;
const reference2 = [
  'internal_id_olos','olos_call_id','data_ligacao','telefone','nome_agente','id_campanha_olos','nome_campanha_olos',
  'id_tabulacao_olos','nome_tabulacao_olos','cpf','lead_id_origem','tma_origem','razao_desligamento','email',
  'raw_olos_call_id','telefone_match_key','arquivo_origem','lote_carga','data_carga','created_at','updated_at',
  'kommo_event_hash','kommo_event_hash_processado','kommo_status_processamento','kommo_lead_id_encontrado',
  'kommo_match_origem','kommo_tentativas','kommo_processado_em','kommo_erro','kommo_pipeline_id_atual',
  'kommo_status_id_atual','kommo_batch_id','kommo_reservado_em',
] as const;
export type KnownField = keyof typeof reference1 | typeof reference2[number];
export type Candidate = Partial<Record<KnownField, string | boolean | null>>;
export type Ownership = 'olos_candidate' | 'local_candidate' | 'external_candidate' | 'undetermined';
const local = new Set(['telefone_normalizado','telefone_match_key','cpf_normalizado','tma_segundos']);
const external = new Set(['produto_id','lead_id_banco','deal_id_banco','hubspot_contact_id','hubspot_deal_id']);
const undetermined = new Set(['raw_olos_call_id','internal_id_olos','lead_id_origem','tma_origem','created_at','updated_at','arquivo_origem','lote_carga','data_carga']);
export const destinationVersion = 'raw.olos_calls/v1' as const;
export const fields: readonly KnownField[] = Object.freeze([...new Set([...Object.keys(reference1), ...reference2])] as KnownField[]);
export const destinationContract = Object.freeze({
  version: destinationVersion, relation: 'raw.olos_calls', physicalDdlConfirmed: false, callIdentityConfirmed: false,
  fields: Object.freeze(Object.fromEntries(fields.map(name => [name, Object.freeze({
    reference1Type: Object.hasOwn(reference1, name) ? reference1[name as keyof typeof reference1] : null,
    reference1PrimaryKey: name === 'raw_olos_call_id',
    inReference2: (reference2 as readonly string[]).includes(name), reference2Type: null,
    nullable: 'unknown', default: 'unknown',
    ownership: name.startsWith('kommo_') || external.has(name) ? 'external_candidate' :
      local.has(name) ? 'local_candidate' : undetermined.has(name) ? 'undetermined' : 'olos_candidate',
    requiresExplicitAuthorization: name.startsWith('kommo_') || external.has(name) || local.has(name) || undetermined.has(name) && name !== 'raw_olos_call_id',
    discrepancy: name === 'data_carga' ? 'select_only_in_reference1' : null,
  })])) as Record<KnownField, Readonly<{
    reference1Type: string | null; reference1PrimaryKey: boolean; inReference2: boolean; reference2Type: null;
    nullable: 'unknown'; default: 'unknown'; ownership: Ownership; requiresExplicitAuthorization: boolean;
    discrepancy: string | null;
  }>>),
});
export function isKnownField(value: string): value is KnownField { return Object.hasOwn(destinationContract.fields, value); }
