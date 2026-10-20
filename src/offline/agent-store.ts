import { createHash } from 'node:crypto';
import type { Pool,PoolClient } from 'pg';
import { transaction } from '../persistence/transaction.js';
import { PersistenceError } from '../persistence/types.js';
import { agentId,agentMap,buildAgentCatalog,type AgentSnapshot } from './agents.js';
const hash=(s:string)=>createHash('sha256').update(s).digest('hex');
interface HistoricalRow { call_id:string;original_id:string;agent_id:unknown;nome_agente:string|null;linked_agent:string|null;applied_name_hash:string|null; }
export async function agentCatalogInstalled(client: Pick<PoolClient,'query'>): Promise<boolean> {
  const row=(await client.query("SELECT to_regclass('olos_export.offline_agent_state') IS NOT NULL AS present")).rows[0];
  if (!row.present) return false;
  const ledger=(await client.query('SELECT version FROM olos_export.offline_agent_state WHERE singleton')).rows;
  if (ledger.length!==1 || ledger[0].version!==1) throw Error('AGENT_CATALOG_SCHEMA_NOT_READY');
  return true;
}
export async function loadAgentMap(client: Pick<PoolClient,'query'>): Promise<ReadonlyMap<string,string> | null> {
  if (!await agentCatalogInstalled(client)) return null;
  const rows=(await client.query<{agent_id:string;user_name:string}>('SELECT agent_id::text,user_name FROM olos_export.offline_agents')).rows;
  return agentMap(buildAgentCatalog(rows.map(r=>({AgentId:r.agent_id,UserName:r.user_name}))));
}
/** Do not let a fresh delivery overwrite a name edited outside the catalog either. */
export async function ownedAgentName(client: PoolClient,callId: string,requested: string | null): Promise<string | null> {
  if (requested===null) return null;
  const old=(await client.query<{nome_agente:string|null;applied_name_hash:string|null}>(`SELECT t.nome_agente,l.applied_name_hash FROM raw.olos_calls t
    LEFT JOIN olos_export.offline_agent_links l ON l.call_id=t.internal_id_olos WHERE t.internal_id_olos=$1 FOR UPDATE OF t`,[callId])).rows[0];
  return !old || old.nome_agente===null || old.nome_agente===requested || hash(old.nome_agente)===old.applied_name_hash ? requested : null;
}
/** In the business transaction: capture only a verified extraction identity and the name we own. */
export async function linkAgent(client: PoolClient,callId: string,id: unknown,name: string | null) {
  const valid=agentId(id); if (!valid) return;
  await client.query('INSERT INTO olos_export.offline_agent_links(call_id,agent_id,applied_name_hash) VALUES($1,$2,$3) ON CONFLICT(call_id) DO NOTHING',[callId,valid,name===null?null:hash(name)]);
  const prior=(await client.query<{agent_id:string}>('SELECT agent_id::text FROM olos_export.offline_agent_links WHERE call_id=$1 FOR UPDATE',[callId])).rows[0];
  if (!prior || prior.agent_id!==valid) throw Error('AGENT_ASSOCIATION_CHANGED');
  if (name!==null) await client.query('UPDATE olos_export.offline_agent_links SET applied_name_hash=$2 WHERE call_id=$1 AND applied_name_hash IS DISTINCT FROM $2',[callId,hash(name)]);
}
/** Same lock namespace as ingestion. Only the local business name can change; no source cursor/receipt rewrite. */
export async function updateAgentCatalog(pool: Pick<Pool,'connect'>,input: AgentSnapshot) {
  const checked=buildAgentCatalog(input.entries);
  if (checked.digest!==input.digest || !Number.isSafeInteger(input.received) || input.received<checked.entries.length || input.received>10000 ||
      !Number.isSafeInteger(input.skipped) || input.skipped<0 || input.skipped>input.received) throw Error('AGENT_CATALOG_CHANGED');
  return transaction(pool,async client=>{
    if (!(await client.query('SELECT pg_try_advisory_xact_lock(740210,1) AS acquired')).rows[0].acquired) throw new PersistenceError('BUSY');
    if (!await agentCatalogInstalled(client)) throw Error('AGENT_CATALOG_SCHEMA_NOT_READY');
    // Refuse catalog replacement during incomplete ingestion/delivery; replay that job first.
    const state=(await client.query('SELECT snapshot_digest,revision::text FROM olos_export.offline_agent_state WHERE singleton FOR UPDATE')).rows[0];
    const changed=state.snapshot_digest!==input.digest;
    if (changed && (await client.query('SELECT 1 FROM olos_export.offline_imports WHERE completed_at IS NULL LIMIT 1')).rowCount) throw Error('AGENT_CATALOG_INGESTION_PENDING');
    if (changed) {
      // Removal applies only to this reference snapshot, never operational calls.
      await client.query('DELETE FROM olos_export.offline_agents WHERE NOT(agent_id=ANY($1::integer[]))',[input.entries.map(e=>e.AgentId)]);
      for (const e of input.entries) await client.query(`INSERT INTO olos_export.offline_agents(agent_id,user_name) VALUES($1,$2)
        ON CONFLICT(agent_id) DO UPDATE SET user_name=EXCLUDED.user_name WHERE offline_agents.user_name IS DISTINCT FROM EXCLUDED.user_name`,[e.AgentId,e.UserName]);
      await client.query('UPDATE olos_export.offline_agent_state SET revision=revision+1,snapshot_digest=$1,received=$2,skipped=$3,updated_at=clock_timestamp() WHERE singleton',[input.digest,input.received,input.skipped]);
    }
    const names=agentMap(input); let cursor: string | null=null,updated=0,unmatched=0,unverified=0,matched=0;
    for (;;) {
      const rows: HistoricalRow[]=(await client.query<HistoricalRow>(`SELECT t.internal_id_olos::text AS call_id,
        (e.payload->>'source_record')::jsonb->>'AttemptsRawDataID' AS original_id,
        (e.payload->>'source_record')::jsonb->>'AgentId' AS agent_id,t.nome_agente,l.agent_id::text AS linked_agent,l.applied_name_hash
        FROM raw.olos_calls t JOIN olos_export.attempts_raw_data e ON e.payload->>'AttemptsRawDataID'=t.internal_id_olos::text
        LEFT JOIN olos_export.offline_agent_links l ON l.call_id=t.internal_id_olos
        WHERE e.contract_version='offline-json-archive/v1' AND ($1::bigint IS NULL OR t.internal_id_olos>$1::bigint)
        ORDER BY t.internal_id_olos LIMIT 500 FOR UPDATE OF t`,[cursor])).rows;
      if (!rows.length) break;
      for (const row of rows) {
        cursor=row.call_id;const id=agentId(row.agent_id);
        if (row.original_id!==row.call_id || !id || row.linked_agent!==null && row.linked_agent!==id) { unverified++; continue; }
        const name=names.get(id);
        if (name===undefined) { unmatched++; continue; }
        if (row.nome_agente!==null && row.nome_agente!==name && (row.applied_name_hash===null || hash(row.nome_agente)!==row.applied_name_hash)) { unverified++; continue; }
        matched++;
        const result=await client.query('UPDATE raw.olos_calls SET nome_agente=$2 WHERE internal_id_olos=$1 AND nome_agente IS DISTINCT FROM $2',[row.call_id,name]);
        updated+=result.rowCount ?? 0;await linkAgent(client,row.call_id,id,name);
      }
    }
    const revision=(await client.query('SELECT revision::text FROM olos_export.offline_agent_state WHERE singleton')).rows[0].revision;
    return {event:'global_agents_updated',changed,revision,agents:input.entries.length,received:input.received,skipped:input.skipped,matched,updated,unmatched,unverified,sqlServerQueries:0};
  });
}
