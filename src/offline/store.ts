import type { Pool, PoolClient } from 'pg';
import { PostgresDeliveryTarget, type TransactionalBusinessWriter } from '../projection/postgres-target.js';
import { ProjectionOutbox } from '../projection/outbox.js';
import { ProjectionDispatcher } from '../projection/dispatcher.js';
import { fields as knownFields, destinationVersion, type Candidate } from '../projection/contract.js';
import type { FieldResolution } from '../projection/transform.js';
import { parseLossless, mapOffline, type Catalogs, type OfflineRecord, type OfflineProfile } from './model.js';
import { agentCatalogInstalled,linkAgent,ownedAgentName } from './agent-store.js';
import { agentId } from './agent-identity.js';

export interface OfflineJob { id: string; input_digest: string; mapping_digest: string; run_id: string; stream_id: string; fencing_token: string; batch_id: string;
  expected: number; confirmed: number; loaded_at: string; spool_manifest: unknown; metrics: Record<string,number>; }
/** Constructed only for the isolated offline destination after its Docker identity is checked. Global raw writer stays blocked. */
class OfflineWriter implements TransactionalBusinessWriter {
  constructor(private readonly profile: OfflineProfile) {}
  preflight(row: Readonly<Candidate>): 'allowed' | 'rejected' {
    return Object.keys(row).length === this.profile.columns.length && this.profile.columns.every(c => Object.hasOwn(row,c)) &&
      typeof row.internal_id_olos === 'string' && /^-?(0|[1-9]\d*)$/.test(row.internal_id_olos) ? 'allowed' : 'rejected';
  }
  async writeInTransaction(client: PoolClient, request: Parameters<TransactionalBusinessWriter['writeInTransaction']>[1]): Promise<void> {
    const job = (await client.query<{ id: string; payload: Record<string,string> }>(`SELECT i.id,o.payload FROM olos_export.offline_imports i JOIN olos_export.projection_outbox o ON o.batch_id=i.batch_id
      WHERE o.delivery_id=$1 AND o.destination_id=$2 FOR UPDATE OF i`,[request.deliveryId,request.destinationId])).rows[0];
    if (!job) throw new Error('OFFLINE_JOB_MISSING');
    const linked=this.profile.columns.includes('nome_agente') && await agentCatalogInstalled(client);
    const original=linked ? parseLossless(job.payload.source_record!) as OfflineRecord : null;
    let name=request.row.nome_agente as string | null;
    if (original) {
      if (original.AttemptsRawDataID!==request.row.internal_id_olos) throw Error('AGENT_ASSOCIATION_UNVERIFIED');
      if (name!==null) {
        const id=agentId(original.AgentId), authorized=id ? (await client.query('SELECT user_name FROM olos_export.offline_agents WHERE agent_id=$1',[id])).rows[0]?.user_name : undefined;
        if (authorized!==name) throw Error('AGENT_ASSOCIATION_UNVERIFIED');
      }
      await linkAgent(client,request.row.internal_id_olos as string,original.AgentId,null);
      name=await ownedAgentName(client,request.row.internal_id_olos as string,name);
    }
    // Column identifiers come solely from the validated local profile allowlist, never arbitrary input SQL.
    const cols = this.profile.columns.map(c => `"${c}"`), updates = this.profile.columns.filter(c => c !== 'internal_id_olos');
    const expression = (c: typeof updates[number]) => c === 'data_carga' ? `COALESCE(target."${c}",EXCLUDED."${c}")` : `COALESCE(EXCLUDED."${c}",target."${c}")`;
    await client.query(`INSERT INTO raw.olos_calls AS target(${cols.join(',')}) VALUES(${cols.map((_,i) => `$${i+1}`).join(',')})
      ON CONFLICT(internal_id_olos) DO UPDATE SET ${updates.map(c => `"${c}"=${expression(c)}`).join(',')}
      WHERE ROW(${updates.map(c => `target."${c}"`).join(',')}) IS DISTINCT FROM ROW(${updates.map(expression).join(',')})`,this.profile.columns.map(c => linked && c==='nome_agente' ? name : request.row[c] ?? null));
    if (original) await linkAgent(client,request.row.internal_id_olos as string,original.AgentId,name);
    const inserted = await client.query('INSERT INTO olos_export.offline_delivered(delivery_id,import_id) VALUES($1,$2) ON CONFLICT DO NOTHING RETURNING delivery_id',[request.deliveryId,job.id]);
    if (inserted.rowCount) await client.query('UPDATE olos_export.offline_imports SET confirmed=confirmed+1,completed_at=CASE WHEN confirmed+1=expected THEN clock_timestamp() ELSE completed_at END WHERE id=$1',[job.id]);
  }
}
export function offlineDelivery(pool: Pool, destination: string, profile: OfflineProfile, catalogs: Catalogs, loadedAt: string) {
  const outbox = new ProjectionOutbox(pool,destination,60000,1000), target = new PostgresDeliveryTarget(pool,destination,new OfflineWriter(profile));
  const dispatcher = new ProjectionDispatcher(outbox,target,message => {
    const record = parseLossless(message.payload.source_record as string) as OfflineRecord;
    const result = mapOffline(record,catalogs,profile,loadedAt);
    const resolutions = Object.fromEntries(knownFields.map(k => [k,{ state: Object.hasOwn(result.row,k) ? result.row[k] === null ? 'null' : 'value' : 'absent',
      ...(Object.hasOwn(result.row,k) ? { value: result.row[k]!,origin: 'local_transform' } : {}) }])) as Record<typeof knownFields[number],FieldResolution>;
    return { status: 'ready',contractVersion: destinationVersion,mappingVersion: message.mapping_version,original: message.payload,fields: resolutions,issues: [],row: result.row };
  });
  return { dispatcher,target,outbox };
}
