import { digest } from '../persistence/validation.js';
import { parseLossless, recordsFromJson } from './model.js';
import { agentId } from './agent-identity.js';
export { agentId } from './agent-identity.js';

export interface AgentEntry { readonly AgentId: string; readonly UserName: string; }
export interface AgentSnapshot { readonly entries: readonly AgentEntry[]; readonly digest: string; readonly received: number; readonly skipped: number; }
function validName(value: unknown): value is string {
  return typeof value==='string' && value.trim().length>0 && value.length<=510 &&
    !/[\u0000-\u001f\u007f]|[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/.test(value);
}
/** Accept only an already sanitized snapshot, never a raw Users dataset. Errors contain no row values. */
export function buildAgentCatalog(rows: readonly unknown[]): AgentSnapshot {
  if (rows.length>10000) throw Error('AGENT_CATALOG_LIMIT');
  const names=new Map<string,string>(), blocked=new Set<string>(); let skipped=0;
  for (const row of rows) {
    if (!row || typeof row!=='object' || Array.isArray(row) || Object.keys(row).some(k=>k!=='AgentId' && k!=='UserName')) throw Error('UNSAFE_AGENT_CATALOG');
    const r=row as Record<string,unknown>, id=agentId(r.AgentId);
    if (!id) { skipped++; continue; }
    if (!validName(r.UserName)) { blocked.add(id); skipped++; continue; }
    if (names.has(id) && names.get(id)!==r.UserName) throw Error('AGENT_CATALOG_CONFLICT');
    names.set(id,r.UserName);
  }
  for (const id of blocked) names.delete(id);
  const entries=Object.freeze([...names].sort(([a],[b])=>Number(a)-Number(b)).map(([AgentId,UserName])=>Object.freeze({AgentId,UserName})));
  return Object.freeze({entries,digest:digest(entries),received:rows.length,skipped});
}
export function parseAgentCatalog(text: string): AgentSnapshot {
  const value=parseLossless(text);
  if (value && !Array.isArray(value) && typeof value==='object' && Object.keys(value).some(k=>!['recordset','recordsets','records','data'].includes(k))) throw Error('UNSAFE_AGENT_CATALOG');
  return buildAgentCatalog(recordsFromJson(value,true));
}
/** Future acquisition boundary ONLY: immediately drop every column except the two authorized ones.
 * No transport, logging, raw spool, mutation of driver rows, or authorization to read Users. */
export function sanitizeUsersInMemory(rows: readonly Readonly<Record<string,unknown>>[]): AgentSnapshot {
  if (rows.length>10000) throw Error('AGENT_CATALOG_LIMIT');
  try { return buildAgentCatalog(rows.map(row=>({AgentId:row.AgentId,UserName:row.UserName}))); }
  catch { throw Error('USERS_SANITIZATION_REJECTED'); }
}
export function agentMap(snapshot: AgentSnapshot): ReadonlyMap<string,string> { return new Map(snapshot.entries.map(e=>[e.AgentId,e.UserName])); }
