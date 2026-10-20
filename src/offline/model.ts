import { z } from 'zod';
import { parse } from 'lossless-json';
import { SaxesParser } from 'saxes';
import { validDate, canonical } from '../persistence/validation.js';
import type { Candidate, KnownField } from '../projection/contract.js';
import { agentId } from './agent-identity.js';

export const columnTypes = {
  internal_id_olos: 'bigint',olos_call_id: 'text',data_ligacao: 'timestamp',telefone: 'text',telefone_normalizado: 'text',telefone_match_key: 'text',
  cpf: 'text',cpf_normalizado: 'text',email: 'text',nome_agente: 'text',id_campanha_olos: 'text',nome_campanha_olos: 'text',
  id_tabulacao_olos: 'text',nome_tabulacao_olos: 'text',lead_id_origem: 'text',razao_desligamento: 'text',tma_origem: 'integer',tma_segundos: 'integer',data_carga: 'timestamp',raw_olos_call_id: 'bigint',
} as const;
const names = Object.keys(columnTypes) as [keyof typeof columnTypes,...(keyof typeof columnTypes)[]];
export const profileSchema = z.object({
  version: z.literal('offline-homologation/v1'),approval: z.literal('APPROVE_ISOLATED_OFFLINE_MAPPING_V1'),identityEvidence: z.string().min(10).max(500),
  columns: z.array(z.enum(names)).length(16).refine(a => new Set(a).size === 16 && a.includes('internal_id_olos')),
  xmlCpfNames: z.array(z.string().regex(/^[A-Za-z_][A-Za-z0-9_.-]*$/)).min(1).max(10),
  xmlEmailNames: z.array(z.string().regex(/^[A-Za-z_][A-Za-z0-9_.-]*$/)).min(1).max(10),
  xmlFieldAttributes: z.object({ name: z.string().min(1).max(50),value: z.string().min(1).max(50) }).strict().optional(),
  phonePolicy: z.enum(['digits-only','br-e164']),phoneMatchKey: z.enum(['unresolved','normalized']),
  timestampPolicy: z.enum(['reject-offset','utc-civil']),tmaPolicy: z.enum(['disabled','EndCall-minus-StartDate-provisional']),
  agentNameField: z.enum(['AgentName','UserName']).optional(),
}).strict();
export type OfflineProfile = z.infer<typeof profileSchema>;
export const exampleProfile: OfflineProfile = {
  version: 'offline-homologation/v1',approval: 'APPROVE_ISOLATED_OFFLINE_MAPPING_V1',identityEvidence: 'LOCAL HOMOLOGATION ONLY: internal_id_olos stores AttemptsRawDataID; no equivalence with CallId or raw_olos_call_id',
  columns: ['internal_id_olos','olos_call_id','data_ligacao','telefone','telefone_normalizado','nome_agente','id_campanha_olos','nome_campanha_olos',
    'id_tabulacao_olos','nome_tabulacao_olos','cpf','email','lead_id_origem','tma_origem','razao_desligamento','data_carga'],
  xmlCpfNames: ['CPF'],xmlEmailNames: ['Email'],phonePolicy: 'digits-only',phoneMatchKey: 'unresolved',timestampPolicy: 'reject-offset',tmaPolicy: 'disabled',
};
export type OfflineRecord = Readonly<Record<string,string | boolean | null>>;
export function parseLossless(text: string): unknown {
  try { return parse(text.replace(/^\uFEFF/,''),null,{ parseNumber: value => value,onDuplicateKey: () => { throw new Error('DUPLICATE_JSON_KEY'); } }); }
  catch { throw new Error('INVALID_PRIVATE_JSON'); }
}
export function recordsFromJson(input: unknown, allowEmpty = false): OfflineRecord[] {
  let value: unknown = input;
  if (!Array.isArray(value) && value && typeof value === 'object') {
    const o = value as Record<string,unknown>;
    if (o.ErrorMessage !== undefined && o.ErrorMessage !== null || o.output && typeof o.output === 'object' && (o.output as Record<string,unknown>).ErrorMessage != null) throw new Error('SOURCE_ERROR_METADATA');
    if (o.recordsets !== undefined) {
      if (!Array.isArray(o.recordsets) || o.recordsets.length !== 1 || o.recordset !== undefined && canonical(o.recordset) !== canonical(o.recordsets[0])) throw new Error('AMBIGUOUS_DATASETS');
      value = o.recordsets[0];
    } else {
      const present = ['recordset','records','data'].filter(k => o[k] !== undefined);
      if (present.length !== 1) throw new Error('UNSUPPORTED_JSON_LAYOUT'); value = o[present[0]!];
    }
  }
  if (!Array.isArray(value) || !allowEmpty && value.length < 1 || value.length > 10000) throw new Error('INVALID_RECORD_COUNT');
  return value.map(record => {
    if (!record || typeof record !== 'object' || Array.isArray(record)) throw new Error('INVALID_SOURCE_ROW');
    for (const [key,v] of Object.entries(record)) {
      if (/password|passwd|senha/i.test(key.replace(/[^a-z]/gi,'')) || !/^[A-Za-z_][A-Za-z0-9_]{0,127}$/.test(key) ||
        v !== null && typeof v !== 'string' && typeof v !== 'boolean') throw new Error('UNSAFE_SOURCE_VALUE');
    }
    return Object.freeze({ ...record }) as OfflineRecord;
  });
}
export function originalId(record: OfflineRecord): string {
  const id = record.AttemptsRawDataID;
  if (typeof id !== 'string' || id.length>20 || id==='-0' || !/^-?(0|[1-9]\d*)$/.test(id) || BigInt(id) < -9223372036854775808n || BigInt(id) > 9223372036854775807n) throw new Error('INVALID_ATTEMPTS_ID');
  return id;
}
export function validCpf(input: string): string | null {
  const s = input.trim().replace(/[.\-\s]/g,'');
  if (!/^\d{11}$/.test(s) || /^(\d)\1{10}$/.test(s)) return null;
  for (const length of [9,10]) {
    let total = 0; for (let i=0;i<length;i++) total += Number(s[i])*(length+1-i);
    const digit = total%11 < 2 ? 0 : 11-total%11; if (digit !== Number(s[length])) return null;
  }
  return s;
}
export function validEmail(input: string): string | null {
  const s = input.trim(); if (s.length > 254 || !/^[A-Za-z0-9.!#$%&'*+/=?^_`{|}~-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,63}$/.test(s)) return null;
  const [local,domain] = s.split('@');
  return !local || local.length>64 || local.startsWith('.') || local.endsWith('.') || local.includes('..') || domain!.split('.').some(p => !p || p.length>63 || p.startsWith('-') || p.endsWith('-')) ? null : s;
}
export function xmlContacts(xml: unknown, profile: OfflineProfile): { cpf: string | null; email: string | null; invalidXml: boolean; ambiguous: boolean } {
  const empty = { cpf: null,email: null,invalidXml: false,ambiguous: false };
  if (xml === null || xml === undefined || xml === '') return empty;
  if (typeof xml !== 'string' || Buffer.byteLength(xml)>262144) return { ...empty,invalidXml: true };
  const cpfs = new Set<string>(), emails = new Set<string>();
  const cpfNames = profile.xmlCpfNames.map(s => s.toLowerCase()), emailNames = profile.xmlEmailNames.map(s => s.toLowerCase());
  const capture = (name: string, text: string) => {
    if (cpfNames.includes(name.toLowerCase())) { const c = validCpf(text); if (c) cpfs.add(c); }
    if (emailNames.includes(name.toLowerCase())) { const e = validEmail(text); if (e) emails.add(e); }
  };
  try {
    const stack: { name: string; text: string; alias: string | null }[] = []; let nodes = 0;
    const parser = new SaxesParser({ xmlns: false });
    parser.on('doctype',() => { throw new Error('DTD_FORBIDDEN'); });
    parser.on('error',() => { throw new Error('INVALID_XML'); });
    parser.on('opentag',tag => {
      if (++nodes>20000 || stack.length>=32) throw new Error('XML_LIMIT');
      for (const [key,value] of Object.entries(tag.attributes)) capture(key,String(value));
      const rule = profile.xmlFieldAttributes, alias = rule ? String(tag.attributes[rule.name] ?? '') : null;
      if (alias && rule && rule.value !== '$text') capture(alias,String(tag.attributes[rule.value] ?? ''));
      stack.push({ name: tag.name,text: '',alias: rule?.value === '$text' ? alias : null });
    });
    const text = (value: string) => { const top = stack.at(-1); if (top) top.text += value; };
    parser.on('text',text); parser.on('cdata',text);
    parser.on('closetag',() => { const tag = stack.pop()!; capture(tag.name,tag.text); if (tag.alias) capture(tag.alias,tag.text); });
    // Olos CampaignData may be an XML fragment with multiple root elements.
    // Wrap only for parsing; never modify the original source payload.
    parser.write('<OlosCampaignDataRoot>');
    parser.write(xml);
    parser.write('</OlosCampaignDataRoot>').close();
    return { cpf: cpfs.size === 1 ? [...cpfs][0]! : null,email: emails.size === 1 ? [...emails][0]! : null,invalidXml: false,ambiguous: cpfs.size>1 || emails.size>1 };
  } catch { return { ...empty,invalidXml: true }; }
}
export function normalizePhone(value: string | null, policy: OfflineProfile['phonePolicy']): string | null {
  if (value === null || !/^[+()\s.\-\d]+$/.test(value)) return null;
  let n = value.replace(/\D/g,''); if (!n) return null;
  if (policy === 'br-e164') { if (/^55\d{10,11}$/.test(n)) return n; if (!/^[1-9]\d{9,10}$/.test(n)) return null; n = `55${n}`; }
  return n;
}
function temporal(value: string): { seconds: bigint; fraction: string; offset: number | null } | null {
  const m = /^(\d{4}-\d{2}-\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|[+-]\d{2}:\d{2})?$/.exec(value);
  if (!m || !validDate(m[1]!) || Number(m[2])>23 || Number(m[3])>59 || Number(m[4])>59) return null;
  let offset: number | null = null;
  if (m[6]) { if (m[6] === 'Z') offset = 0; else { const h = Number(m[6].slice(1,3)),min = Number(m[6].slice(4)); if (h>14 || min>59 || h===14 && min!==0) return null; offset = (h*60+min)*(m[6][0]==='-' ? -1 : 1); } }
  const civil = new Date(`${m[1]}T00:00:00.000Z`).getTime();
  return { seconds: BigInt(civil/1000+Number(m[2])*3600+Number(m[3])*60+Number(m[4]))-BigInt((offset ?? 0)*60),fraction: m[5] ?? '',offset };
}
export function businessTimestamp(value: unknown, policy: OfflineProfile['timestampPolicy']): string | null {
  if (typeof value !== 'string') return null; const t = temporal(value); if (!t || t.fraction.slice(6).replace(/0/g,'') !== '') return null;
  if (t.offset !== null && policy !== 'utc-civil') return null;
  const date = new Date(Number(t.seconds)*1000).toISOString().slice(0,19);
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$/.test(date)) return null;
  return `${date}${t.fraction ? `.${t.fraction.slice(0,6)}` : ''}`;
}
export function provisionalTma(start: unknown, end: unknown, enabled: boolean): string | null {
  if (!enabled || typeof start !== 'string' || typeof end !== 'string') return null;
  const a = temporal(start),b = temporal(end); if (!a || !b || (a.offset === null)!==(b.offset === null)) return null;
  const nanos = (b.seconds-a.seconds)*1000000000n+BigInt(b.fraction.padEnd(9,'0'))-BigInt(a.fraction.padEnd(9,'0'));
  return nanos<0n || nanos>86400n*1000000000n ? null : (nanos/1000000000n).toString();
}
export interface Catalogs { campaigns: ReadonlyMap<string,string>; dispositions: ReadonlyMap<string,string>; agents: ReadonlyMap<string,string>; }
export function catalog(records: OfflineRecord[], key: string, label: string): ReadonlyMap<string,string> {
  const result = new Map<string,string>();
  for (const row of records) { const id = row[key],name = row[label]; if (typeof id !== 'string' || typeof name !== 'string' || result.has(id) && result.get(id)!==name) throw new Error('CATALOG_CONFLICT'); result.set(id,name); }
  return result;
}
export function mapOffline(record: OfflineRecord, catalogs: Catalogs, profile: OfflineProfile, loadedAt: string) {
  const id = originalId(record), text = (key: string) => {
    const value=record[key]; if (value!==null && value!==undefined && typeof value!=='string') throw new Error('OFFLINE_SOURCE_TYPE_REQUIRES_REVIEW');
    return value as string | null | undefined ?? null;
  };
  const contact = xmlContacts(record.CampaignData,profile), phone = text('OriginalPhoneNumber'), normalized = normalizePhone(phone,profile.phonePolicy);
  const campaign = text('CampaignId'), disposition = text('DispositionId'), agent = text('AgentId');
  const tma = provisionalTma(record.StartDate,record.EndCall,profile.tmaPolicy !== 'disabled');
  const timestamp=businessTimestamp(record.StartDate,profile.timestampPolicy);
  if (record.StartDate!==null && record.StartDate!==undefined && timestamp===null) throw new Error('OFFLINE_TIMESTAMP_REQUIRES_REVIEW');
  const all: Candidate = { internal_id_olos: id,olos_call_id: text('CallId'),data_ligacao: timestamp,
    telefone: phone,telefone_normalizado: normalized,telefone_match_key: profile.phoneMatchKey==='normalized' ? normalized : null,
    cpf: contact.cpf,cpf_normalizado: contact.cpf,email: contact.email,nome_agente: agentId(agent) ? catalogs.agents.get(agent!) ?? null : null,
    id_campanha_olos: campaign,nome_campanha_olos: campaign ? catalogs.campaigns.get(campaign) ?? null : null,
    id_tabulacao_olos: disposition,nome_tabulacao_olos: disposition ? catalogs.dispositions.get(disposition) ?? null : null,
    lead_id_origem: null,razao_desligamento: null,raw_olos_call_id: null,tma_origem: tma,tma_segundos: tma,data_carga: loadedAt };
  const row = Object.fromEntries(profile.columns.map(c => [c,all[c] ?? null])) as Candidate;
  for (const value of Object.values(row)) if (typeof value==='string' && (/\u0000|[\uD800-\uDBFF](?![\uDC00-\uDFFF])|(?<![\uD800-\uDBFF])[\uDC00-\uDFFF]/.test(value))) throw new Error('OFFLINE_TEXT_REQUIRES_REVIEW');
  return { row,metrics: { cpf: contact.cpf ? 1 : 0,email: contact.email ? 1 : 0,tma: tma ? 1 : 0,invalidXml: contact.invalidXml ? 1 : 0,
    ambiguousXml: contact.ambiguous ? 1 : 0,missingTimestamp: row.data_ligacao === null ? 1 : 0,missingCampaign: campaign && !catalogs.campaigns.has(campaign) ? 1 : 0,missingDisposition: disposition && !catalogs.dispositions.has(disposition) ? 1 : 0 } };
}
