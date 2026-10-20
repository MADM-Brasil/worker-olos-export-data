import { destinationContract, destinationVersion, fields, isKnownField, type Candidate, type KnownField } from './contract.js';
import { validDate } from '../persistence/validation.js';

export type Scalar = string | boolean | null;
export interface AuthorizedReference {
  readonly evidence: string;
  readonly category: 'local_transform' | 'external';
  readonly values: Readonly<Record<string, unknown>>;
}
export interface MappingPlan {
  readonly version: string;
  readonly approvalEvidence: string;
  readonly identity: { readonly approved: boolean; readonly evidence: string; readonly granularityEvidence: string; readonly sourceKeyFields: readonly string[] };
  readonly requiredFields: readonly KnownField[]; // Completude da projeção, não nullability do banco.
  readonly ignoredSourceFields: readonly string[];
  readonly authorizations: Readonly<Partial<Record<KnownField, string>>>;
  readonly bindings: readonly { readonly target: KnownField; readonly sourceField: string; readonly reference?: string }[];
}
export interface ProjectionIssue { readonly code: string; readonly field?: KnownField; }
export type FieldResolution = { readonly state: 'absent' | 'protected' | 'unresolved' } |
  { readonly state: 'value' | 'null'; readonly value: Scalar; readonly origin: 'olos' | 'local_transform' | 'external' };
export interface ProjectionResult {
  readonly status: 'ready' | 'pending' | 'rejected';
  readonly contractVersion: typeof destinationVersion;
  readonly mappingVersion: string;
  readonly original: Readonly<Record<string, Scalar>>;
  readonly fields: Readonly<Record<KnownField, FieldResolution>>;
  readonly issues: readonly ProjectionIssue[];
  readonly row?: Readonly<Candidate>;
}
export class ProjectionError extends Error {
  constructor(readonly code: string) { super(code); this.name = 'ProjectionError'; }
}
function evidence(value: unknown): value is string { return typeof value==='string' && value.trim().length>0; }
export function exactFlat(input: Readonly<Record<string, unknown>>): Readonly<Record<string, Scalar>> {
  if (!input || typeof input !== 'object' || Array.isArray(input) || (Object.getPrototypeOf(input) !== Object.prototype && Object.getPrototypeOf(input) !== null)) throw new ProjectionError('INVALID_SOURCE');
  const output: Record<string, Scalar> = Object.create(null) as Record<string, Scalar>;
  for (const [key, value] of Object.entries(input)) {
    if (/password|passwd|senha/.test(key.toLowerCase().replace(/[^a-z]/g,''))) throw new ProjectionError('SENSITIVE_FIELD');
    if (value !== null && typeof value !== 'string' && typeof value !== 'boolean') throw new ProjectionError('INEXACT_VALUE');
    output[key] = value;
  }
  return Object.freeze(output);
}
export function validateCandidateValue(field: KnownField, value: Scalar): void {
  if (value === null) return; // Permissão de null físico será verificada somente com contrato aprovado.
  const type = destinationContract.fields[field].reference1Type;
  if (type === null) throw new ProjectionError('UNKNOWN_DESTINATION_TYPE');
  if (typeof value !== 'string') throw new ProjectionError('INVALID_DESTINATION_VALUE');
  if (type === 'BIGINT' || type === 'INTEGER') {
    if (!/^-?(0|[1-9]\d*)$/.test(value)) throw new ProjectionError('INEXACT_IDENTIFIER');
    const n = BigInt(value), bound = type === 'BIGINT' ? 9223372036854775808n : 2147483648n;
    if (n < -bound || n >= bound || n.toString() !== value) throw new ProjectionError('NUMERIC_OVERFLOW');
  }
  if (type === 'TIMESTAMP') {
    const match = /^(\d{4}-\d{2}-\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,6}))?$/.exec(value);
    if (!match || !validDate(match[1]!) || Number(match[2]) > 23 || Number(match[3]) > 59 || Number(match[4]) > 59) throw new ProjectionError('TEMPORAL_POLICY_REQUIRED');
  }
  // Não aplicar padding/trim ou validação de CPF inventada.
  if (type === 'CHAR(11)' && value.length !== 11) throw new ProjectionError('NORMALIZATION_POLICY_REQUIRED');
}

// Nenhum binding, join, normalização, lookup ou gerador de identidade é implícito.
export function projectCall(source: Readonly<Record<string, unknown>>, references: Readonly<Record<string, AuthorizedReference>>, plan: MappingPlan): ProjectionResult {
  const original = exactFlat(source);
  const resolved = Object.fromEntries(fields.map(f => [f, { state: destinationContract.fields[f].requiresExplicitAuthorization ? 'protected' : 'absent' }])) as Record<KnownField, FieldResolution>;
  const issues: ProjectionIssue[] = [];
  const row: Candidate = Object.create(null) as Candidate;
  let rejected = false, blocked = false;
  const add = (code: string, field?: KnownField) => { issues.push(field === undefined ? { code } : { code, field }); };
  if (!evidence(plan.approvalEvidence) || !evidence(plan.version)) { blocked = true; add('MAPPING_UNAPPROVED'); }
  const used = new Set(plan.identity.sourceKeyFields);
  const bound = new Set<KnownField>();
  for (const binding of plan.bindings) {
    if (!isKnownField(binding.target) || bound.has(binding.target)) { rejected = true; add('INVALID_BINDING'); continue; }
    bound.add(binding.target);
    const target = binding.target;
    if (destinationContract.fields[target].requiresExplicitAuthorization && (!Object.hasOwn(plan.authorizations,target) || !evidence(plan.authorizations[target]))) {
      blocked = true; add('FIELD_AUTHORIZATION_REQUIRED', target); continue;
    }
    let values: Readonly<Record<string, unknown>> = original;
    let origin: 'olos' | 'local_transform' | 'external' = 'olos';
    if (binding.reference !== undefined) {
      const ref = Object.hasOwn(references, binding.reference) ? references[binding.reference] : undefined;
      if (!ref || !evidence(ref.evidence) || !['local_transform','external'].includes(ref.category)) { blocked = true; resolved[target] = { state: 'unresolved' }; add('REFERENCE_UNRESOLVED',target); continue; }
      values = exactFlat(ref.values); origin = ref.category;
    } else { used.add(binding.sourceField); }
    if (!Object.hasOwn(values, binding.sourceField)) { resolved[target] = { state: 'absent' }; add('FIELD_ABSENT',target); continue; }
    const value = values[binding.sourceField] as Scalar;
    try { validateCandidateValue(target,value); }
    catch (error) {
      blocked = true; resolved[target] = { state: 'unresolved' };
      add(error instanceof ProjectionError ? error.code : 'INVALID_VALUE',target); continue;
    }
    row[target] = value;
    resolved[target] = { state: value === null ? 'null' : 'value', value, origin };
  }
  for (const key of Object.keys(original)) if (!used.has(key) && !plan.ignoredSourceFields.includes(key)) { blocked = true; add('UNMAPPED_SOURCE_FIELD'); }
  const identity = plan.identity;
  if (identity.approved!==true || !evidence(identity.evidence) || !evidence(identity.granularityEvidence) || identity.sourceKeyFields.length === 0 ||
    identity.sourceKeyFields.some(k => !Object.hasOwn(original,k) || original[k] === null || original[k] === '') ||
    typeof row.raw_olos_call_id !== 'string' || row.raw_olos_call_id === '') { blocked = true; add('IDENTITY_UNPROVEN'); }
  for (const field of plan.requiredFields) if (!isKnownField(field) || !Object.hasOwn(row,field)) { blocked = true; add('REQUIRED_FIELD_UNRESOLVED', isKnownField(field) ? field : undefined); }
  for (const field of fields) if (!bound.has(field)) add('FIELD_UNRESOLVED',field);
  const status = rejected ? 'rejected' : blocked ? 'pending' : 'ready';
  const result = { status, contractVersion: destinationVersion, mappingVersion: plan.version, original,
    fields: Object.freeze(resolved), issues: Object.freeze(issues) } as const;
  return status === 'ready' ? { ...result, row: Object.freeze(row) } : result;
}
