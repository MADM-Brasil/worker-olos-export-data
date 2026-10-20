import { destinationContract, destinationVersion, isKnownField, type Candidate, type KnownField } from './contract.js';
import { exactFlat, ProjectionError, validateCandidateValue } from './transform.js';
import type { DeliveryRequest } from './dispatcher.js';
import type { PoolClient } from 'pg';
import type { TransactionalBusinessWriter } from './postgres-target.js';

export interface ColumnApproval { readonly column: KnownField; readonly evidence: string; readonly nullable: boolean; readonly ownershipEvidence?: string; }
export interface WriteApproval {
  readonly ddlEvidence: string; readonly identityEvidence: string; readonly contractVersion: typeof destinationVersion;
  readonly columns: readonly ColumnApproval[];
}
// Compila somente um plano revisável. Não abre pool e não altera raw.
// A PK é a única declarada em R1; seu uso físico continua condicionado ao gate do writer.
export function prepareRawStatement(row: Readonly<Candidate>, approval: WriteApproval): { text: string; values: readonly (string | boolean | null)[] } {
  if (!approval.ddlEvidence || !approval.identityEvidence || approval.contractVersion!==destinationVersion) throw new ProjectionError('DESTINATION_BLOCKED');
  const values = exactFlat(row), columns = Object.keys(values);
  if (!columns.includes('raw_olos_call_id') || values.raw_olos_call_id===null || columns.length===0) throw new ProjectionError('IDENTITY_UNPROVEN');
  const allowed = new Map(approval.columns.map(c => [c.column,c]));
  if (allowed.size!==approval.columns.length) throw new ProjectionError('INVALID_ALLOWLIST');
  for (const column of columns) {
    if (!isKnownField(column)) throw new ProjectionError('COLUMN_NOT_ALLOWED');
    const field = destinationContract.fields[column], granted = allowed.get(column);
    if (!granted?.evidence || field.reference1Type===null || field.requiresExplicitAuthorization && !granted.ownershipEvidence) throw new ProjectionError('COLUMN_NOT_ALLOWED');
    if (values[column]===null && !granted.nullable) throw new ProjectionError('NULL_NOT_ALLOWED');
    validateCandidateValue(column,values[column]!);
  }
  // Identificadores vêm da allowlist fixa de campos conhecidos, nunca de SQL recebido.
  const identifiers = columns.map(c => `"${c}"`);
  const updates = columns.filter(c => c!=='raw_olos_call_id').map(c => `"${c}"=EXCLUDED."${c}"`);
  return { text: `INSERT INTO raw.olos_calls (${identifiers.join(',')}) VALUES (${columns.map((_,i) => `$${i+1}`).join(',')}) ON CONFLICT (raw_olos_call_id) ${updates.length ? `DO UPDATE SET ${updates.join(',')}` : 'DO NOTHING'}`,
    values: columns.map(c => values[c]!) };
}
export class RawCallsWriter implements TransactionalBusinessWriter {
  constructor(private readonly approval?: WriteApproval) {}
  // Não há flag/env/argumento que aprove DDL ou identidade reais nesta entrega.
  preflight(row: Readonly<Candidate>): 'allowed' | 'blocked' | 'rejected' {
    if (!destinationContract.physicalDdlConfirmed || !destinationContract.callIdentityConfirmed || !this.approval) return 'blocked';
    try { prepareRawStatement(row,this.approval); return 'allowed'; }
    catch { return 'rejected'; }
  }
  async writeInTransaction(client: PoolClient, request: DeliveryRequest): Promise<void> {
    if (this.preflight(request.row)!=='allowed' || !this.approval) throw new ProjectionError('DESTINATION_BLOCKED');
    const statement = prepareRawStatement(request.row,this.approval);
    await client.query(statement.text,[...statement.values]);
  }
}
