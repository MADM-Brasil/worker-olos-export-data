import type { ExportTable } from './contracts.js';
import type { ReadType } from '../persistence/types.js';

// Modelos INTERNOS; não afirmam nomes, tipos ou ordem dos parâmetros das procedures.
export interface ReconciliationRequest {
  readonly caseId: string;
  readonly sourceTable: ExportTable;
  readonly readType: ReadType;
  readonly exportDate: string;
  readonly pkRange: Readonly<Record<string, string>> | null;
}
export interface LogReadEvidence {
  readonly digest: string;
  readonly conclusion: 'inconclusive' | 'remote_read_observed';
}
export interface ValidatedReReadPlan {
  readonly request: ReconciliationRequest;
  readonly procedureContractEvidence: string;
  readonly operatorApprovalReference: string;
}
export interface OlosRecoveryPort {
  // Futuro adaptador dbo.usp_ExportData_LogReadData: sem implementação nesta fase.
  inspectLogReadData(request: ReconciliationRequest, signal: AbortSignal): Promise<LogReadEvidence>;
  // Futuro adaptador dbo.usp_ExportData_ReReadData: apenas planejar até validar a semântica real.
  planReReadData(request: ReconciliationRequest, evidence: LogReadEvidence): Promise<ValidatedReReadPlan>;
}
