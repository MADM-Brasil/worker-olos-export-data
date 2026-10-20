import type { Procedure } from './contracts.js';

export type OlosFailure = 'REAL_EXECUTION_BLOCKED' | 'PROCEDURE_BLOCKED' | 'CONTRACT_UNCONFIRMED' | 'INVALID_PARAMETER' |
  'CONNECTION_FAILED' | 'RECONNECT_BACKOFF' | 'EXECUTION_FAILED' | 'TIMEOUT' | 'OUTPUT_ERROR' | 'INVALID_RESPONSE' |
  'PRECISION_UNSAFE' | 'LIMIT_EXCEEDED' | 'BUSY' | 'ABORTED' | 'REPETITION_UNCONFIRMED' | 'NO_PROGRESS';
// Não contém mensagem do SQL Server, parâmetros, payload, stack original ou cause.
export class OlosError extends Error {
  constructor(readonly code: OlosFailure, readonly outcome: 'not_sent' | 'unknown' = 'not_sent') {
    super(code); this.name = 'OlosError';
  }
}
// Suporte deliberadamente restrito a parâmetros que não exigem number/Date inexatos.
// Outros tipos somente após contrato e codec próprios. Nenhum nome abaixo é assinatura Olos presumida.
export type ParameterType = { readonly kind: 'nvarchar' | 'varchar'; readonly length: number | 'max' } |
  { readonly kind: 'bigint' | 'int' | 'bit' | 'date' };
export interface Parameter {
  readonly name: string;
  readonly direction: 'input' | 'output';
  readonly type: ParameterType;
  readonly nullable: boolean;
}
export interface ProcedureSignature {
  readonly procedure: Procedure;
  readonly version: string;
  readonly evidence: string;
  readonly parameters: readonly Parameter[];
  // Associação de OUTPUT fornecida pelo contrato, sem adivinhar tipo/tamanho ou returnValue.
  readonly messageOutput: string;
  readonly errorOutput: string;
}
export interface PreparedCall {
  readonly procedure: Procedure;
  readonly signature: ProcedureSignature;
  readonly values: Readonly<Record<string, string | boolean | null>>;
  readonly timeoutMs: number;
}
export type NativeSqlType = 'bigint' | 'int' | 'smallint' | 'tinyint' | 'decimal' | 'numeric' |
  'nvarchar' | 'varchar' | 'nchar' | 'char' | 'uniqueidentifier' | 'bit' |
  'datetime2' | 'datetimeoffset' | 'datetime' | 'smalldatetime' | 'varbinary' | 'binary';
export interface Column {
  readonly name: string;
  readonly type: NativeSqlType;
  readonly precision?: number;
  readonly scale?: number;
  readonly length?: number | 'max'; // unidades da declaração; NVARCHAR/NCHAR em unidades UTF-16, não bytes de catálogo
}
export interface Dataset {
  readonly columns: readonly Column[];
  readonly rows: readonly Readonly<Record<string, unknown>>[];
  // Não certifica strings produzidas a posteriori a partir de number/Date arredondados.
  readonly representation: 'mssql-native' | 'exact-text/v1';
  readonly exactEncodingEvidence?: string;
}
export interface ProcedureResponse {
  readonly datasets: readonly Dataset[];
  readonly output: Readonly<Record<string, unknown>>;
  readonly returnValue: unknown;
}
export interface ProcedureReply extends ProcedureResponse {
  // Conteúdo de origem: somente em memória, nunca em logs/erros/controles duráveis.
  readonly Message: string | null;
  readonly ErrorMessage: string | null;
}
export interface ProcedureTransport {
  readonly kind: 'synthetic' | 'mssql';
  connect(): Promise<void>;
  // Deve respeitar timeout/cancelamento nativos e só resolver quando o transporte tiver terminado.
  invoke(call: PreparedCall, signal: AbortSignal): Promise<ProcedureResponse>;
  close(): Promise<void>;
}
