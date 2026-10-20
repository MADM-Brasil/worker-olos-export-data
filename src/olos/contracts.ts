import type { ExportTable } from '../sync/contracts.js';

// Histórico imutável v1 da Fase 3A, baseado apenas nos nomes informados naquele momento.
// A versão documental completa v2 está em documented-tables.ts; nenhuma é aprovada para consumo.
export { documentedTableContracts, manualEvidence } from './documented-tables.js';
export { verifiedProcedureContracts } from './procedure-signatures.js';
function table<T extends ExportTable>(name: T, key: string, kind: 'transactional' | 'reference') {
  return Object.freeze({ table: name, version: `${name}/v1`, kind,
    documentedKey: key, keyEvidence: 'empresa:phase3a:manual-key-name',
    fields: Object.freeze({ [key]: Object.freeze({ nameConfirmed: true, sqlType: null, nullable: null }) }),
    excludedFields: Object.freeze(name === 'Users' ? ['Password'] : []),
    approved: false as const, layoutComplete: false as const,
    pending: Object.freeze(['manual edition and full layout', 'SQL types, precision, scale and nullability',
      'key scope and stability', 'procedure signature and resultset association']) });
}
export const attemptsContract = table('AttemptsRawData', 'AttemptsRawDataID', 'transactional');
export const inboundContract = table('InboundRawData', 'InboundRawDataID', 'transactional');
export const pbxBillingContract = table('PbxBillingData', 'PbxBillingDataId', 'transactional');
export const usersContract = table('Users', 'UserId', 'reference');
export const campaignContract = table('Campaign', 'CampaignId', 'reference');
export const dispositionContract = table('Disposition', 'DispositionId', 'reference');
export const tableContracts = Object.freeze({ AttemptsRawData: attemptsContract, InboundRawData: inboundContract,
  PbxBillingData: pbxBillingContract, Users: usersContract, Campaign: campaignContract, Disposition: dispositionContract });
export const correlationIdentifiers = Object.freeze(['CallId','CallIdTelecom']);
// Nomes mencionados pela empresa, ainda sem associação a colunas/tipos/tabelas comprovados aqui.
// Não participam automaticamente de key_fields nem de qualquer correlação entre tabelas.

export const procedures = Object.freeze({
  ReadData: 'dbo.usp_ExportData_ReadData',
  ReadDataCurrentDay: 'dbo.usp_ExportData_ReadDataCurrentDay',
  ReadDataPreviousDays: 'dbo.usp_ExportData_ReadDataPreviousDays',
  LogReadData: 'dbo.usp_ExportData_LogReadData',
  ReReadData: 'dbo.usp_ExportData_ReReadData',
  ResetReadData: 'dbo.usp_ExportData_ResetReadData',
  DataAvailable: 'dbo.usp_ExportData_DataAvailable',
} as const);
export type Procedure = keyof typeof procedures;
export const procedureContracts = Object.freeze(Object.fromEntries(Object.keys(procedures).map(name => [name,
  Object.freeze({ version: `${name}/v1`, signatureConfirmed: false, parameters: null,
    sourceControlExclusive: false, realExecutionAllowed: false })])));
// procedureContracts mantém o snapshot v1. verifiedProcedureContracts expõe a comparação documental/servidor v2.
export const sensitiveField = (name: string): boolean => /password|passwd|senha/.test(name.toLowerCase().replace(/[^a-z]/g, ''));
