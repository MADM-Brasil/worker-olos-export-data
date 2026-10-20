import type { ExportTable } from '../sync/contracts.js';

export const manualEvidence = Object.freeze({ code: 'DBA-MN002', title: 'ExportData V3.0 - Cliente',
  revision: '1', revisionDate: '2020-06-03', pages: 33, path: 'docs/reference/olos-exportdata-v3.pdf',
  sha256: 'db4f934af564f0e5a3bb244cdb056e50e8b294dfcf6f94c25d14987ee95c1b71' });
export type DocumentedSqlType = 'int' | 'datetime' | `varchar(${number | 'max'})` | `nvarchar(${number | 'max'})` | 'char(16)';
export interface DocumentedField {
  readonly name: string;
  readonly sqlType: DocumentedSqlType;
  readonly page: number;
  readonly nullable: null;
  readonly serverVerification: null;
}
type Layout = readonly (readonly [string, DocumentedSqlType, number])[];
function documented(table: ExportTable, key: string, kind: 'transactional' | 'reference', layout: Layout,
  keyRole: 'primary_key' | 'reference_identifier', excluded: Layout = []) {
  const fields = (items: Layout) => Object.freeze(items.map(([name, sqlType, page]): DocumentedField =>
    Object.freeze({ name, sqlType, page, nullable: null, serverVerification: null })));
  return Object.freeze({ table, version: `${table}/v2`, evidence: manualEvidence, kind,
    documentedKey: key, documentedKeyRole: keyRole, fields: fields(layout), excludedFields: fields(excluded),
    documentedLayoutComplete: true, approved: false, currentLayoutConfirmed: false,
    pending: Object.freeze(['current resultset metadata and nullability', 'key scope across daily partitions and source instances',
      'privacy allowlist approval', 'exact acquisition before destructive parsing', 'operational semantics and recovery validation']) });
}
// Transcrição dos layouts, sem copiar descrições/SQL de exemplo nem inferir defaults ou constraints físicas atuais.
export const documentedAttempts = documented('AttemptsRawData', 'AttemptsRawDataID', 'transactional', [
  ['CallId','int',16], ['AgentId','int',16], ['CampaignId','int',16], ['DispositionId','int',16],
  ['StartDate','datetime',16], ['EndWrap','datetime',16], ['TableName','varchar(255)',16], ['EndCall','datetime',16],
  ['OriginalPhoneNumber','varchar(16)',16], ['Dnis','varchar(255)',16], ['PhoneTypeId','int',16], ['Route','varchar(256)',16],
  ['CampaignData','varchar(max)',16], ['CustomerId','varchar(255)',16], ['MailingRecordId','int',16], ['MailingPhoneNumberId','int',16],
  ['CallIdTelecom','varchar(16)',17], ['AttemptsRawDataID','int',17],
], 'primary_key');
export const documentedInbound = documented('InboundRawData', 'InboundRawDataID', 'transactional', [
  ['CallId','int',23], ['CampaignId','int',23], ['AgentId','int',23], ['DispositionId','int',23],
  ['CallStart','datetime',23], ['AgentStart','datetime',23], ['CallEnd','datetime',23], ['WrapEnd','datetime',23],
  ['QueueStart','datetime',23], ['QueueEnd','datetime',23], ['Ani','varchar(255)',23], ['Dnis','varchar(20)',23],
  ['CampaignData','varchar(max)',23], ['CustomerId','varchar(255)',23], ['InboundDispositionId','int',23],
  ['CallIdTelecom','varchar(16)',23], ['InboundRawDataID','int',23],
], 'primary_key');
export const documentedPbxBilling = documented('PbxBillingData', 'PbxBillingDataId', 'transactional', [
  ['PbxBillingDataId','int',26], ['StartDate','datetime',26], ['TelecomCallid','char(16)',26],
  ['CallDirectionId','int',27], ['DeviceName','varchar(70)',27], ['Route','int',27], ['Ani','varchar(16)',27], ['Dnis','varchar(20)',27],
  ['SignalingExtraInfo','int',27], ['TotalTime','int',27], ['SignalingTime','int',27], ['AlertingTime','int',27], ['AnalysisTime','int',27],
  ['ConversationTime','int',27], ['PbxDispositionId','int',27], ['CampaignId','int',27], ['MailingName','varchar(100)',27],
  ['AgentId','int',27], ['Company','int',27], ['CampaignCode','varchar(100)',27], ['BillingCode','varchar(200)',27], ['SessionId','int',27],
  ['TrunkNumber','varchar(20)',27], ['OriginalPhoneNumber','varchar(20)',27], ['PbxName','varchar(20)',27], ['Board','varchar(10)',27], ['Link','varchar(2)',27],
  ['Channel','varchar(5)',28], ['Signaling','varchar(10)',28], ['CustomerId','varchar(100)',28], ['RecordId','int',28], ['PhoneNumberId','int',28],
], 'primary_key');
export const documentedUsers = documented('Users', 'UserId', 'reference', [
  ['UserId','int',30], ['AgentId','int',30], ['Flag_Agent','int',30], ['Flag_Supervisor','int',30], ['Flag_Manager','int',30], ['Activated','int',30],
  ['LastLogin','datetime',30], ['Email','varchar(max)',30], ['Login','nvarchar(100)',30], ['UserName','nvarchar(510)',30], ['EnablePersonalTransfer','int',30],
], 'primary_key', [['Password','nvarchar(100)',30]]);
export const documentedCampaign = documented('Campaign', 'CampaignId', 'reference', [
  ['CampaignId','int',17], ['CampaignTypeId','int',17], ['Description','nvarchar(510)',17], ['CampaignCode','nvarchar(100)',17],
  ['BillingCode','nvarchar(100)',17], ['DispositionPlanId','int',17], ['Activated','int',17],
], 'reference_identifier');
export const documentedDisposition = documented('Disposition', 'DispositionId', 'reference', [
  ['DispositionId','int',20], ['Description','nvarchar(200)',20],
], 'reference_identifier');
export const documentedTableContracts = Object.freeze({ AttemptsRawData: documentedAttempts, InboundRawData: documentedInbound,
  PbxBillingData: documentedPbxBilling, Users: documentedUsers, Campaign: documentedCampaign, Disposition: documentedDisposition });
