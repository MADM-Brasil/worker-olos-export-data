import {z} from 'zod';
import {canonical,digest,validDate} from '../../persistence/validation.js';
import {validateLargePlan} from '../../large-batch/wire.js';
import type {LargePlan} from '../../large-batch/types.js';
import type {TdsField} from '../../olos/tds/raw-parser.js';
import {profileSchema} from '../../offline/model.js';
import {APPROVED_COLUMNS} from '../../offline/tds-writer.js';
export const acquisitionProcedures=['ReadDataCurrentDay','ReadDataPreviousDays'] as const;
export type AcquisitionProcedure=typeof acquisitionProcedures[number];
const text=z.string().trim().min(1).max(1000),sha=z.string().regex(/^[a-f0-9]{64}$/);
export const serviceConfigSchema=z.object({version:z.literal('olos-service/v2'),workerId:text,endpoint:z.unknown(),
  postgres:z.object({host:z.string().min(1),port:z.number().int(),database:z.literal('olos_worker')}).strict(),
  pinApprovalFile:z.string().startsWith('/'),approvalFile:z.string().startsWith('/'),profile:profileSchema,
  agentFile:z.string().startsWith('/').optional(),campaignFile:z.string().startsWith('/'),dispositionFile:z.string().startsWith('/')}).strict();
export type ServiceConfig=z.infer<typeof serviceConfigSchema>;
export const serviceApprovalSchema=z.object({scope:z.literal('OLOS_ATTEMPTS_SERVICE'),approvalId:text,workerId:text,
  configurationDigest:sha,procedures:z.tuple([z.literal('ReadDataCurrentDay'),z.literal('ReadDataPreviousDays')]),
  table:z.literal('AttemptsRawData'),destination:z.literal('olos_worker.raw.olos_calls'),operator:text,reference:text,
  issuedAt:z.string().datetime(),reviewPolicy:z.literal('REVOKE_BY_PRIVATE_FILE'),revocationFile:z.string().startsWith('/'),
  bootstrapRecoveryEvidence:text}).strict();
export type ServiceApproval=z.infer<typeof serviceApprovalSchema>;
export function validateServiceApproval(input:unknown,config:ServiceConfig,configurationDigest:string,revoked:boolean){
 const a=serviceApprovalSchema.parse(input);
 if(revoked || Date.parse(a.issuedAt)>Date.now() || a.workerId!==config.workerId || a.configurationDigest!==configurationDigest)throw Error('SERVICE_AUTHORIZATION_REJECTED');
 if(config.profile.phoneMatchKey!=='normalized' || config.profile.tmaPolicy!=='disabled' || canonical(config.profile.columns)!==canonical(APPROVED_COLUMNS))throw Error('DESTINATION_MAPPING_NOT_APPROVED');
 return a;
}
export const contractReviewSchema=z.object({version:z.literal('olos-contract-review/v1'),procedure:z.enum(acquisitionProcedures),
  captureId:z.string().uuid(),captureSha256:sha,configurationDigest:sha,operator:text,reference:text,reviewedAt:z.string().datetime(),
  sourceTimeZone:text,controlDayOffset:z.number().int().min(-366).max(0),partitionSemanticsEvidence:text,
  plan:z.custom<LargePlan>(v=>!!v && typeof v==='object'),fields:z.custom<readonly TdsField[]>(Array.isArray)}).strict();
export type ContractReview=z.infer<typeof contractReviewSchema>;
export function validateReviewedContract(review:ContractReview,observed:unknown,procedure:AcquisitionProcedure){
 const p=review.plan;validateLargePlan(p);
 const visible=review.fields.map(({sqlLength:_s,collationHex:_c,unicodeMetadataPolicy:_u,...f})=>f);
 if(review.procedure!==procedure || p.signature.procedure!==procedure || p.run.readType!==(procedure==='ReadDataCurrentDay'?'current_day':'previous_days') ||
  p.run.table!=='AttemptsRawData' || p.run.pkRange!==null || canonical(p.parameters)!==canonical({TableName:'AttemptsRawData'}) ||
  ['AttemptsRawDataID','CallId','StartDate','OriginalPhoneNumber','AgentId','CampaignId','DispositionId','CampaignData'].some(name=>!review.fields.some(f=>f.name===name)) ||
  review.fields.find(f=>f.name==='StartDate')?.type!=='datetime' || canonical(p.successReturnValues)!=='[0]' || canonical(p.contract.key_fields)!=='["AttemptsRawDataID"]' ||
  !['int','bigint'].includes(review.fields.find(f=>f.name==='AttemptsRawDataID')?.type??'') || review.fields.find(f=>f.name==='AttemptsRawDataID')?.nullable!==false ||
  canonical(visible)!==canonical(p.fields) || canonical(review.fields)!==canonical(observed) || /fixture|synthetic|pending/i.test(p.contract.version) ||
  Date.parse(review.reviewedAt)>Date.now())throw Error('OBSERVED_CONTRACT_NOT_APPROVED');
 new Intl.DateTimeFormat('en-CA',{timeZone:review.sourceTimeZone}).format();
}
/** An explicitly reviewed control bucket; never inferred from StartDate or claimed as a remote date parameter. */
export function planForCapture(review:ContractReview,at=new Date()):LargePlan{
 const date=new Intl.DateTimeFormat('en-CA',{timeZone:review.sourceTimeZone,year:'numeric',month:'2-digit',day:'2-digit'}).format(at);
 const d=new Date(`${date}T00:00:00Z`);d.setUTCDate(d.getUTCDate()+review.controlDayOffset);
 const exportDate=d.toISOString().slice(0,10);if(!validDate(exportDate))throw Error('PARTITION_POLICY_REJECTED');
 return {...structuredClone(review.plan),run:{...review.plan.run,exportDate}};
}
export const activationSchema=z.object({version:z.literal('olos-activation/v1'),workerId:text,configurationDigest:sha,
  approvalDigest:sha,contractDigests:z.record(z.string(),sha),captureIds:z.record(z.string(),z.string().uuid()),
  recoveryEvidence:text,operator:text,reference:text,issuedAt:z.string().datetime()}).strict();
export function validateActivation(input:unknown,config:ServiceConfig,configurationDigest:string,approval:ServiceApproval,reviews:readonly ContractReview[]){
 const a=activationSchema.parse(input);
 if(a.workerId!==config.workerId || a.configurationDigest!==configurationDigest || a.approvalDigest!==digest(approval) || Date.parse(a.issuedAt)>Date.now() ||
  acquisitionProcedures.some(proc=>{const r=reviews.find(x=>x.procedure===proc);return !r || a.contractDigests[proc]!==digest(r) || a.captureIds[proc]!==r.captureId;}))throw Error('ACTIVATION_REQUIREMENTS_MISSING');
 return a;
}
