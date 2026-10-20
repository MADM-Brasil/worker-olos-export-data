import { z } from 'zod';
import { randomUUID } from 'node:crypto';
import type { LargePlan,RawSource } from '../../large-batch/types.js';
import { validateLargePlan } from '../../large-batch/wire.js';
import type { TdsField } from '../../olos/tds/raw-parser.js';
import { IsolatedTdsSource } from '../../olos/tds/source.js';
import { prepareCorporateAttemptsEndpoint, authorizeCorporateAttemptsOnce } from '../../olos/tds/attempts-access.js';
import { assertOlosPinApproval,type OlosPinApproval } from '../../security/olos-certificate-pin.js';
import { canonical,digest } from '../../persistence/validation.js';
import { profileSchema,type Catalogs } from '../../offline/model.js';
import { APPROVED_COLUMNS } from '../../offline/tds-writer.js';
import type { AttemptsBinding } from './pipeline.js';
const definition=z.object({plan:z.custom<LargePlan>(v=>!!v && typeof v==='object'),fields:z.custom<readonly TdsField[]>(Array.isArray)}).strict();
export const corporateConfigSchema=z.object({
  version:z.literal('olos-corporate-local/v1'),endpoint:z.unknown(),pinApprovalFile:z.string().startsWith('/'),
  current:definition,previous:definition.optional(),profile:profileSchema,
  campaignFile:z.string().startsWith('/'),dispositionFile:z.string().startsWith('/'),
  authorization:z.object({operator:z.string().min(1).max(100),reference:z.string().min(1).max(200),
    issuedAt:z.string().datetime(),expiresAt:z.string().datetime(),sourceTimeZone:z.string().min(1).max(100),
    partitionPolicy:z.literal('ACQUISITION_DAY_CONTROL_BUCKET').default('ACQUISITION_DAY_CONTROL_BUCKET'),
    layoutEvidence:z.string().min(1).max(1000).optional(),sharedConsumptionEvidence:z.string().min(1).max(1000),
    recoveryEvidence:z.string().min(1).max(1000),
    failurePolicy:z.literal('STOP_PRESERVE_CASE_RECONCILE_LOG_REREAD_NO_RESET').default('STOP_PRESERVE_CASE_RECONCILE_LOG_REREAD_NO_RESET')}).strict(),
}).strict();
export type CorporateConfig=z.infer<typeof corporateConfigSchema>;
/** First response acquisition may observe a layout; it cannot approve it or arm recurrence. */
export const corporateObservationSchema=corporateConfigSchema.omit({current:true,previous:true}).extend({
  contractMode:z.literal('observe'),
}).strict();
export type CorporateObservationConfig=z.infer<typeof corporateObservationSchema>;
export function validateCorporateObservation(config:CorporateObservationConfig,pin:OlosPinApproval):void {
  assertOlosPinApproval(pin);
  const a=config.authorization,now=Date.now();
  if(Date.parse(a.issuedAt)>now || Date.parse(a.expiresAt)<=now || Date.parse(a.expiresAt)<=Date.parse(a.issuedAt) || Date.parse(a.expiresAt)-Date.parse(a.issuedAt)>12*3600000)throw Error('CORPORATE_APPROVAL_EXPIRED');
  sourceDay(a.sourceTimeZone);
  if(config.profile.tmaPolicy!=='disabled' || config.profile.phoneMatchKey!=='normalized' || canonical(config.profile.columns)!==canonical(APPROVED_COLUMNS))throw Error('DESTINATION_MAPPING_NOT_APPROVED');
  if(pin.purpose!=='OLOS_ATTEMPTS_TDS_ACQUISITION')throw Error('PIN_APPROVAL_INVALID');
}
const bindings=new WeakMap<object,()=>void>();
export function assertCorporateBinding(binding:object):void {const check=bindings.get(binding);if(!check)throw Error('ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED');check();}
function sourceDay(zone:string){return new Intl.DateTimeFormat('en-CA',{timeZone:zone,year:'numeric',month:'2-digit',day:'2-digit'}).format(new Date());}
export function validateCorporateConfiguration(config:CorporateConfig,pin:OlosPinApproval):void {
  assertOlosPinApproval(pin);
  const a=config.authorization,now=Date.now();
  if(Date.parse(a.issuedAt)>now || Date.parse(a.expiresAt)<=now || Date.parse(a.expiresAt)<=Date.parse(a.issuedAt) || Date.parse(a.expiresAt)-Date.parse(a.issuedAt)>12*3600000)throw Error('CORPORATE_APPROVAL_EXPIRED');
  if(config.profile.tmaPolicy!=='disabled' || config.profile.phoneMatchKey!=='normalized' || canonical(config.profile.columns)!==canonical(APPROVED_COLUMNS))throw Error('DESTINATION_MAPPING_NOT_APPROVED');
  for(const [d,proc,type] of [[config.current,'ReadDataCurrentDay','current_day'],[config.previous,'ReadDataPreviousDays','previous_days']] as const){
    if(!d)continue; // CurrentDay once does not depend on a fictitious PreviousDays contract.
    validateLargePlan(d.plan);
    const visible=d.fields.map(({sqlLength:_,collationHex:_c,unicodeMetadataPolicy:_u,...f})=>f);
    if(canonical(visible)!==canonical(d.plan.fields) || d.plan.run.table!=='AttemptsRawData' || d.plan.run.readType!==type || d.plan.signature.procedure!==proc ||
      canonical(d.plan.parameters)!==canonical({TableName:'AttemptsRawData'}) || canonical(d.plan.successReturnValues)!=='[0]' ||
      d.plan.run.exportDate!==sourceDay(a.sourceTimeZone) || canonical(d.plan.contract.key_fields)!=='["AttemptsRawDataID"]' ||
      /fixture|synthetic|pending/i.test(d.plan.contract.version) || a.layoutEvidence!==undefined && d.plan.contract.layout_evidence!==a.layoutEvidence ||
      d.plan.run.pkRange!==null)throw Error('CORPORATE_CONTRACT_OR_SOURCE_DAY_UNCONFIRMED');
    prepareCorporateAttemptsEndpoint(config.endpoint,pin,d.plan.signature,d.plan.parameters);
  }
}
export interface CorporateCycleState {next:'current'|'previous';pending:string|null;}
export interface CorporateAttemptIntent {
  attemptId:string;configurationDigest:string;procedure:string;controlDate:string;operator:string;approvalReference:string;
  pinApprovalId:string;createdAt:string;next:'current'|'previous';
}
/** A completed CurrentDay receipt survives authorization renewal or addition of a separately validated PreviousDays layout. */
export function corporateAcquisitionScope(config:CorporateConfig,catalogs:Catalogs):string {
  return digest({source:'10.196.234.11:1433/ExportDataV3/AttemptsRawData',contract:config.current.plan.contract,fields:config.current.fields,
    profile:config.profile,campaigns:[...catalogs.campaigns],dispositions:[...catalogs.dispositions],destination:'local.olos_worker.raw.olos_calls'});
}
/** A single selected procedure per cycle. Source is created only after a durable exclusive intent. */
export function createCorporateAttemptsBinding(input:unknown,pin:OlosPinApproval,catalogs:Catalogs,state:CorporateCycleState,
  saveIntent:(intent:CorporateAttemptIntent)=>Promise<{intentPath:string;pendingPath:string}>,saveConfirmed:(intent:CorporateAttemptIntent,result:unknown)=>Promise<void>):AttemptsBinding {
  const config=structuredClone(corporateConfigSchema.parse(input));
  const freeze=(v:unknown):void=>{if(v && typeof v==='object'){for(const child of Object.values(v))freeze(child);Object.freeze(v);}};freeze(config);
  validateCorporateConfiguration(config,pin);
  const configurationDigest=digest({config,pinApprovalId:pin.approvalId,fingerprint:pin.fingerprintSha256,
    campaigns:[...catalogs.campaigns],dispositions:[...catalogs.dispositions]});
  let source:IsolatedTdsSource|undefined,intent:CorporateAttemptIntent|undefined;
  const binding:AttemptsBinding={configurationDigest,acquisitionScopeDigest:corporateAcquisitionScope(config,catalogs),kind:'corporate-local',
    destinationId:'local.olos_worker.raw.olos_calls',mappingVersion:'local-approved-16/v1',sourceNamespace:'olos.ExportDataV3.AttemptsRawData',
    profile:config.profile,catalogs,
    plan:()=>{const d=config[state.next];if(!d)throw Error('PREVIOUS_CONTRACT_UNCONFIRMED');return structuredClone(d.plan);},
    source:()=>{if(!source)throw Error('DURABLE_INTENT_REQUIRED');return source;},
    async beforeAcquire(plan,signal){
      validateCorporateConfiguration(config,pin);
      if(signal.aborted || state.pending)throw Error('RECOVERY_REQUIRED');
      const selected=config[state.next];if(!selected)throw Error('PREVIOUS_CONTRACT_UNCONFIRMED');
      if(canonical(plan)!==canonical(selected.plan))throw Error('REQUEST_CHANGED');
      const ep=prepareCorporateAttemptsEndpoint(config.endpoint,pin,plan.signature,plan.parameters);
      const candidate:CorporateAttemptIntent={attemptId:randomUUID(),configurationDigest,procedure:plan.signature.procedure,controlDate:plan.run.exportDate,
        operator:config.authorization.operator,approvalReference:config.authorization.reference,pinApprovalId:pin.approvalId,createdAt:new Date().toISOString(),
        next:state.next==='current'?'previous':'current'};
      await authorizeCorporateAttemptsOnce(ep,()=>{if(signal.aborted)throw Error('ABORTED');validateCorporateConfiguration(config,pin);},async()=>{
        const paths=await saveIntent(candidate);state.pending=candidate.attemptId;intent=candidate;return paths;
      });
      source=new IsolatedTdsSource(ep,selected.fields);
    },
    async afterAcquire(result){if(!intent || !state.pending)throw Error('ATTEMPT_STATE_MISSING');await saveConfirmed(intent,result);state.next=intent.next;state.pending=null;intent=undefined;source=undefined;},
  };
  bindings.set(binding,()=>validateCorporateConfiguration(config,pin));return binding;
}
/** Approval is still checked against PostgreSQL by the ordinary pipeline; replay has no remote permit. */
export function createCorporateResponseReplayBinding(config:CorporateConfig,catalogs:Catalogs,source:RawSource,
  observedFields:unknown,controlDate:string):AttemptsBinding {
  const plan=structuredClone(config.current.plan);
  const check=()=>{
    validateLargePlan(plan);
    const visible=config.current.fields.map(({sqlLength:_,collationHex:_c,unicodeMetadataPolicy:_u,...f})=>f);
    if(canonical(config.current.fields)!==canonical(observedFields) || canonical(visible)!==canonical(plan.fields) ||
      plan.run.table!=='AttemptsRawData' || plan.run.exportDate!==controlDate || plan.run.readType!=='current_day' ||
      plan.signature.procedure!=='ReadDataCurrentDay' || canonical(plan.parameters)!==canonical({TableName:'AttemptsRawData'}) ||
      /fixture|synthetic|pending/i.test(plan.contract.version) || canonical(plan.contract.key_fields)!=='["AttemptsRawDataID"]')throw Error('CORPORATE_CONTRACT_OR_SOURCE_DAY_UNCONFIRMED');
  };
  check();
  const binding:AttemptsBinding={kind:'corporate-local',destinationId:'local.olos_worker.raw.olos_calls',mappingVersion:'local-approved-16/v1',
    sourceNamespace:'olos.ExportDataV3.AttemptsRawData',profile:config.profile,catalogs,plan:()=>structuredClone(plan),source:()=>source};
  bindings.set(binding,check);return binding;
}
