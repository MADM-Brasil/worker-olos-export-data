import {beforeAll,afterAll,beforeEach,afterEach,it,expect,vi} from 'vitest';
import {mkdtemp,mkdir,readFile,writeFile,rm} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {randomUUID} from 'node:crypto';
import pg from 'pg';
import {isolatedPostgres} from './support/postgres.js';
import {fields,response,collation} from './support/activation-capture.js';
import {LocalSpool} from '../adapters/local-spool.js';
import {spoolLimits} from '../src/large-batch/config.js';
import {attemptsFixturePlan,signature} from '../src/integration/attempts/contracts.js';
import {deliverServiceCapture,captureIntentSchema,loadServiceConfiguration,reviewServiceContract,promoteService,serviceCycle} from '../adapters/attempts-service.js';
import {serviceConfigSchema,type ContractReview} from '../src/integration/attempts/activation.js';
import {exampleProfile,columnTypes} from '../src/offline/model.js';
import {APPROVED_COLUMNS,IsolatedTdsCallsWriter} from '../src/offline/tds-writer.js';
import {digest} from '../src/persistence/validation.js';
import {IsolatedTdsSource} from '../src/olos/tds/source.js';
let fixture:Awaited<ReturnType<typeof isolatedPostgres>>,root:string,captureDate:string,sequence=0;
let pools:{ingest:pg.Pool;project:pg.Pool;receiver:pg.Pool};
beforeAll(async()=>{
 fixture=await isolatedPostgres();
 for(const f of ['migrations/003_projection_control.sql','permissions/projection.sql','migrations/005_large_batch_control.sql','permissions/large-batch.sql','migrations/004_delivery_receipts.sql','permissions/receiver.sql'])await fixture.admin.query(await readFile(new URL('../'+f,import.meta.url),'utf8'));
 await fixture.admin.query('CREATE SCHEMA raw');
 await fixture.admin.query(`CREATE TABLE raw.olos_calls(${APPROVED_COLUMNS.map(c=>`"${c}" ${columnTypes[c]} ${c==='internal_id_olos'?'PRIMARY KEY':''}`).join(',')})`);
 await fixture.admin.query('GRANT USAGE ON SCHEMA raw TO olos_export_receiver');await fixture.admin.query('GRANT SELECT,INSERT ON raw.olos_calls TO olos_export_receiver');
},120000);
afterAll(async()=>{await fixture?.stop();});
beforeEach(async()=>{
 const date=new Date('2026-10-22T00:00:00Z');date.setUTCDate(date.getUTCDate()+sequence++);captureDate=date.toISOString().slice(0,10);
 root=await mkdtemp(join(tmpdir(),'olos-service-delivery-'));
 pools={ingest:fixture.worker(),project:new pg.Pool({...fixture.connection,options:'-c role=olos_export_projector'}),receiver:new pg.Pool({...fixture.connection,options:'-c role=olos_export_receiver'})};
 vi.spyOn(IsolatedTdsSource.prototype,'call').mockImplementation(()=>{throw Error('REMOTE_RPC_FORBIDDEN_DURING_LOCAL_REPLAY');});
});
afterEach(async()=>{expect(IsolatedTdsSource.prototype.call).not.toHaveBeenCalled();vi.restoreAllMocks();await Promise.all(Object.values(pools).map(p=>p.end()));await rm(root,{recursive:true,force:true});});
async function prepared(version:string,procedure:'ReadDataCurrentDay'|'ReadDataPreviousDays'='ReadDataCurrentDay'){
 const config=serviceConfigSchema.parse({version:'olos-service/v2',workerId:'isolated-test',endpoint:{server:'10.196.234.11',port:1433,database:'ExportDataV3',user:'OEC',password:'SYNTHETIC'},postgres:{host:'127.0.0.1',port:5434,database:'olos_worker'},pinApprovalFile:'/unused',approvalFile:join(root,'approval.json'),campaignFile:join(root,'campaign.json'),dispositionFile:join(root,'disposition.json'),profile:{...exampleProfile,phoneMatchKey:'normalized',columns:[...APPROVED_COLUMNS]}});
 await writeFile(join(root,'config.json'),JSON.stringify(config),{mode:0o600});
 await writeFile(config.campaignFile,'[]',{mode:0o600});await writeFile(config.dispositionFile,'[]',{mode:0o600});
 const loaded=await loadServiceConfiguration(join(root,'config.json'));
 const template=attemptsFixturePlan(captureDate,collation.toBuffer().toString('hex'));
 const day=new Date(`${captureDate}T00:00:00Z`);if(procedure==='ReadDataPreviousDays')day.setUTCDate(day.getUTCDate()-1);const controlDate=day.toISOString().slice(0,10);
 const plan={...template,signature:signature(procedure),fields:fields.map(({sqlLength:_s,collationHex:_c,unicodeMetadataPolicy:_u,...f})=>f) as typeof template.fields,contract:{...template.contract,version},run:{...template.run,contractVersion:version,readType:procedure==='ReadDataCurrentDay'?'current_day' as const:'previous_days' as const,exportDate:controlDate}};
 await fixture.admin.query('INSERT INTO olos_export.source_contracts(source_table,version,approved,fields,key_fields,layout_evidence,key_evidence) VALUES($1,$2,true,$3::jsonb,$4,$5,$6) ON CONFLICT DO NOTHING',[plan.contract.source_table,version,JSON.stringify(plan.contract.fields),plan.contract.key_fields,plan.contract.layout_evidence,plan.contract.key_evidence]);
 const attemptId=randomUUID(),dir=join(root,'cases',attemptId);await mkdir(dir,{recursive:true,mode:0o700});
 const spool=await LocalSpool.open(join(dir,'capture'),spoolLimits()),capture=await spool.createResponseCapture(attemptId);await capture.append(response);const seal=await capture.seal({received:1,fields,responseValidated:true,transportSecurity:[{encrypted:true,identityValidated:true,protocol:'TLSv1.2',tlsMode:'certificate-pinned'}]});
 const review:ContractReview={version:'olos-contract-review/v1',procedure,captureId:attemptId,captureSha256:seal.sha256,configurationDigest:loaded.configurationDigest,operator:'test-reviewer',reference:'isolated-source-observation',reviewedAt:new Date().toISOString(),sourceTimeZone:'UTC',controlDayOffset:procedure==='ReadDataCurrentDay'?0:-1,partitionSemanticsEvidence:'isolated control bucket',plan,fields};
 const intent=captureIntentSchema.parse({attemptId,procedure,configurationDigest:loaded.configurationDigest,workDir:root,createdAt:`${captureDate}T10:00:00.000Z`,controlDate,operator:'test',approvalReference:'isolated',pinApprovalId:'unused-local',contractReviewDigest:digest(review),plan,state:'PREPARED'});
 await writeFile(join(root,`attempt-${attemptId}.json`),JSON.stringify(intent),{mode:0o600});return {intent,review,loaded};
}
it('PostgreSQL unavailable before import resumes the complete spool locally without a new RPC',async()=>{
 const p=await prepared('service-local-outage/v1'),failure=vi.spyOn(pools.ingest,'query').mockRejectedValueOnce(Error('local postgres down'));
 await expect(deliverServiceCapture(root,p.loaded,p.intent,p.review,pools,new AbortController().signal)).rejects.toThrow();failure.mockRestore();
 const result=await deliverServiceCapture(root,p.loaded,p.intent,p.review,pools,new AbortController().signal);expect(result.received).toBe(1);
 expect((await fixture.admin.query('SELECT c.version::text FROM olos_export.checkpoints c WHERE c.stream_id=(SELECT stream_id FROM olos_export.runs WHERE contract_version=$1 ORDER BY started_at DESC LIMIT 1)',[p.review.plan.contract.version])).rows[0].version).toBe('1');
},30000);
it('receiver COMMIT without ACK reconciles its durable receipt before checkpoint, preserving an existing PK',async()=>{
 const p=await prepared('service-local-ack-loss/v1');
 await fixture.admin.query("INSERT INTO raw.olos_calls(internal_id_olos,olos_call_id) VALUES(1,'preserve-existing') ON CONFLICT(internal_id_olos) DO UPDATE SET olos_call_id='preserve-existing'");
 const original=pools.receiver.connect.bind(pools.receiver);let lost=false;
 const spy=vi.spyOn(pools.receiver,'connect').mockImplementation((async()=>{
  const client=await original();return new Proxy(client,{get(target,key){if(key==='query')return async(sql:string,...args:unknown[])=>{
   const result=await (target.query as (...a:unknown[])=>Promise<unknown>).call(target,sql,...args);
   if(sql==='COMMIT' && !lost){lost=true;throw Error('simulated lost commit ACK');}return result;
  };const value=Reflect.get(target,key);return typeof value==='function'?value.bind(target):value;}});
 }) as typeof pools.receiver.connect);
 const writes=vi.spyOn(IsolatedTdsCallsWriter.prototype,'writeInTransaction');
 await expect(deliverServiceCapture(root,p.loaded,p.intent,p.review,pools,new AbortController().signal)).rejects.toThrow();spy.mockRestore();
 expect(lost).toBe(true);expect((await fixture.admin.query('SELECT c.version::text FROM olos_export.checkpoints c WHERE c.stream_id=(SELECT stream_id FROM olos_export.runs WHERE contract_version=$1 ORDER BY started_at DESC LIMIT 1)',[p.review.plan.contract.version])).rows[0].version).toBe('0');
 const before=writes.mock.calls.length;
 const result=await deliverServiceCapture(root,p.loaded,p.intent,p.review,pools,new AbortController().signal);expect(result.received).toBe(1);expect(writes.mock.calls.length).toBe(before);
 expect((await fixture.admin.query('SELECT olos_call_id FROM raw.olos_calls WHERE internal_id_olos=1')).rows[0].olos_call_id).toBe('preserve-existing');
 const second=await prepared('service-local-ack-loss/v1');const duplicate=await deliverServiceCapture(root,second.loaded,second.intent,second.review,pools,new AbortController().signal);
 expect(duplicate.duplicates).toBe(1);
},30000);

it('ACTIVE promotion requires both real local deliveries and restart replays a complete pending batch without RPC',async()=>{
 const current=await prepared('service-active-current/v1');await reviewServiceContract(root,current.loaded,current.review);
 await deliverServiceCapture(root,current.loaded,current.intent,current.review,pools,new AbortController().signal);
 const previous=await prepared('service-active-previous/v1','ReadDataPreviousDays');await reviewServiceContract(root,previous.loaded,previous.review);
 const grant={scope:'OLOS_ATTEMPTS_SERVICE',approvalId:'test-approval',workerId:current.loaded.config.workerId,configurationDigest:current.loaded.configurationDigest,
  procedures:['ReadDataCurrentDay','ReadDataPreviousDays'],table:'AttemptsRawData',destination:'olos_worker.raw.olos_calls',operator:'fixture-authority',reference:'isolated approval only',
  issuedAt:'2020-01-01T00:00:00.000Z',reviewPolicy:'REVOKE_BY_PRIVATE_FILE',revocationFile:join(root,'revoked.json'),bootstrapRecoveryEvidence:'isolated approved first-read exercise'} as const;
 await writeFile(current.loaded.config.approvalFile,JSON.stringify(grant),{mode:0o600});
 const recoveryFile=join(root,'recovery.json');await writeFile(recoveryFile,JSON.stringify({scope:'OLOS_CONSUMPTIVE_RECOVERY',configurationDigest:current.loaded.configurationDigest,
  captureIds:[current.intent.attemptId,previous.intent.attemptId],operator:'test',reference:'fixture-only',approvedAt:new Date().toISOString(),ownershipEvidence:'isolated ownership exercise',retentionCompletenessEvidence:'isolated fixture completeness',recoveryExerciseEvidence:'real local receipt exercise above; NOT Olos proof',externalRunbookReference:'synthetic fixture reference',failurePolicy:'STOP_PRESERVE_EXTERNAL_RECONCILIATION_NO_REMOTE_RETRY'}),{mode:0o600});
 const request={version:'olos-activation/v1',workerId:current.loaded.config.workerId,configurationDigest:current.loaded.configurationDigest,approvalDigest:digest(grant),
  contractDigests:{ReadDataCurrentDay:digest(current.review),ReadDataPreviousDays:digest(previous.review)},captureIds:{ReadDataCurrentDay:current.intent.attemptId,ReadDataPreviousDays:previous.intent.attemptId},recoveryEvidence:recoveryFile,operator:'fixture-authority',reference:'fixture-only',issuedAt:new Date().toISOString()};
 await expect(promoteService(root,current.loaded,grant as never,request)).rejects.toThrow();
 await deliverServiceCapture(root,previous.loaded,previous.intent,previous.review,pools,new AbortController().signal);
 await promoteService(root,current.loaded,grant as never,request);
 const pending=await prepared('service-active-current/v1');pending.intent.contractReviewDigest=digest(current.review);
 await writeFile(join(root,`attempt-${pending.intent.attemptId}.json`),JSON.stringify(pending.intent),{mode:0o600});
 const base=join(root,'isolated-control');await mkdir(base,{mode:0o700});await writeFile(join(base,'remote-pending.json'),JSON.stringify({attemptId:pending.intent.attemptId,workDir:root,configurationDigest:pending.loaded.configurationDigest}),{mode:0o600});
 await writeFile(join(base,'log-inspection-active.json'),JSON.stringify({attemptId:'193c7869-6ba0-480f-a7c8-3fcd6e178394',workDir:join(base,'log-inspections','193c7869-6ba0-480f-a7c8-3fcd6e178394')}),{mode:0o600});
 const result=await serviceCycle({base,root,configFile:join(root,'config.json'),intervalMs:120000},pools,new AbortController().signal);
 expect(result.received).toBe(1);expect(result.acquisitionPerformed).toBe(false);
 expect(JSON.parse(await readFile(join(root,'cases',pending.intent.attemptId,'remote-pending.completed.json'),'utf8')).attemptId).toBe(pending.intent.attemptId);
},30000);
