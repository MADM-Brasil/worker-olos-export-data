import { IsolatedTdsSource, type IsolatedEndpoint, type TdsEvent } from '../olos/tds/source.js';
import { isDispositionEndpoint, type DispositionEndpoint, type DispositionApproval } from '../olos/tds/disposition-access.js';
import { reportedSignature } from '../olos/procedure-signatures.js';
import { OlosError, type ProcedureSignature, type PreparedCall } from '../olos/types.js';
import type { TdsField } from '../olos/tds/raw-parser.js';
import type { LargePlan, RawEvent, SpoolManifest, SpoolRepository, WholeBatchWriter } from '../large-batch/types.js';
import { canonical, prepareRecords, validDate } from '../persistence/validation.js';
import { defaultLimits } from '../persistence/types.js';

const registered = new WeakSet<object>();
export const isControlledDispositionSource = (source: object): boolean => registered.has(source);
export function dispositionPlan(approval: DispositionApproval): LargePlan {
  if (!validDate(approval.snapshotDate)) throw new OlosError('INVALID_PARAMETER');
  const fields = [{ name: 'DispositionId',type: 'int' as const,nullable: false },
    { name: 'Description',type: 'nvarchar' as const,nullable: false,maxBytes: 400 }];
  return { fields, contract: { source_table: 'Disposition',version: 'phase4-disposition-v1',approved: true,
    fields: { DispositionId: { type: 'int64',nullable: false },Description: { type: 'text',nullable: false } },key_fields: ['DispositionId'],
    layout_evidence: 'company:phase3e:resultset;phase4:isolated-destination-only',key_evidence: 'conditional unique IDs within this single snapshot; duplicates reject; no global PK claim' },
    run: { table: 'Disposition',contractVersion: 'phase4-disposition-v1',readType: 'reference_snapshot',exportDate: approval.snapshotDate,pkRange: null },
    signature: reportedSignature('ReadData',{ evidence: 'phase3e outputs null; phase4 isolated test',nullable: { TableName: false,Message: true,ErrorMessage: true } }),
    parameters: { TableName: 'Disposition' },successReturnValues: [0],acquisitionEvidence: 'phase3d exact binary parser; phase4 controlled single call',
    responseEvidence: 'phase3e 1 dataset, return 0, null outputs; row count variable',partitionEvidence: `reference snapshot label;approval:${approval.reference};test:${approval.testId}` };
}
export class ControlledDispositionSource extends IsolatedTdsSource {
  override readonly kind = 'controlled-disposition' as const;
  private used = false;
  private attempted = false;
  constructor(endpoint: DispositionEndpoint | IsolatedEndpoint, private readonly plan: LargePlan, collationHex?: string) {
    if (!isDispositionEndpoint(endpoint) && !(endpoint.server === '127.0.0.1' && endpoint.database === 'phase3d_fixture')) throw new OlosError('REAL_EXECUTION_BLOCKED');
    if (collationHex !== undefined && !/^[a-f0-9]{10}$/.test(collationHex)) throw new OlosError('CONTRACT_UNCONFIRMED');
    const fields: TdsField[] = plan.fields.map(f => ({ ...f,...(f.type === 'nvarchar' ? { sqlLength: 200,...(collationHex === undefined ? { unicodeMetadataPolicy: 'capture-code-units' as const } : { collationHex }) } : {}) }));
    super(endpoint,fields); registered.add(this);
  }
  override async *call(signature: ProcedureSignature, values: PreparedCall['values'], signal: AbortSignal, timeoutMs = 30000): AsyncGenerator<TdsEvent> {
    if (this.attempted || signature.procedure !== 'ReadData' || canonical(values) !== canonical({ TableName: 'Disposition' }) || timeoutMs > 30000) throw new OlosError('REAL_EXECUTION_BLOCKED');
    this.attempted = true; yield* super.call(signature,values,signal,timeoutMs);
  }
  override async *read(plan: LargePlan, signal: AbortSignal): AsyncGenerator<RawEvent> {
    if (this.used || canonical(plan) !== canonical(this.plan) || plan.run.table !== 'Disposition' || plan.signature.procedure !== 'ReadData') throw new OlosError('REAL_EXECUTION_BLOCKED');
    this.used = true; const ids = new Set<string>();
    for await (const event of this.call(plan.signature,plan.parameters,signal,30000)) {
      if (event.kind === 'row') {
        const id = event.values[0]; if (!id || id.byteLength !== 4) throw new OlosError('INVALID_RESPONSE','unknown');
        const key = Buffer.from(id).readInt32LE().toString();
        if (ids.has(key) || ids.size >= 10000) throw new OlosError('INVALID_RESPONSE','unknown'); ids.add(key);
      } else if (event.returnValue !== 0 || event.message !== null || event.errorMessage !== null) throw new OlosError('OUTPUT_ERROR','unknown');
      yield event;
    }
  }
}
/** Replay valida o arquivo inteiro, incluindo hash via repositório; nunca recebe uma porta SQL Server. */
export async function replayDisposition(manifest: SpoolManifest, spool: SpoolRepository, target: WholeBatchWriter, signal: AbortSignal) {
  if (!manifest.sealed || manifest.identity.plan.run.table !== 'Disposition' || manifest.identity.plan.signature.procedure !== 'ReadData') throw new OlosError('INVALID_RESPONSE');
  const ids = new Set<string>(); let count = 0;
  for await (const row of spool.records(manifest)) {
    prepareRecords([row],manifest.identity.plan.contract,defaultLimits);
    if (typeof row.DispositionId !== 'string' || ids.has(row.DispositionId) || ++count > 10000) throw new OlosError('INVALID_RESPONSE');
    ids.add(row.DispositionId);
  }
  if (count !== manifest.rows) throw new OlosError('INVALID_RESPONSE');
  return target.persist(manifest,{ runId: manifest.identity.runId,streamId: manifest.identity.streamId,fencingToken: manifest.identity.fencingToken,signal },() => spool.records(manifest));
}
