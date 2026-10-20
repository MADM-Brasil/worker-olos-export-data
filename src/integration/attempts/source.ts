import { z } from 'zod';
import { IsolatedTdsSource, type TdsEvent } from '../../olos/tds/source.js';
import type { LargePlan, RawEvent } from '../../large-batch/types.js';
import type { TdsField } from '../../olos/tds/raw-parser.js';
import type { PreparedCall, ProcedureSignature } from '../../olos/types.js';
import { OlosError } from '../../olos/types.js';
import { canonical } from '../../persistence/validation.js';

export const fixtureEndpointSchema = z.object({ server: z.literal('127.0.0.1'),database: z.literal('phase3d_fixture'),
  port: z.number().int().min(1024).max(65535),user: z.literal('sa'),password: z.string().min(1),ca: z.string().includes('BEGIN CERTIFICATE') }).strict();
/** Single approved RPC, with strict TLS. Copying the endpoint deliberately discards any Disposition capability. */
export class AttemptsFixtureSource extends IsolatedTdsSource {
  private attempted = false;
  private readonly expectedFields: LargePlan['fields'];
  constructor(endpoint: unknown, fields: readonly TdsField[], private readonly callContract: ProcedureSignature,
    private readonly parameters: PreparedCall['values'], private readonly expectedRows?: number) {
    super(fixtureEndpointSchema.parse(endpoint),fields);
    if (!['ReadDataCurrentDay','ReadDataPreviousDays','LogReadData','ReReadData'].includes(callContract.procedure) ||
      (callContract.procedure === 'LogReadData' ? parameters.tablename : parameters.TableName) !== 'AttemptsRawData') throw new OlosError('PROCEDURE_BLOCKED');
    if (callContract.procedure === 'ReReadData' && (!Number.isSafeInteger(expectedRows) || expectedRows! < 1 || expectedRows! > 200000)) throw new OlosError('CONTRACT_UNCONFIRMED');
    this.expectedFields = fields.filter(f => f.type !== 'date').map(({ sqlLength: _,collationHex: _c,unicodeMetadataPolicy: _u,...f }) => {
      if (f.type === 'date') throw new OlosError('CONTRACT_UNCONFIRMED'); return { ...f,type: f.type };
    });
    this.callContract = structuredClone(callContract); this.parameters = structuredClone(parameters);
  }
  override async *call(s: ProcedureSignature, p: PreparedCall['values'], signal: AbortSignal, timeoutMs = 300000): AsyncGenerator<TdsEvent> {
    if (this.attempted || canonical(s) !== canonical(this.callContract) || canonical(p) !== canonical(this.parameters)) throw new OlosError('REAL_EXECUTION_BLOCKED');
    this.attempted = true;
    for await (const event of super.call(s,p,signal,timeoutMs)) {
      if (event.kind === 'complete' && (event.returnValue !== 0 || event.message !== null || event.errorMessage !== null)) throw new OlosError('OUTPUT_ERROR','unknown');
      yield event;
    }
  }
  override async *read(plan: LargePlan, signal: AbortSignal): AsyncGenerator<RawEvent> {
    if (plan.run.table !== 'AttemptsRawData' || canonical(plan.fields) !== canonical(this.expectedFields) || canonical(plan.signature) !== canonical(this.callContract) || canonical(plan.parameters) !== canonical(this.parameters)) throw new OlosError('REAL_EXECUTION_BLOCKED');
    let count = 0, min: bigint | undefined, max: bigint | undefined;
    const ids = new Set<string>(), index = plan.fields.findIndex(f => f.name === 'AttemptsRawDataID');
    for await (const event of this.call(plan.signature,plan.parameters,signal)) {
      if (event.kind === 'row') {
        const body = event.values[index];
        if (!body || body.byteLength !== 4) throw new OlosError('INVALID_RESPONSE','unknown');
        const id = BigInt(Buffer.from(body).readInt32LE());
        if (ids.has(id.toString()) || ++count > 200000) throw new OlosError('INVALID_RESPONSE','unknown');
        ids.add(id.toString()); min = min === undefined || id < min ? id : min; max = max === undefined || id > max ? id : max;
        if (this.callContract.procedure === 'ReReadData' && (id < BigInt(this.parameters.PKIDStart as string) || id > BigInt(this.parameters.PKIDEnd as string))) throw new OlosError('INVALID_RESPONSE','unknown');
      } else if (this.expectedRows !== undefined && (count !== this.expectedRows || min?.toString() !== this.parameters.PKIDStart || max?.toString() !== this.parameters.PKIDEnd)) throw new OlosError('INVALID_RESPONSE','unknown');
      yield event;
    }
  }
}
