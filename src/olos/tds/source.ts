import {reportInspectionFailure,type InspectionDiagnosticObserver} from './inspection-diagnostics.js';
import { canonical } from '../../persistence/validation.js';
import { isLogInspectionEndpoint, isPreparedAttemptsEndpoint, assertPreparedAttemptsCall, assertCorporateAttemptsExecution, type PreparedAttemptsEndpoint } from './attempts-access.js';
import { isDispositionEndpoint, assertDispositionCall, type DispositionEndpoint } from './disposition-access.js';
import { isExceptionalDispositionEndpoint } from './disposition-exception.js';
import { TYPES, Request } from 'tedious';
import type { ColumnMetadata } from 'tedious/lib/token/colmetadata-token-parser.js';
import { prepareCall } from '../client.js';
import { procedures } from '../contracts.js';
import { compareSignatures, verifiedProcedureContracts } from '../procedure-signatures.js';
import { utcDriverDate } from '../parameter-codecs.js';
import { OlosError, type PreparedCall, type ProcedureSignature, type ParameterType } from '../types.js';
import type { LargePlan, RawEvent } from '../../large-batch/types.js';
import { ExactConnection } from './connection.js';
import { parserLimits, validateParserLimits, observeMetadata, type RawParserLimits, type TdsField } from './raw-parser.js';
import { ExactTokenParser } from './raw-parser.js';
import { createRequire } from 'node:module';
import type { RawSource } from '../../large-batch/types.js';

export interface IsolatedEndpoint {
  readonly server: '127.0.0.1'; readonly database: 'phase3d_fixture'; readonly port: number;
  readonly user: string; readonly password: string; readonly ca: string;
}
export type TdsEvent = { readonly kind: 'row'; readonly values: readonly (Uint8Array | null)[] } |
  { readonly kind: 'complete'; readonly datasetCount: number; readonly returnValue: number; readonly errorMessage: string | null;
    readonly message: string | null; readonly output: Readonly<Record<string,string | null>> };
const registered = new WeakSet<object>();
export const isIsolatedTdsSource = (value: object): boolean => registered.has(value);
export function assertRegisteredTdsExecution(value: object): void {
  if (registered.has(value)) (value as IsolatedTdsSource).assertExecutionAllowed();
}
function sqlType(type: ParameterType) {
  switch (type.kind) {
    case 'varchar': return TYPES.VarChar;
    case 'nvarchar': return TYPES.NVarChar;
    case 'bigint': return TYPES.BigInt;
    case 'date': return TYPES.Date;
    case 'int': return TYPES.Int;
    case 'bit': return TYPES.Bit;
  }
}
/** RPC allowlisted e tipado. Sem conexão e sem query SQL livre. */
export function prepareTdsRequest(request: Request, signature: ProcedureSignature, values: PreparedCall['values'], timeoutMs: number): PreparedCall {
  const call = prepareCall(signature,values,timeoutMs);
  if (compareSignatures(verifiedProcedureContracts[call.procedure].server.parameters,call.signature.parameters.map(p => ({ ...p, nullable: null }))).length) throw new OlosError('CONTRACT_UNCONFIRMED');
  for (const p of call.signature.parameters) {
    const opts = 'length' in p.type ? { length: p.type.length === 'max' ? Infinity : p.type.length } : undefined;
    if (p.direction === 'output') request.addOutputParameter(p.name,sqlType(p.type),undefined,opts);
    else {
      const v = call.values[p.name];
      const value = v === null ? null : p.type.kind === 'bigint' ? BigInt(v as string) : p.type.kind === 'int' ? Number(v) : p.type.kind === 'date' ? utcDriverDate(v as string) : v;
      request.addParameter(p.name,sqlType(p.type),value,opts);
    }
  }
  return call;
}
/** Sessão TDS de ensaio ou capacidade explícita Disposition. Endpoints arbitrários permanecem bloqueados. */
export class IsolatedTdsSource {
  readonly kind: 'isolated-sqlserver' | 'controlled-disposition' | 'mssql';
  readonly metrics = { rows: 0, pauses: 0, resumes: 0, peakQueuedRows: 0, calls: 0, infoMessages: 0 };
  readonly observedMetadata: { name: string; type: string; nullable: boolean; dataLength: number | null; collationHex: string | null }[] = [];
  readonly responseObservation: {returnValue:number|null;datasetCount:number;finalDone:boolean;sqlError:boolean;errorMessagePresent:boolean|null}={returnValue:null,datasetCount:0,finalDone:false,sqlError:false,errorMessagePresent:null};
  readonly observedDatasets: { name: string; type: string; nullable: boolean; dataLength: number | null; collationHex: string | null }[][] = [];
  readonly observedFields: TdsField[] = [];
  readonly transportSecurity: { encrypted: true; identityValidated: boolean; protocol: string; tlsMode: 'verified' | 'exceptional-unverified-test' | 'certificate-pinned' }[] = [];
  private busy = false;
  private readonly endpoint: IsolatedEndpoint | DispositionEndpoint | PreparedAttemptsEndpoint;
  private readonly fields: readonly TdsField[];
  private readonly limits: RawParserLimits;
  constructor(endpoint: IsolatedEndpoint | DispositionEndpoint | PreparedAttemptsEndpoint, fields: readonly TdsField[], limits: RawParserLimits = parserLimits,
    private readonly capture?: (chunk: Buffer) => Promise<void>,
    private readonly inspectionReplay?: () => AsyncIterable<Buffer>,
    private readonly inspectionDiagnostics?: InspectionDiagnosticObserver) {
    if(inspectionReplay && isPreparedAttemptsEndpoint(endpoint) && !isLogInspectionEndpoint(endpoint))throw new OlosError('REAL_EXECUTION_BLOCKED');
    if (!isPreparedAttemptsEndpoint(endpoint) && !isDispositionEndpoint(endpoint) && (endpoint.server !== '127.0.0.1' || endpoint.database !== 'phase3d_fixture' || !Number.isInteger(endpoint.port) || endpoint.port < 1024 || endpoint.port > 65535 ||
      !endpoint.user || !endpoint.password || !endpoint.ca.includes('BEGIN CERTIFICATE') || !fields.length && !capture || fields.length > 512)) throw new OlosError('REAL_EXECUTION_BLOCKED');
    if (!fields.length && (!capture || isDispositionEndpoint(endpoint))) throw new OlosError('REAL_EXECUTION_BLOCKED');
    this.kind = isPreparedAttemptsEndpoint(endpoint) ? 'mssql' : 'isolated-sqlserver';
    validateParserLimits(limits);
    this.endpoint = isPreparedAttemptsEndpoint(endpoint) || isDispositionEndpoint(endpoint) ? endpoint : Object.freeze({ ...endpoint }); this.fields = structuredClone(fields); this.limits = Object.freeze({ ...limits }); registered.add(this);
  }
  assertExecutionAllowed(): void {
    if (isPreparedAttemptsEndpoint(this.endpoint)) assertCorporateAttemptsExecution(this.endpoint);
  }
  async *read(plan: LargePlan, signal: AbortSignal): AsyncGenerator<RawEvent> {
    const visible = this.fields.map(({ sqlLength: _s, collationHex: _c, unicodeMetadataPolicy: _u, ...f }) => f);
    if (canonical(visible) !== canonical(plan.fields) || !['ReadData','ReadDataCurrentDay','ReadDataPreviousDays'].includes(plan.signature.procedure) ||
      plan.parameters.TableName !== plan.run.table) throw new OlosError('CONTRACT_UNCONFIRMED');
    yield* this.call(plan.signature,plan.parameters,signal);
  }
  async *call(signature: ProcedureSignature, values: PreparedCall['values'], signal: AbortSignal, timeoutMs = 300_000): AsyncGenerator<TdsEvent> {
    if(this.inspectionReplay && signature.procedure!=='LogReadData')throw new OlosError('PROCEDURE_BLOCKED');
    if (this.busy) throw new OlosError('BUSY');
    if (signal.aborted) throw new OlosError('ABORTED');
    let request!: Request, dispatched = false, timedOut = false, finished = false, finalDone = false;
    let failure: OlosError | undefined, slot: readonly (Uint8Array | null)[] | undefined, wake: (() => void) | undefined;
    let datasets = 0, returnValue: number | undefined, lastStatus: number | undefined;
    const output: Record<string,string | null> = {}, snapshot = prepareCall(signature,values,timeoutMs);
    const e = this.endpoint;
    if (isPreparedAttemptsEndpoint(e)) assertPreparedAttemptsCall(e,snapshot);
    if (isDispositionEndpoint(e)) assertDispositionCall(e,snapshot.procedure,snapshot.values);
    this.inspectionDiagnostics?.({kind:'stage',stage:'CONNECTION_INIT_STARTED'});
    const connection = new ExactConnection({ server: e.server, authentication: { type: 'default', options: { userName: e.user, password: e.password } },
      options: { database: e.database, port: e.port, serverName: 'serverName' in e ? e.serverName : 'localhost', encrypt: true, trustServerCertificate: isExceptionalDispositionEndpoint(e),
        cryptoCredentialsDetails: isPreparedAttemptsEndpoint(e) ? { minVersion: 'TLSv1.2', maxVersion: 'TLSv1.2' } : { ca: e.ca, minVersion: 'TLSv1.2' }, tdsVersion: '7_4', useUTC: true,
        maxRetriesOnTransientErrors: 0, connectTimeout: 10000, requestTimeout: timeoutMs, cancelTimeout: 3000, packetSize: 4096,
        useColumnNames: false, rowCollectionOnDone: false, rowCollectionOnRequestCompletion: false } },isPreparedAttemptsEndpoint(e) ? e : isDispositionEndpoint(e) ? e as DispositionEndpoint : undefined);
    connection.inspectionDiagnostics=this.inspectionDiagnostics;
    let closed = false;
    const end = new Promise<void>(resolve => connection.once('end',() => { closed = true; resolve(); }));
    const notify = () => { wake?.(); wake = undefined; };
    const fail = (code: OlosError['code']) => { reportInspectionFailure(this.inspectionDiagnostics,new OlosError(code));failure ??= new OlosError(code,dispatched ? 'unknown' : 'not_sent'); notify(); };
    const cancel = () => { fail(timedOut ? 'TIMEOUT' : 'ABORTED'); if (dispatched) connection.cancel(); else connection.close(); };
    connection.on('secure',tls => {
      const exceptional = isExceptionalDispositionEndpoint(e), protocol = tls.getProtocol();
      if (!['TLSv1.2','TLSv1.3'].includes(protocol ?? '') || !exceptional && !tls.authorized) { fail('CONNECTION_FAILED'); connection.close(); return; }
      this.transportSecurity.push({ encrypted: true,identityValidated: !exceptional && tls.authorized,protocol: protocol!,tlsMode: exceptional ? 'exceptional-unverified-test' : isPreparedAttemptsEndpoint(e) ? 'certificate-pinned' : 'verified' });
    });
    request = new Request(procedures[snapshot.procedure],error => {
      if(error)reportInspectionFailure(this.inspectionDiagnostics,error,timedOut?'TIMEOUT':signal.aborted?'ABORTED':'EXECUTION_FAILED');
      if (error) fail(timedOut ? 'TIMEOUT' : signal.aborted ? 'ABORTED' : 'EXECUTION_FAILED');
      finished = true; notify();
    });
    // Assinaturas atuais são conferidas antes da conexão, inclusive bloqueios Reset/DataAvailable.
    prepareTdsRequest(request,snapshot.signature,snapshot.values,timeoutMs);
    connection.responseCapture = this.capture;
    if(snapshot.procedure==='LogReadData' && this.inspectionReplay)connection.responseReplay=this.inspectionReplay;
    connection.acquisition = { metadataObserved:columns=>{
      if(this.observedDatasets.length>=64)throw new OlosError('LIMIT_EXCEEDED','unknown');
      const metadata=columns.map(c=>({name:c.colName,type:c.type.name,nullable:Boolean(c.flags&1),dataLength:c.dataLength??null,collationHex:c.collation?.toBuffer().toString('hex')??null}));
      this.observedDatasets.push(metadata);this.observedMetadata.push(...metadata);this.responseObservation.datasetCount=this.observedDatasets.length;
    },fields: this.fields.length ? this.fields : undefined, limits: this.limits, fail: () => fail('INVALID_RESPONSE'), token: token => {
      if (finalDone) throw new OlosError('INVALID_RESPONSE','unknown');
      if (token.name === 'RETURNSTATUS') {lastStatus = token.value as number;this.responseObservation.returnValue=lastStatus;}
      if (['DONE','DONEPROC','DONEINPROC'].includes(token.name)) {
        if (token.sqlError || token.serverError || token.attention) {this.responseObservation.sqlError=true;fail('EXECUTION_FAILED');}
        if (token.name === 'DONEPROC' && !token.more) { finalDone = true; this.responseObservation.finalDone=true;returnValue = lastStatus; }
      }
    } };
    request.on('columnMetadata',columns => {
      if (!Array.isArray(columns) || (++datasets !== 1 && !this.inspectionReplay)) { fail('INVALID_RESPONSE'); connection.cancel(); return; }
      this.observedFields.push(...observeMetadata(columns,this.limits));
    });
    request.on('row',(columns: { value: Buffer | null; metadata: ColumnMetadata }[]) => {
      request.pause(); this.metrics.pauses++;
      if (slot || (datasets !== 1 && !this.inspectionReplay) || ++this.metrics.rows > 200_000 || columns.some(c => c.value !== null && !Buffer.isBuffer(c.value))) {
        fail('LIMIT_EXCEEDED'); connection.cancel(); return;
      }
      slot = columns.map(c => c.value); this.metrics.peakQueuedRows = Math.max(this.metrics.peakQueuedRows,1); notify();
    });
    request.on('returnValue',(name,value,metadata) => {
      const parameter = snapshot.signature.parameters.find(p => p.direction === 'output' && p.name === name);
      const type = parameter?.type;
      const length = type && 'length' in type ? type.length === 'max' ? 65535 : type.length * (type.kind === 'nvarchar' ? 2 : 1) : undefined;
      if (!parameter || !type || !('length' in type) || metadata.type.name !== (type.kind === 'nvarchar' ? 'NVarChar' : 'VarChar') || metadata.dataLength !== length || Object.hasOwn(output,name) || !(value === null || typeof value === 'string') || value === null && !parameter.nullable ||
        typeof value === 'string' && (Buffer.byteLength(value) > this.limits.maxCellBytes || 'length' in parameter.type && parameter.type.length !== 'max' && value.length > parameter.type.length)) {
        fail('INVALID_RESPONSE'); connection.cancel(); return;
      }
      if(name===snapshot.signature.errorOutput)this.responseObservation.errorMessagePresent=Boolean(value);
      Object.defineProperty(output,name,{ value, enumerable: true });
    });
    connection.on('infoMessage',() => { this.metrics.infoMessages++; }); // não guardar conteúdo pessoal/mensagens em logs
    connection.on('error',error => { reportInspectionFailure(this.inspectionDiagnostics,error,'CONNECTION_FAILED');fail('CONNECTION_FAILED'); finished = true; notify(); });
    connection.once('end',() => { if (!finished) fail('EXECUTION_FAILED'); finished = true; notify(); });
    this.busy = true;
    signal.addEventListener('abort',cancel,{ once: true });
    const timer = setTimeout(() => { timedOut = true; cancel(); },timeoutMs);
    try {
      await new Promise<void>((resolve,reject) => { connection.connect(error => {if(error){reportInspectionFailure(this.inspectionDiagnostics,error,'CONNECTION_FAILED');reject(new OlosError('CONNECTION_FAILED'));}else{this.inspectionDiagnostics?.({kind:'stage',stage:'SQL_LOGIN_ACCEPTED'});resolve();}}); });
      if ((isExceptionalDispositionEndpoint(e) || isPreparedAttemptsEndpoint(e)) && this.transportSecurity.length !== 1) throw new OlosError('CONNECTION_FAILED');
      if (failure || signal.aborted) throw failure ?? new OlosError('ABORTED');
      this.metrics.rows = 0; this.observedMetadata.length = 0; this.observedFields.length = 0; this.metrics.calls++; dispatched = true;
      this.inspectionDiagnostics?.({kind:'stage',stage:'RPC_DISPATCH_STARTED'});
      connection.callProcedure(request);
      while (true) {
        if (failure) throw failure;
        if (slot) {
          const row = slot; slot = undefined;
          yield { kind: 'row', values: row };
          if (signal.aborted || failure) throw failure ?? new OlosError('ABORTED','unknown');
          request.resume(); this.metrics.resumes++; continue;
        }
        if (finished) break;
        await new Promise<void>(resolve => { wake = resolve; });
      }
      if (!finalDone || returnValue === undefined || (datasets !== 1 && !this.inspectionReplay) || !Object.hasOwn(output,snapshot.signature.messageOutput) || !Object.hasOwn(output,snapshot.signature.errorOutput)) throw new OlosError('INVALID_RESPONSE','unknown');
      const errorMessage = output[snapshot.signature.errorOutput]!, message = output[snapshot.signature.messageOutput]!;
      if(isPreparedAttemptsEndpoint(e) && !this.inspectionReplay && (returnValue!==0 || errorMessage!==null && errorMessage!=='' || message!==null && message!==''))throw new OlosError('OUTPUT_ERROR','unknown');
      yield { kind: 'complete', datasetCount: datasets, returnValue, errorMessage, message, output: Object.freeze(output) };
    } catch (error) {
      reportInspectionFailure(this.inspectionDiagnostics,error);
      if (error instanceof OlosError) throw error;
      throw new OlosError('EXECUTION_FAILED',dispatched ? 'unknown' : 'not_sent');
    } finally {
      clearTimeout(timer); signal.removeEventListener('abort',cancel);
      // Nunca devolver conexão ao pool ou liberar exclusão enquanto houver execução sem término.
      if (!finished && dispatched) { connection.cancel(); request.resume(); }
      connection.close(); if (!closed) await end; this.busy = false;
    }
  }
}
/** Local sealed response replay: same exact parser, no Connection/LOGIN7/RPC and no remote retry. */
export function replayTdsResponse(fields:readonly TdsField[],chunks:()=>AsyncIterable<Buffer>,expectedRows:number):RawSource {
  const Debug=createRequire(import.meta.url)('tedious/lib/debug.js') as typeof import('tedious/lib/debug.js').default;
  const source={kind:'mssql' as const,assertExecutionAllowed(){},async *read(plan:LargePlan,signal:AbortSignal):AsyncGenerator<RawEvent>{
    const visible=fields.map(({sqlLength:_,collationHex:_c,unicodeMetadataPolicy:_u,...f})=>f);
    if(canonical(visible)!==canonical(plan.fields))throw new OlosError('CONTRACT_UNCONFIRMED');
    const parser=new ExactTokenParser(chunks(),new Debug({data:false,payload:false,packet:false,token:false}),
      {useUTC:true,lowerCaseGuids:false,tdsVersion:'7_4',useColumnNames:false,columnNameReplacer:undefined,camelCaseColumns:false},fields);
    let datasets=0,rows=0,final=false,status:number|undefined;
    const output=new Map<string,unknown>();
    for await(const token of parser.tokens()){
      if(signal.aborted || final)throw new OlosError('INVALID_RESPONSE');
      if(token.name==='COLMETADATA' && ++datasets!==1)throw new OlosError('INVALID_RESPONSE');
      if(token.name==='ROW' || token.name==='NBCROW'){
        if(datasets!==1 || ++rows>200000)throw new OlosError('INVALID_RESPONSE');
        yield {kind:'row',values:(token as unknown as {columns:{value:Buffer|null}[]}).columns.map(c=>c.value)};
      }
      if(token.name==='RETURNSTATUS')status=(token as unknown as {value:number}).value;
      if(token.name==='RETURNVALUE'){
        const t=token as unknown as {paramName:string;value:unknown;metadata:{type:{name:string};dataLength:number}};
        const p=plan.signature.parameters.find(p=>p.direction==='output' && p.name===t.paramName);
        if(!p || !('length' in p.type) || !['varchar','nvarchar'].includes(p.type.kind) ||
          t.metadata.type.name!==(p.type.kind==='nvarchar'?'NVarChar':'VarChar') ||
          t.metadata.dataLength!==(p.type.length==='max'?65535:p.type.length*(p.type.kind==='nvarchar'?2:1)) ||
          t.value===null && !p.nullable || ![plan.signature.messageOutput,plan.signature.errorOutput].includes(t.paramName) || output.has(t.paramName))throw new OlosError('INVALID_RESPONSE');
        output.set(t.paramName,t.value);
      }
      if(token.name==='ERROR')throw new OlosError('EXECUTION_FAILED');
      if(['DONE','DONEPROC','DONEINPROC'].includes(token.name)){
        const t=token as unknown as {sqlError:boolean;serverError:boolean;attention:boolean;more:boolean};
        if(t.sqlError || t.serverError || t.attention)throw new OlosError('INVALID_RESPONSE');
        if(token.name==='DONEPROC' && !t.more)final=true;
      }
    }
    // The chunk iterator verifies SHA-256 before EOF. Never confirm a partial/corrupt response.
    if(!final || status!==0 || datasets!==1 || rows!==expectedRows || output.size!==2 ||
      [...output.values()].some(v=>v!==null && v!==''))throw new OlosError('INVALID_RESPONSE');
    yield {kind:'complete',returnValue:0,errorMessage:null,datasetCount:1};
  }};
  registered.add(source);return source;
}
