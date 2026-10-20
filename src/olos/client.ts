import sql from 'mssql';
import { procedures, sensitiveField } from './contracts.js';
import { MssqlResponseCollector } from './mssql-collector.js';
import { dateParameter, utcDriverDate } from './parameter-codecs.js';
import { OlosError, type ParameterType, type PreparedCall, type ProcedureReply, type ProcedureSignature,
  type ProcedureTransport } from './types.js';

const parameterName = /^[A-Za-z][A-Za-z0-9_]{0,127}$/;
export function prepareCall(signature: ProcedureSignature, values: PreparedCall['values'], timeoutMs: number): PreparedCall {
  if (!Object.hasOwn(procedures, signature.procedure) || !signature.evidence || !signature.version) throw new OlosError('CONTRACT_UNCONFIRMED');
  if (['DataAvailable','ResetReadData'].includes(signature.procedure)) throw new OlosError('PROCEDURE_BLOCKED');
  if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 300_000) throw new OlosError('INVALID_PARAMETER');
  const names = new Set<string>(), inputs = new Set<string>();
  for (const p of signature.parameters) {
    const lower = p.name.toLowerCase();
    if (!parameterName.test(p.name) || sensitiveField(p.name) || names.has(lower) || typeof p.nullable !== 'boolean' ||
      !['input','output'].includes(p.direction)) throw new OlosError('INVALID_PARAMETER');
    names.add(lower);
    if (!['nvarchar','varchar','bigint','int','bit','date'].includes(p.type.kind)) throw new OlosError('INVALID_PARAMETER');
    if ((p.type.kind === 'nvarchar' || p.type.kind === 'varchar') &&
      p.type.length !== 'max' && (!Number.isInteger(p.type.length) || p.type.length < 1 || p.type.length > (p.type.kind === 'nvarchar' ? 4000 : 8000))) throw new OlosError('INVALID_PARAMETER');
    if (p.direction === 'output') continue;
    inputs.add(p.name);
    if (!Object.hasOwn(values, p.name)) throw new OlosError('INVALID_PARAMETER');
    const v = values[p.name];
    if (v === null && p.nullable) continue;
    if (p.type.kind === 'bit') { if (typeof v !== 'boolean') throw new OlosError('INVALID_PARAMETER'); }
    else {
      if (typeof v !== 'string') throw new OlosError('INVALID_PARAMETER');
      if (p.type.kind === 'nvarchar' || p.type.kind === 'varchar') {
        // VARCHAR depende de codepage ainda não confirmada: aceitar somente ASCII em parâmetros.
        if (p.type.length !== 'max' && v.length > p.type.length || (p.type.kind === 'varchar' && /[^\x00-\x7f]/.test(v))) throw new OlosError('INVALID_PARAMETER');
      } else if (p.type.kind === 'date') {
        dateParameter(v);
      } else {
        if (!/^(0|-?[1-9]\d*)$/.test(v)) throw new OlosError('INVALID_PARAMETER');
        const n = BigInt(v), min = p.type.kind === 'int' ? -2147483648n : -9223372036854775808n;
        const max = p.type.kind === 'int' ? 2147483647n : 9223372036854775807n;
        if (n < min || n > max) throw new OlosError('INVALID_PARAMETER');
      }
    }
  }
  for (const output of [signature.messageOutput, signature.errorOutput]) {
    const p = signature.parameters.find(p => p.name === output && p.direction === 'output');
    if (!p || !['nvarchar','varchar'].includes(p.type.kind)) throw new OlosError('CONTRACT_UNCONFIRMED');
  }
  if (signature.messageOutput === signature.errorOutput || Object.keys(values).some(k => !inputs.has(k))) throw new OlosError('INVALID_PARAMETER');
  // Snapshot antes de qualquer await; o chamador não altera assinatura/valores em voo.
  return structuredClone({ procedure: signature.procedure, signature, values, timeoutMs });
}
function mssqlType(t: ParameterType): sql.ISqlType | (() => sql.ISqlType) {
  switch (t.kind) {
    case 'nvarchar': return sql.NVarChar(t.length === 'max' ? sql.MAX : t.length);
    case 'varchar': return sql.VarChar(t.length === 'max' ? sql.MAX : t.length);
    case 'bigint': return sql.BigInt;
    case 'int': return sql.Int;
    case 'bit': return sql.Bit;
    case 'date': return sql.Date;
  }
}
/** Apenas prepara o Request node-mssql: não conecta e não invoca procedure. */
export function prepareMssqlRequest(request: sql.Request, input: PreparedCall): sql.Request {
  const call = prepareCall(input.signature, input.values, input.timeoutMs);
  if (call.signature.parameters.some(p => p.type.kind === 'date')) {
    // Leitura da configuração efetiva do parent do Request, não uma flag fornecida para facilitar testes.
    const parent = (request as sql.Request & { parent?: { config?: { options?: { useUTC?: boolean } } } }).parent;
    if (parent?.config?.options?.useUTC !== true) throw new OlosError('CONTRACT_UNCONFIRMED');
  }
  // node-mssql 11 não expõe timeout por Request: o cliente coordena abort e drenagem;
  // um executor futuro deve configurar também requestTimeout no pool dedicado.
  request.arrayRowMode = true; // futura aquisição deve detectar nomes duplicados por ordinal
  for (const p of call.signature.parameters) {
    if (p.direction === 'output') request.output(p.name, mssqlType(p.type));
    else {
      const value = call.values[p.name]!;
      const encoded = value === null ? null : p.type.kind === 'date' ? utcDriverDate(value as string) : p.type.kind === 'int' ? Number(value) : value;
      request.input(p.name, mssqlType(p.type), encoded);
    }
  }
  return request;
}

/** Gate imutável da Fase 3A. Não cria pool, não lê ambiente e não abre sockets. */
export class BlockedMssqlTransport implements ProcedureTransport {
  readonly kind = 'mssql' as const;
  private pool: sql.ConnectionPool | undefined;
  constructor(private readonly options?: sql.config, private readonly factory = (options: sql.config) => new sql.ConnectionPool(options)) {}
  private gate(): void { throw new OlosError('REAL_EXECUTION_BLOCKED'); }
  async connect(): Promise<void> {
    this.gate(); // Sem flag/env/argumento para liberar. Código nativo preparado para revisão futura.
    if (!this.options) throw new OlosError('CONTRACT_UNCONFIRMED');
    this.pool = this.factory(this.options);
    this.pool.on('error', () => { /* sem mensagem bruta; próxima operação deve falhar e invalidar pool */ });
    await this.pool.connect();
  }
  async invoke(call: PreparedCall, signal: AbortSignal): Promise<import('./types.js').ProcedureResponse> {
    this.gate();
    if (!this.pool) throw new OlosError('CONNECTION_FAILED');
    const request = prepareMssqlRequest(this.pool.request(), call);
    const collector = new MssqlResponseCollector(request, signal, { maxRows: 1000, maxBytes: 4_194_304 });
    // Vincular o receiver é necessário para o método do driver. O gate acima precede qualquer RPC.
    const invokeProcedure = request.execute.bind(request);
    try { return collector.finish(await invokeProcedure(procedures[call.procedure])); }
    finally { collector.dispose(); }
  }
  async close(): Promise<void> { await this.pool?.close(); this.pool = undefined; }
}

export interface ClientLimits { readonly maxRows: number; readonly maxBytes: number; readonly reconnectDelayMs: number; }
export class OlosProcedureClient {
  private connected = false;
  private busy = false;
  private retryAt = 0;
  private readonly limits: ClientLimits;
  constructor(private readonly transport: ProcedureTransport, limits: Partial<ClientLimits> = {}, private readonly now = Date.now) {
    this.limits = Object.freeze({ maxRows: 1000, maxBytes: 4_194_304, reconnectDelayMs: 1000, ...limits });
    for (const n of Object.values(this.limits)) if (!Number.isSafeInteger(n) || n < 1) throw new OlosError('INVALID_PARAMETER');
    if (this.limits.maxRows > 100_000 || this.limits.maxBytes > 64 * 1024 * 1024 || this.limits.reconnectDelayMs > 300_000) throw new OlosError('INVALID_PARAMETER');
  }
  async call(signature: ProcedureSignature, values: PreparedCall['values'], signal: AbortSignal, timeoutMs = 5000): Promise<ProcedureReply> {
    if (this.transport.kind !== 'synthetic') throw new OlosError('REAL_EXECUTION_BLOCKED');
    const call = prepareCall(signature, values, timeoutMs);
    if (signal.aborted) throw new OlosError('ABORTED');
    if (this.busy) throw new OlosError('BUSY');
    if (this.now() < this.retryAt) throw new OlosError('RECONNECT_BACKOFF');
    this.busy = true;
    let dispatched = false, timedOut = false;
    const controller = new AbortController();
    const abort = () => controller.abort();
    signal.addEventListener('abort', abort, { once: true });
    let timer: ReturnType<typeof setTimeout> | undefined;
    try {
      if (!this.connected) {
        try { await this.transport.connect(); this.connected = true; }
        catch { throw new OlosError('CONNECTION_FAILED'); }
      }
      if (signal.aborted) throw new OlosError('ABORTED');
      timer = setTimeout(() => { timedOut = true; controller.abort(); }, timeoutMs);
      dispatched = true;
      // Não usar Promise.race: timeout/abort não libera exclusão enquanto invoke estiver pendente.
      const response = await this.transport.invoke(call, controller.signal);
      if (timedOut) throw new OlosError('TIMEOUT', 'unknown');
      if (signal.aborted) throw new OlosError('ABORTED', 'unknown');
      let rows = 0, bytes = 0;
      const datasets = response.datasets.map(dataset => {
        const columns = dataset.columns.filter(c => !sensitiveField(c.name));
        if (new Set(columns.map(c => c.name.toLowerCase())).size !== columns.length) throw new OlosError('INVALID_RESPONSE', 'unknown');
        const safeRows = dataset.rows.map(row => {
          if (++rows > this.limits.maxRows) throw new OlosError('LIMIT_EXCEEDED', 'unknown');
          if (!row || Object.getPrototypeOf(row) !== Object.prototype) throw new OlosError('INVALID_RESPONSE', 'unknown');
          const safe: Record<string, unknown> = {};
          for (const [key, value] of Object.entries(row)) {
            if (sensitiveField(key)) continue; // nunca copiar Password para o resultado público
            if (value instanceof Date || typeof value === 'number' && !Number.isSafeInteger(value)) throw new OlosError('PRECISION_UNSAFE', 'unknown');
            if (value !== null && !['string','boolean','number'].includes(typeof value) && !(value instanceof Uint8Array)) throw new OlosError('INVALID_RESPONSE', 'unknown');
            bytes += Buffer.byteLength(key) + (typeof value === 'string' ? Buffer.byteLength(value) : value instanceof Uint8Array ? value.byteLength : 16);
            if (bytes > this.limits.maxBytes) throw new OlosError('LIMIT_EXCEEDED', 'unknown');
            Object.defineProperty(safe, key, { value: value instanceof Uint8Array ? new Uint8Array(value) : value, enumerable: true });
          }
          return Object.freeze(safe);
        });
        return Object.freeze({ representation: dataset.representation,
          ...(dataset.exactEncodingEvidence === undefined ? {} : { exactEncodingEvidence: dataset.exactEncodingEvidence }),
          columns: structuredClone(columns), rows: Object.freeze(safeRows) });
      });
      const message = response.output[call.signature.messageOutput], error = response.output[call.signature.errorOutput];
      if (!(message === null || typeof message === 'string') || !(error === null || typeof error === 'string')) throw new OlosError('INVALID_RESPONSE', 'unknown');
      for (const [name, value] of [[call.signature.messageOutput, message], [call.signature.errorOutput, error]] as const) {
        const p = call.signature.parameters.find(p => p.name === name)!;
        if (value === null && !p.nullable) throw new OlosError('INVALID_RESPONSE', 'unknown');
        if (typeof value === 'string') {
          bytes += Buffer.byteLength(value);
          if ('length' in p.type && p.type.length !== 'max' && value.length > p.type.length || bytes > this.limits.maxBytes) throw new OlosError('LIMIT_EXCEEDED', 'unknown');
        }
      }
      if (typeof response.returnValue !== 'number' || !Number.isSafeInteger(response.returnValue)) throw new OlosError('INVALID_RESPONSE', 'unknown');
      // Não devolver outputs adicionais não contratados, potencialmente sensíveis.
      return Object.freeze({ datasets: Object.freeze(datasets), Message: message, ErrorMessage: error,
        output: Object.freeze({ [call.signature.messageOutput]: message, [call.signature.errorOutput]: error }), returnValue: response.returnValue });
    } catch (error) {
      this.connected = false;
      this.retryAt = this.now() + this.limits.reconnectDelayMs;
      try { await this.transport.close(); } catch { /* erro original sanitizado prevalece */ }
      if (timedOut) throw new OlosError('TIMEOUT', 'unknown');
      if (signal.aborted) throw new OlosError('ABORTED', dispatched ? 'unknown' : 'not_sent');
      if (error instanceof OlosError) throw error;
      if (typeof error === 'object' && error !== null && 'code' in error && error.code === 'ETIMEOUT') throw new OlosError('TIMEOUT', dispatched ? 'unknown' : 'not_sent');
      throw new OlosError('EXECUTION_FAILED', dispatched ? 'unknown' : 'not_sent');
    } finally {
      if (timer) clearTimeout(timer);
      signal.removeEventListener('abort', abort);
      this.busy = false;
    }
  }
  async close(): Promise<void> {
    if (this.busy) throw new OlosError('BUSY');
    this.connected = false;
    await this.transport.close();
  }
}
