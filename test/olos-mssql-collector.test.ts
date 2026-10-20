import sql from 'mssql';
import { afterEach, describe, expect, it, vi } from 'vitest';
import { BlockedMssqlTransport, prepareCall } from '../src/olos/client.js';
import { MssqlResponseCollector } from '../src/olos/mssql-collector.js';
import { signature } from './support/olos.js';

afterEach(() => vi.restoreAllMocks());
function fixture(maxRows = 1000, maxBytes = 4_194_304) {
  // Request sem pool: apenas EventEmitter e cancel mockado. Nenhum execute/connect é chamado.
  const request = new sql.Request(), cancel = vi.spyOn(request, 'cancel').mockImplementation(() => {});
  const controller = new AbortController();
  const collector = new MssqlResponseCollector(request, controller.signal, { maxRows, maxBytes });
  return { request, cancel, controller, collector };
}
const result = { output: { Message: null, ErrorMessage: null }, returnValue: 0 };
describe('fronteira streaming node-mssql por eventos sintéticos', () => {
  it('captura metadados, datasets, BIGINT textual, null e OUTPUT sem invocar SQL', () => {
    const { request, collector } = fixture();
    try {
      request.emit('recordset', [{ name: 'AttemptsRawDataID', type: sql.BigInt }, { name: 'FixtureText', type: sql.NVarChar }]);
      request.emit('row', ['9223372036854775807', null]);
      request.emit('recordset', [{ name: 'FixtureOther', type: sql.Int }]);
      request.emit('row', [1]);
      const reply = collector.finish(result);
      expect(reply.datasets).toHaveLength(2);
      expect(reply.datasets[0]?.rows).toEqual([{ AttemptsRawDataID: '9223372036854775807', FixtureText: null }]);
      expect(reply.output).toEqual(result.output);
      expect(request.stream).toBe(true); expect(request.arrayRowMode).toBe(true);
    } finally { collector.dispose(); }
    expect(request.listenerCount('row')).toBe(0); expect(request.listenerCount('error')).toBe(0);
  });
  it.each([sql.Decimal,sql.Numeric,sql.DateTime2,sql.DateTimeOffset,sql.DateTime,sql.SmallDateTime])('rejeita tipo com parser destrutivo no evento de metadado %#', type => {
    const { request, collector, cancel } = fixture();
    try {
      request.emit('recordset', [{ name: 'FixtureValue', type, precision: 38, scale: 7 }]);
      expect(cancel).toHaveBeenCalledTimes(1);
      expect(() => collector.finish(result)).toThrow('PRECISION_UNSAFE');
    } finally { collector.dispose(); }
  });
  it('remove Password pelo ordinal sem copiar valor para o dataset', () => {
    const { request, collector } = fixture();
    try {
      request.emit('recordset', [{ name: 'UserId', type: sql.Int }, { name: 'Password', type: sql.NVarChar }]);
      request.emit('row', [1,'secret']);
      expect(collector.finish(result).datasets[0]?.rows).toEqual([{ UserId: 1 }]);
      expect(JSON.stringify(collector.finish(result))).not.toContain('secret');
    } finally { collector.dispose(); }
  });
  it('limita volume durante streaming e descarta linhas adicionais após cancelar', () => {
    const { request, collector, cancel } = fixture(2);
    try {
      request.emit('recordset', [{ name: 'FixtureId', type: sql.BigInt }]);
      for (let i = 0; i < 10_000; i++) request.emit('row', [String(i)]);
      expect(cancel).toHaveBeenCalledTimes(1);
      expect(() => collector.finish(result)).toThrow('LIMIT_EXCEEDED');
    } finally { collector.dispose(); }
  });
  it('limita bytes durante streaming sem stringify de payload', () => {
    const { request, collector } = fixture(1000, 20);
    try {
      request.emit('recordset', [{ name: 'Fixture', type: sql.NVarChar }]);
      request.emit('row', ['x'.repeat(100)]);
      expect(() => collector.finish(result)).toThrow('LIMIT_EXCEEDED');
    } finally { collector.dispose(); }
  });
  it('erros nativos, colunas duplicadas e tipos desconhecidos ficam sanitizados', () => {
    const emitters = [
      (request: sql.Request) => request.emit('error', new Error('private values')),
      (request: sql.Request) => request.emit('recordset', [{ name: 'x', type: sql.Int },{ name: 'X', type: sql.Int }]),
      (request: sql.Request) => request.emit('recordset', [{ name: 'x', type: sql.Float }]),
      (request: sql.Request) => { request.emit('recordset', [{ name: 'x', type: sql.BigInt }]); request.emit('row',[new Date()]); },
    ];
    for (const emit of emitters) {
      const { request, collector } = fixture();
      try { emit(request); expect(() => collector.finish(result)).toThrow(/EXECUTION_FAILED|INVALID_RESPONSE|PRECISION_UNSAFE/); }
      finally { collector.dispose(); }
    }
  });
  it('abort cancela mas a resposta só é finalizada explicitamente após drenagem', () => {
    const { request, collector, controller, cancel } = fixture();
    try {
      controller.abort(); expect(cancel).toHaveBeenCalledOnce();
      request.emit('row', ['late']);
      expect(() => collector.finish(result)).toThrow('ABORTED');
    } finally { collector.dispose(); }
  });
  it('timeout emitido pelo driver continua TIMEOUT sem expor mensagem', () => {
    const { request, collector } = fixture();
    try {
      request.emit('error', new sql.RequestError('private message', 'ETIMEOUT'));
      expect(() => collector.finish(result)).toThrow('TIMEOUT');
    } finally { collector.dispose(); }
  });
  it('gate real bloqueia inclusive acesso direto antes de construir pool ou Request', async () => {
    const factory = vi.fn((_config: sql.config): sql.ConnectionPool => { throw new Error('must not run'); });
    const native = new BlockedMssqlTransport({ server: 'must-not-connect.invalid' }, factory);
    await expect(native.connect()).rejects.toMatchObject({ code: 'REAL_EXECUTION_BLOCKED' });
    await expect(native.invoke(prepareCall(signature(), { FixtureTable: 'AttemptsRawData', FixtureAfter: null }, 1000), new AbortController().signal)).rejects.toMatchObject({ code: 'REAL_EXECUTION_BLOCKED' });
    expect(factory).not.toHaveBeenCalled(); await native.close();
  });
});
