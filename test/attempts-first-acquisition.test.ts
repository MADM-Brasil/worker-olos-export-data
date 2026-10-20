import { expect, it, vi } from 'vitest';
import { Socket } from 'node:net';
import pg from 'pg';
import { attemptsCommand } from '../adapters/attempts-integration.js';
import { firstAttemptsAcquisitionRequirements } from '../src/integration/attempts/contracts.js';
import { prepareTdsRequest } from '../src/olos/tds/source.js';
import { reportedSignature, verifiedProcedureContracts } from '../src/olos/procedure-signatures.js';
import { Request } from 'tedious';

it('expõe as informações faltantes para uma aquisição recuperável e não promete o dia inteiro', () => {
  const r = firstAttemptsAcquisitionRequirements('2026-10-19');
  expect(r).toMatchObject({ requestedDate: '2026-10-19',executionAllowed: false,remoteDateParameter: false,wholeDayGuaranteed: false,
    procedure: 'ReadDataCurrentDay',parameters: { TableName: 'AttemptsRawData' },destination: { host: '127.0.0.1',port: 5434,database: 'olos_worker' } });
  expect(r.missing.map(m => m.code)).toContain('CONSUMED_RANGE_RECOVERY_NOT_PROVEN');
  expect(() => firstAttemptsAcquisitionRequirements('2026-02-30')).toThrow();
});
it('ReadDataCurrentDay não aceita DateExport para selecionar uma data passada', () => {
  const signature = reportedSignature('ReadDataCurrentDay',{ evidence: 'synthetic policy only',
    nullable: Object.fromEntries(verifiedProcedureContracts.ReadDataCurrentDay.server.parameters.map(p => [p.name,p.direction === 'output'])) });
  expect(() => prepareTdsRequest(new Request('dbo.usp_ExportData_ReadDataCurrentDay',() => {}),signature,
    { TableName: 'AttemptsRawData',DateExport: '2026-10-19' },300000)).toThrow();
});
it('CLI de primeira leitura falha antes de rede, PostgreSQL, segredos ou diretório inexistente; confirmação não dispensa recuperação', async () => {
  vi.stubEnv('SYNC_ENABLED','false');
  const socket = vi.spyOn(Socket.prototype,'connect').mockImplementation(() => { throw new Error('UNEXPECTED_NETWORK'); });
  const pool = vi.spyOn(pg.Pool.prototype,'connect').mockImplementation(() => { throw new Error('UNEXPECTED_POSTGRES'); });
  try {
    const args = ['read','--mode','corporate-local','--work-dir','/nonexistent/olos-first-test','--source-config','/nonexistent/private.json',
      '--export-date','2026-10-19','--confirm','EXECUTE_ATTEMPTS_READ_ONCE'];
    for (let attempt=0;attempt<2;attempt++) await expect(attemptsCommand([...args])).rejects.toMatchObject({
      message: 'ATTEMPTS_CORPORATE_CONTRACT_UNCONFIRMED',report: { requestedDate: '2026-10-19',executionAllowed: false } });
    await expect(attemptsCommand([...args,'--allow-unsafe-recovery','true'])).rejects.toThrow('ATTEMPTS_CONTROLLED_TEST_BLOCKED');
    await expect(attemptsCommand([...args].map(v => v === 'corporate-local' ? 'isolated-fixture' : v))).rejects.toThrow('ATTEMPTS_CONTROLLED_TEST_BLOCKED');
    expect(socket).not.toHaveBeenCalled(); expect(pool).not.toHaveBeenCalled();
  } finally { pool.mockRestore(); socket.mockRestore(); vi.unstubAllEnvs(); }
});
