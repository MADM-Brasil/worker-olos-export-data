import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { describe, expect, it } from 'vitest';
import { documentedTableContracts, manualEvidence } from '../src/olos/documented-tables.js';
import { tableContracts, type Procedure } from '../src/olos/contracts.js';
import { documentedReadSemantics, verifiedProcedureContracts, textTypeFromCatalog, compareSignatures } from '../src/olos/procedure-signatures.js';
import { adaptDataset } from '../src/olos/adapter.js';
import { defaultLimits } from '../src/persistence/types.js';
import { documentedFixture, documentedRecord } from './support/olos-documented-fixture.js';

const tables = Object.keys(documentedTableContracts) as (keyof typeof documentedTableContracts)[];
describe('evidência documental DBA-MN002 e separação do servidor atual', () => {
  it('vincula evidência ao PDF completo e ao hash da cópia lida', async () => {
    const pdf = await readFile(new URL('../docs/reference/olos-exportdata-v3.pdf',import.meta.url));
    expect(createHash('sha256').update(pdf).digest('hex')).toBe(manualEvidence.sha256);
    expect(manualEvidence).toMatchObject({ code: 'DBA-MN002', revisionDate: '2020-06-03', pages: 33 });
  });
  it('preserva v1 e registra todos os 87 campos elegíveis e Password somente na exclusão', () => {
    expect(tables.map(t => documentedTableContracts[t].fields.length)).toEqual([18,17,32,11,7,2]);
    expect(tables.reduce((n,t) => n + documentedTableContracts[t].fields.length,0)).toBe(87);
    expect(documentedTableContracts.Users.excludedFields).toEqual([{ name: 'Password', sqlType: 'nvarchar(100)', page: 30, nullable: null, serverVerification: null }]);
    for (const t of tables) {
      expect(tableContracts[t].version).toBe(`${t}/v1`);
      expect(documentedTableContracts[t].version).toBe(`${t}/v2`);
      expect(documentedTableContracts[t].approved).toBe(false);
      expect(documentedTableContracts[t].currentLayoutConfirmed).toBe(false);
      expect(documentedTableContracts[t].fields.every(f => f.serverVerification === null && f.nullable === null)).toBe(true);
    }
  });
  it.each(tables)('nome e tipo de chave de %s são documentados, sem presumir BIGINT ou escopo global', table => {
    const c = documentedTableContracts[table];
    expect(c.fields.find(f => f.name === c.documentedKey)?.sqlType).toBe('int');
    expect(c.documentedKeyRole).toBe(['Campaign','Disposition'].includes(table) ? 'reference_identifier' : 'primary_key');
    expect(c.fields.some(f => f.sqlType.includes('decimal'))).toBe(false);
  });
  it('preserva diferenças de nomes, tipo, comprimentos e flags inteiras do manual', () => {
    const field = (t: keyof typeof documentedTableContracts, n: string) => documentedTableContracts[t].fields.find(f => f.name === n)?.sqlType;
    expect(field('AttemptsRawData','Route')).toBe('varchar(256)'); expect(field('PbxBillingData','Route')).toBe('int');
    expect(field('PbxBillingData','TelecomCallid')).toBe('char(16)'); expect(field('PbxBillingData','CallIdTelecom')).toBeUndefined();
    expect(field('AttemptsRawData','Dnis')).toBe('varchar(255)'); expect(field('InboundRawData','Dnis')).toBe('varchar(20)');
    expect(field('Users','Flag_Agent')).toBe('int'); expect(field('Users','AgentId')).toBe('int'); expect(field('Users','UserId')).toBe('int');
    expect(field('Campaign','Description')).toBe('nvarchar(510)'); expect(field('Disposition','Description')).toBe('nvarchar(200)');
  });
  it.each(tables)('valida linha sintética completa de %s usando o layout documental', table => {
    const c = documentedFixture(table), row = documentedRecord(table);
    const data = { columns: c.columns, rows: [row], representation: 'exact-text/v1' as const, exactEncodingEvidence: 'SYNTHETIC ONLY' };
    expect(adaptDataset(data,c)[0]).toEqual(row);
    const nulls = { ...documentedRecord(table,'1') };
    for (const field of documentedTableContracts[table].fields) if (field.name !== documentedTableContracts[table].documentedKey) nulls[field.name] = null;
    expect(adaptDataset({ ...data, rows: [nulls] },c)[0]).toEqual(nulls);
    expect(() => adaptDataset({ ...data, rows: [{ ...row, [documentedTableContracts[table].documentedKey]: '9007199254740993' }] },c)).toThrow('PRECISION_UNSAFE');
    expect(() => adaptDataset({ ...data, rows: [{ ...row, Unknown: 'x' }] },c)).toThrow('INVALID_RESPONSE');
  });
  it('preserva CHAR com espaço, unicode e null; não transforma flags em boolean', () => {
    const pbx = documentedFixture('PbxBillingData'), users = documentedFixture('Users');
    expect(adaptDataset({ columns: pbx.columns, rows: [documentedRecord('PbxBillingData')], representation: 'exact-text/v1', exactEncodingEvidence: 'synthetic' },pbx)[0]?.TelecomCallid).toBe('000000000000001 ');
    const row = documentedRecord('Users');
    expect(adaptDataset({ columns: users.columns, rows: [row], representation: 'exact-text/v1', exactEncodingEvidence: 'synthetic' },users)[0]?.UserName).toBe('Agente Á');
    expect(() => adaptDataset({ columns: users.columns, rows: [{ ...row, Flag_Agent: true }], representation: 'exact-text/v1', exactEncodingEvidence: 'synthetic' },users)).toThrow();
  });
  it('detecta Password e comprimento incompatível sem usar o PDF como aprovação operacional', () => {
    const c = documentedFixture('Users'), row = documentedRecord('Users');
    const data = { columns: c.columns, representation: 'exact-text/v1' as const, exactEncodingEvidence: 'synthetic' };
    expect(() => adaptDataset({ ...data, rows: [{ ...row, Password: 'must never persist' }] },c)).toThrow('INVALID_RESPONSE');
    expect(() => adaptDataset({ ...data, rows: [{ ...row, Login: 'x'.repeat(101) }] },c)).toThrow('PRECISION_UNSAFE');
    expect(() => adaptDataset({ ...data, rows: [row] },{ ...c, source: { ...c.source, approved: false } })).toThrow('CONTRACT_UNCONFIRMED');
  });
  it('DATETIME não aceita display arredondado como valor exato nem amplia o domínio para DATETIME2', () => {
    const c = documentedFixture('Users'), row = documentedRecord('Users');
    const data = { columns: c.columns, representation: 'exact-text/v1' as const, exactEncodingEvidence: 'synthetic' };
    for (const clock of ['2020-06-03T12:00:00.003','2020-06-03T12:00:00.007','2020-06-03T12:00:00.1234567','0001-01-01T00:00:00']) {
      expect(() => adaptDataset({ ...data, rows: [{ ...row, LastLogin: clock }] },c)).toThrow('PRECISION_UNSAFE');
    }
    expect(adaptDataset({ ...data, rows: [{ ...row, LastLogin: '2020-06-03T12:00:00.010' }] },c)[0]?.LastLogin).toBe('2020-06-03T12:00:00.010');
  });
  it('separa a assinatura relatada de sys.parameters da aprovação de execução/semântica', () => {
    for (const c of Object.values(verifiedProcedureContracts)) {
      expect(c.server.independentlyQueried).toBe(false); expect(c.executionApproved).toBe(false);
      expect(c.realExecutionAllowed).toBe(false); expect(c.semanticsValidated).toBe(false);
      expect(c.server.parameters.every(p => p.nullable === null)).toBe(true);
    }
  });
  it.each(['ReadDataCurrentDay','ReadDataPreviousDays','ResetReadData'] as Procedure[])('%s coincide com a tabela de parâmetros do manual', procedure => {
    expect(verifiedProcedureContracts[procedure].differences).toEqual([]);
  });
  it('registra todas as divergências das tabelas de parâmetros sem esconder os exemplos contraditórios', () => {
    expect(verifiedProcedureContracts.ReadData.differences).toEqual([
      { parameter: 'Message', aspect: 'type', documented: 'varchar(4000)', reported: 'varchar(max)' },
      { parameter: 'ErrorMessage', aspect: 'type', documented: 'varchar(4000)', reported: 'varchar(max)' },
    ]);
    expect(verifiedProcedureContracts.LogReadData.differences?.map(d => d.aspect)).toEqual(['name_case','type','type']);
    expect(verifiedProcedureContracts.LogReadData.documented.notes).toHaveLength(2);
    expect(verifiedProcedureContracts.ReReadData.differences?.map(d => d.reported)).toEqual(['varchar(4000)','varchar(4000)']);
    expect(verifiedProcedureContracts.DataAvailable.documented.parameters).toBeNull();
    expect(verifiedProcedureContracts.DataAvailable.server.parameters.map(p => p.name)).toEqual(['TableName','DateExport']);
    expect(compareSignatures([],verifiedProcedureContracts.DataAvailable.server.parameters).every(d => d.aspect === 'presence')).toBe(true);
  });
  it.each([['nvarchar',256,128],['nvarchar',1020,510],['varchar',128,128],['varchar',-1,'max'],['nvarchar',-1,'max']] as const)('normaliza %s max_length=%s bytes para %s unidades', (kind,bytes,length) => {
    expect(textTypeFromCatalog(kind,bytes)).toEqual({ kind, length });
  });
  it.each([0,-2,255,8001,1.5])('não interpreta max_length inválido %s como NVARCHAR válido', bytes => {
    expect(() => textTypeFromCatalog('nvarchar',bytes)).toThrow('INVALID_PARAMETER');
  });
  it('registra teto 200 mil e ausência de parâmetro de paginação sem elevar os limites internos', () => {
    expect(documentedReadSemantics.normal.ReadDataCurrentDay.maxRows).toBe(200_000);
    expect(documentedReadSemantics.normal.ReadData.maxRows).toBe('unbounded');
    expect(documentedReadSemantics.callerCursorOrLimitParameter).toBe(false);
    expect(documentedReadSemantics.retentionDays).toBeNull();
    expect(defaultLimits.maxRecords).toBeLessThan(200_000);
    expect(verifiedProcedureContracts.ReadDataCurrentDay.server.parameters.map(p => p.name)).toEqual(['TableName','Message','ErrorMessage']);
  });
});
