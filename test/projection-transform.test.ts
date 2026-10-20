import { describe, expect, it, vi } from 'vitest';
import type { PoolClient } from 'pg';
import { destinationContract, fields, type Candidate } from '../src/projection/contract.js';
import { projectCall, type MappingPlan } from '../src/projection/transform.js';
import { prepareRawStatement, RawCallsWriter, type WriteApproval } from '../src/projection/writer.js';

import { fixturePlan } from './support/projection-plan.js';
describe('contrato e transformação sem inferências', () => {
  it('registra os 41 campos e as duas referências sem inventar tipos de R2', () => {
    expect(fields).toHaveLength(41);
    expect(Object.values(destinationContract.fields).filter(f => f.reference1Type!==null)).toHaveLength(23);
    expect(Object.values(destinationContract.fields).filter(f => f.inReference2)).toHaveLength(33);
    expect(destinationContract.fields.data_carga).toMatchObject({ reference1Type: null,reference2Type: null,discrepancy: 'select_only_in_reference1',nullable: 'unknown' });
    expect(destinationContract.physicalDdlConfirmed).toBe(false);
    expect(destinationContract.callIdentityConfirmed).toBe(false);
    expect(destinationContract.fields.internal_id_olos.reference1PrimaryKey).toBe(false);
  });
  it('mapeia só campos autorizados e mantém IDs BIGINT/TEXT independentes e exatos', () => {
    const input = { FixtureId: '9223372036854775807',CallText: '00012-AbC',Internal: 'different-0002' };
    const result = projectCall(input,{},fixturePlan);
    expect(result.status).toBe('ready');
    expect(result.row).toEqual({ raw_olos_call_id: '9223372036854775807',olos_call_id: '00012-AbC' });
    expect(result.original.Internal).toBe('different-0002');
    expect(result.fields.internal_id_olos.state).toBe('protected');
    expect(input).toEqual({ FixtureId: '9223372036854775807',CallText: '00012-AbC',Internal: 'different-0002' });
  });
  it('distingue null de campo ausente sem produzir string vazia', () => {
    const n = projectCall({ FixtureId: '1',CallText: null },{},fixturePlan);
    expect(n.status).toBe('ready'); expect(n.row?.olos_call_id).toBeNull(); expect(n.fields.olos_call_id.state).toBe('null');
    const absent = projectCall({ FixtureId: '1' },{},fixturePlan);
    expect(absent.status).toBe('pending'); expect(absent.row).toBeUndefined(); expect(absent.fields.olos_call_id.state).toBe('absent');
  });
  it('campo desconhecido é pendência e não vira coluna SQL', () => {
    const result = projectCall({ FixtureId: '1',CallText: 'A',Unexpected: 'preserved' },{},fixturePlan);
    expect(result.original.Unexpected).toBe('preserved'); expect(result.row).toBeUndefined();
    expect(result.issues).toContainEqual({ code: 'UNMAPPED_SOURCE_FIELD' });
  });
  it.each([9007199254740992, new Date(), undefined, { nested: 'value' }])('rejeita valores inexatos/não representáveis na origem', value => {
    expect(() => projectCall({ FixtureId: value,CallText: 'A' },{},fixturePlan)).toThrow();
  });
  it.each(['9223372036854775808','1.0','01','-0'])('não converte IDs BIGINT inválidos %s', value => {
    const result = projectCall({ FixtureId: value,CallText: '001' },{},fixturePlan);
    expect(result.status).toBe('pending'); expect(result.row).toBeUndefined();
  });
  it('identidade sem prova impede qualquer linha candidata', () => {
    const result = projectCall({ FixtureId: '1',CallText: 'A' },{}, { ...fixturePlan, identity: { ...fixturePlan.identity,approved: false } });
    expect(result.row).toBeUndefined(); expect(result.issues).toContainEqual({ code: 'IDENTITY_UNPROVEN' });
  });
  it.each(['2026-10-15T12:30:00.1234567','2026-10-15T12:30:00.123456-03:00','2026-02-30T12:30:00'])('preserva timestamp original e bloqueia projeção temporal sem política %s', value => {
    const result = projectCall({ FixtureId: '1',CallText: 'A',Clock: value },{}, { ...fixturePlan, bindings: [...fixturePlan.bindings,{ target: 'data_ligacao',sourceField: 'Clock' }] });
    expect(result.original.Clock).toBe(value); expect(result.row).toBeUndefined();
    expect(result.issues).toContainEqual({ code: 'TEMPORAL_POLICY_REQUIRED',field: 'data_ligacao' });
  });
  it('timestamp civil com seis dígitos atravessa sem Date ou conversão de fuso', () => {
    const value = '2026-10-15T12:30:00.123456';
    const result = projectCall({ FixtureId: '1',CallText: 'A',Clock: value },{}, { ...fixturePlan,bindings: [...fixturePlan.bindings,{ target: 'data_ligacao',sourceField: 'Clock' }] });
    expect(result.row?.data_ligacao).toBe(value);
  });
  it('referência precisa de autorização e campos externos não são inventados', () => {
    const plan: MappingPlan = { ...fixturePlan,bindings: [...fixturePlan.bindings,{ target: 'hubspot_contact_id',sourceField: 'id',reference: 'crm' }] };
    const input = { FixtureId: '1',CallText: 'A' };
    const blocked = projectCall(input,{ crm: { evidence: 'synthetic CRM',category: 'external',values: { id: '999' } } },plan);
    expect(blocked.row).toBeUndefined(); expect(blocked.fields.hubspot_contact_id.state).toBe('protected');
    const authorized = { ...plan,authorizations: { hubspot_contact_id: 'explicit synthetic ownership approval' } };
    const missing = projectCall(input,{},authorized);
    expect(missing.issues).toContainEqual({ code: 'REFERENCE_UNRESOLVED',field: 'hubspot_contact_id' });
    const resolved = projectCall(input,{ crm: { evidence: 'synthetic CRM',category: 'external',values: { id: '0009' } } },authorized);
    expect(resolved.row?.hubspot_contact_id).toBe('0009');
    expect(resolved.row?.cpf).toBeUndefined(); expect(resolved.row?.email).toBeUndefined();
  });

  it('campos calculados exigem regra/autorização explícita; função não normaliza nada', () => {
    const plan: MappingPlan = { ...fixturePlan,bindings: [...fixturePlan.bindings,{ target: 'telefone_normalizado',sourceField: 'value',reference: 'normalizer' }] };
    const input = { FixtureId: '1',CallText: 'A' };
    const refs = { normalizer: { category: 'local_transform' as const,evidence: 'synthetic business rule',values: { value: '+00 synthetic' } } };
    expect(projectCall(input,refs,plan).row).toBeUndefined();
    const approved = projectCall(input,refs,{ ...plan,authorizations: { telefone_normalizado: 'synthetic owner approval' } });
    expect(approved.row?.telefone_normalizado).toBe('+00 synthetic');
    expect(approved.fields.telefone_normalizado).toMatchObject({ origin: 'local_transform',state: 'value' });
    expect(approved.row?.cpf_normalizado).toBeUndefined();
  });
  it('Password não pode entrar no snapshot puro nem com binding desconhecido', () => {
    expect(() => projectCall({ FixtureId: '1',CallText: 'A',Pass_word: 'not-a-real-secret' },{},fixturePlan)).toThrow('SENSITIVE_FIELD');
  });
  it('não renomeia lead_id_origem ou tma_origem implicitamente', () => {
    const result = projectCall({ FixtureId: '1',CallText: 'A',lead_id_origem: '0009',tma_origem: '12000' },{}, { ...fixturePlan,ignoredSourceFields: ['lead_id_origem','tma_origem'] });
    expect(result.row?.lead_id_banco).toBeUndefined(); expect(result.row?.tma_segundos).toBeUndefined();
  });
});

describe('writer raw bloqueado e plano SQL parametrizado', () => {
  const approval: WriteApproval = { ddlEvidence: 'SYNTHETIC SQL PLAN ONLY',identityEvidence: 'SYNTHETIC PK ONLY',contractVersion: 'raw.olos_calls/v1',
    columns: [{ column: 'raw_olos_call_id',evidence: 'test',nullable: false },{ column: 'olos_call_id',evidence: 'test',nullable: true }] };
  it('prepara somente colunas permitidas; valores hostis permanecem parâmetros', () => {
    const value = "001'); DELETE FROM business; --";
    const sql = prepareRawStatement({ raw_olos_call_id: '9007199254740993',olos_call_id: value },approval);
    expect(sql.text).not.toContain(value); expect(sql.values).toEqual(['9007199254740993',value]);
    expect(sql.text).not.toContain('hubspot'); expect(sql.text).not.toContain('kommo');
  });
  it.each(['kommo_event_hash','hubspot_contact_id','lead_id_banco','data_carga','not_a_column'])('não escreve campo sem autorização/tipo %s', key => {
    expect(() => prepareRawStatement({ raw_olos_call_id: '1',[key]: 'x' } as Candidate,approval)).toThrow();
  });
  it('não há liberação de execução raw por contrato sintético ou flag', async () => {
    const writer = new RawCallsWriter(approval);
    const query = vi.fn();
    expect(writer.preflight({ raw_olos_call_id: '1' })).toBe('blocked');
    await expect(writer.writeInTransaction({ query } as unknown as PoolClient,{ deliveryId: 'unused',destinationId: 'unused',requestDigest: 'unused',contractVersion: 'unused',mappingVersion: 'unused',row: {} })).rejects.toThrow('DESTINATION_BLOCKED');
    expect(query).not.toHaveBeenCalled();
  });
});
