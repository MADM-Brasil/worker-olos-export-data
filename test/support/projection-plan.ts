import type { MappingPlan } from '../../src/projection/transform.js';

export const fixturePlan: MappingPlan = {
  version: 'fixture-map-v1',approvalEvidence: 'SYNTHETIC TEST ONLY',
  identity: { approved: true,evidence: 'FixtureId maps one-to-one to synthetic target PK',granularityEvidence: 'one synthetic call',sourceKeyFields: ['FixtureId'] },
  requiredFields: ['raw_olos_call_id','olos_call_id'],ignoredSourceFields: ['Clock','Internal'],authorizations: {},
  bindings: [{ target: 'raw_olos_call_id',sourceField: 'FixtureId' },{ target: 'olos_call_id',sourceField: 'CallText' }],
};
