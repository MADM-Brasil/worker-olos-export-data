import {expect,it,vi} from 'vitest';
import {PostgresLargeBatchPersistence} from '../src/persistence/large-batch.js';
import {spoolLimits} from '../src/large-batch/config.js';
import {attemptsFixturePlan} from '../src/integration/attempts/contracts.js';
import {corporateErrorCode} from '../adapters/attempts-corporate.js';

it.each(['SOURCE_CONTRACT_MISSING','SOURCE_CONTRACT_NOT_APPROVED','SOURCE_CONTRACT_MISMATCH','PERSISTENCE_CONNECTION_READ_ONLY','PERSISTENCE_ROLE_PRIVILEGES_MISSING'])(
 'preflight explains %s while preserving the existing persistence failure contract',async reason=>{
  const plan=attemptsFixturePlan('2026-10-19','0000000000');
  const client={release:vi.fn(),query:vi.fn(async(sql:string)=>{
   if(sql.includes('schema_versions'))return {rows:[{version:1}]};
   if(sql.includes('transaction_read_only'))return {rows:[{value:reason==='PERSISTENCE_CONNECTION_READ_ONLY'?'on':'off'}]};
   if(sql.includes('source_contracts'))return {rows:reason==='SOURCE_CONTRACT_MISSING'?[]:[{...plan.contract,
    ...(reason==='SOURCE_CONTRACT_NOT_APPROVED'?{approved:false}:{}),...(reason==='SOURCE_CONTRACT_MISMATCH'?{layout_evidence:'different verified layout'}:{})}]};
   if(sql.includes('has_table_privilege'))return {rows:[{allowed:reason!=='PERSISTENCE_ROLE_PRIVILEGES_MISSING'}]};
   return {rows:[]};
  })};
  const pool={connect:async()=>client};
  try{await new PostgresLargeBatchPersistence(pool as never,spoolLimits()).preflight(plan);throw Error('EXPECTED_REJECTION');}
  catch(error){expect(error).toMatchObject({code:'CONTRACT_UNCONFIRMED',diagnosticCode:reason});expect(corporateErrorCode(error)).toBe(reason);}
  expect(client.query).toHaveBeenCalledWith('ROLLBACK');expect(client.release).toHaveBeenCalled();
 });
