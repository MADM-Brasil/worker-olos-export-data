import {beforeAll,afterAll,describe,it,expect} from 'vitest';
import {mkdtemp,writeFile,rm,chmod,readFile} from 'node:fs/promises';
import {tmpdir} from 'node:os';
import {join} from 'node:path';
import {execFile} from 'node:child_process';
import {promisify} from 'node:util';
import type pg from 'pg';
import {buildAgentCatalog,parseAgentCatalog,sanitizeUsersInMemory,agentMap} from '../src/offline/agents.js';
import {mapOffline,exampleProfile,xmlContacts} from '../src/offline/model.js';
import {updateAgentCatalog} from '../src/offline/agent-store.js';
import {offlineCycle,prepareOffline,offlinePool,type OfflineState,type OfflineInputs} from '../adapters/offline-homologation.js';
const exec=promisify(execFile), catalogs={campaigns:new Map<string,string>(),dispositions:new Map<string,string>()};
const a=(id:unknown,name:unknown)=>({AgentId:id,UserName:name});

describe('global authorized agents, never UserId or application lists',()=>{
  it('uses all AgentIds across catalogs and leaves unknown/invalid agents NULL; does not trust a row name fallback',()=>{
    const agents=agentMap(buildAgentCatalog([a('7','Synthetic A'),a('81','Synthetic B'),a(null,'Invalid'),a('007','Invalid'),a('0','Sentinel'),a('2147483648','Overflow')]));
    expect(agents.size).toBe(2);
    for (const [AgentId,name] of [['7','Synthetic A'],['81','Synthetic B'],['999',null],['007',null],[null,null]] as const) {
      expect(mapOffline({AttemptsRawDataID:'1',AgentId,AgentName:'Unverified name'}, {...catalogs,agents},{...exampleProfile,agentNameField:'AgentName'},'2026-10-01T00:00:00').row.nome_agente).toBe(name);
    }
  });
  it('rejects sensitive/unnecessary fields and conflicting names; sanitizes the full future Users response only in memory',()=>{
    for(const key of ['Password','Email','Login','UserId']) expect(()=>buildAgentCatalog([{...a('7','Synthetic A'),[key]:'DO_NOT_PERSIST'}])).toThrow('UNSAFE_AGENT_CATALOG');
    expect(()=>buildAgentCatalog([a('7','Synthetic A'),a('7','Synthetic B')])).toThrow('AGENT_CATALOG_CONFLICT');
    expect(buildAgentCatalog([a('7','Synthetic A'),a('7',null)]).entries).toEqual([]);
    const raw={UserId:81,AgentId:7,UserName:'Synthetic A',Activated:0,Flag_Agent:0,Email:'discard@example.invalid',Login:'discard'};
    Object.defineProperty(raw,'Password',{enumerable:true,get:()=>{throw Error('Password must never be accessed');}});
    const safe=sanitizeUsersInMemory([raw]);expect(safe.entries).toEqual([a('7','Synthetic A')]);expect(Object.keys(safe.entries[0]!)).toEqual(['AgentId','UserName']);
    expect(mapOffline({AttemptsRawDataID:'1',AgentId:'81'},{...catalogs,agents:agentMap(safe)},exampleProfile,'2026-10-01T00:00:00').row.nome_agente).toBeNull();
    expect(()=>parseAgentCatalog('{"records":[{"AgentId":7,"UserName":"Synthetic A"}],"Password":"discard"}')).toThrow('UNSAFE_AGENT_CATALOG');
  });
  it('canonicalizes snapshot ordering and identical duplicates; preserves XML fragment parsing',()=>{
    const x=buildAgentCatalog([a('81','Synthetic B'),a('7','Synthetic A'),a('7','Synthetic A')]),y=buildAgentCatalog([a('7','Synthetic A'),a('81','Synthetic B')]);
    expect(x.digest).toBe(y.digest);
    expect(xmlContacts('<CPF>00000000191</CPF><Email>synthetic@example.invalid</Email>',exampleProfile)).toMatchObject({email:'synthetic@example.invalid',invalidXml:false});
  });
});

describe('global agents + real worker offline persistence, isolated synthetic 570 only',()=>{
  let dir:string,root:string,state:OfflineState,pool:pg.Pool,admin:pg.Pool,inputs:OfflineInputs;
  const snapshot=buildAgentCatalog([a('7','Synthetic A'),a('81','Synthetic B')]);
  beforeAll(async()=>{
    dir=await mkdtemp(join(tmpdir(),'olos-global-agents-'));await chmod(dir,0o700);root=join(dir,'run');state=await prepareOffline(root,exampleProfile);pool=await offlinePool(state);admin=await offlinePool(state,false);
    const file=async(name:string,body:unknown)=>{const path=join(dir,name);await writeFile(path,JSON.stringify(body),{mode:0o600});return path;};
    const rows=Array.from({length:570},(_,i)=>({AttemptsRawDataID:String(i+1),AgentId:i%3===0?'7':i%3===1?'81':'999',CampaignId:String(i%2+1),DispositionId:'1',StartDate:'2026-10-01T12:00:00.003333',CampaignData:'<A/><B/>'}));
    inputs={input:await file('attempts.json',rows),campaigns:await file('campaign.json',[{CampaignId:'1',Description:'Synthetic C1'},{CampaignId:'2',Description:'Synthetic C2'}]),dispositions:await file('disposition.json',[{DispositionId:'1',Description:'Synthetic D'}]),exportDate:'2026-10-01',expectedCount:570};
    await offlineCycle(root,state,pool,inputs);
  },120000);
  afterAll(async()=>{await pool?.end();await admin?.end();if(state)await exec('docker',['rm','-f','-v',state.container]);if(dir)await rm(dir,{recursive:true,force:true});},30000);
  const business=()=>pool.query('SELECT internal_id_olos::text,cpf,email,telefone,data_ligacao::text,data_carga::text,id_campanha_olos,nome_campanha_olos,id_tabulacao_olos,nome_tabulacao_olos,lead_id_origem,razao_desligamento FROM raw.olos_calls ORDER BY internal_id_olos');
  const control=async()=>({checkpoints:(await pool.query('SELECT * FROM olos_export.checkpoints ORDER BY stream_id')).rows,receipts:(await pool.query('SELECT * FROM olos_export.delivery_receipts ORDER BY delivery_id')).rows,imports:(await pool.query('SELECT * FROM olos_export.offline_imports ORDER BY id')).rows});
  it('enriches historical calls by source AgentId only, preserving 570 identities, every other field, checkpoint and receipts',async()=>{
    const before=(await business()).rows,controls=await control();
    expect((await pool.query('SELECT count(nome_agente)::int n FROM raw.olos_calls')).rows[0].n).toBe(0);
    expect(await updateAgentCatalog(pool,snapshot)).toMatchObject({changed:true,agents:2,matched:380,updated:380,unmatched:190,unverified:0,revision:'1',sqlServerQueries:0});
    expect((await pool.query('SELECT nome_agente,count(*)::int n FROM raw.olos_calls GROUP BY nome_agente ORDER BY nome_agente NULLS LAST')).rows).toEqual([{nome_agente:'Synthetic A',n:190},{nome_agente:'Synthetic B',n:190},{nome_agente:null,n:190}]);
    expect((await business()).rows).toEqual(before);expect(await control()).toEqual(controls);
    expect((await pool.query('SELECT count(*)::int n FROM olos_export.users')).rows[0].n).toBe(0);
  },30000);
  it('catalog replay and original spool replay do not rewrite names or advance source checkpoints',async()=>{
    const before=(await pool.query('SELECT internal_id_olos::text,xmin::text,nome_agente FROM raw.olos_calls ORDER BY internal_id_olos')).rows,controls=await control();
    expect(await updateAgentCatalog(pool,snapshot)).toMatchObject({changed:false,updated:0,revision:'1'});
    const result=await offlineCycle(root,state,pool,inputs);expect(result).toMatchObject({rows:'570',confirmed:570,ingestionCheckpoint:'1'});
    expect((await pool.query('SELECT internal_id_olos::text,xmin::text,nome_agente FROM raw.olos_calls ORDER BY internal_id_olos')).rows).toEqual(before);expect(await control()).toEqual(controls);
  },30000);
  it('refreshes owned names, discovers new agents, and protects an unverified manually changed name',async()=>{
    await admin.query("UPDATE raw.olos_calls SET nome_agente='Unverified edit' WHERE internal_id_olos=1");
    const renamed=buildAgentCatalog([a('7','Synthetic A2'),a('81','Synthetic B'),a('999','Synthetic C')]);
    expect(await updateAgentCatalog(pool,renamed)).toMatchObject({revision:'2',agents:3,updated:379,unverified:1});
    expect((await pool.query('SELECT nome_agente FROM raw.olos_calls WHERE internal_id_olos=1')).rows[0].nome_agente).toBe('Unverified edit');
    expect((await pool.query('SELECT count(*)::int n,count(DISTINCT internal_id_olos)::int unique FROM raw.olos_calls')).rows[0]).toEqual({n:570,unique:570});
    expect((await pool.query('SELECT version FROM olos_export.checkpoints')).rows[0].version).toBe('1');
  },30000);
  it('rolls back catalog and historical names together; rejects overlap with the ingestion lock',async()=>{
    const before=(await pool.query('SELECT * FROM olos_export.offline_agents ORDER BY agent_id')).rows;
    const next=buildAgentCatalog([a('7','Synthetic A3'),a('81','Synthetic B3'),a('999','Synthetic C3')]);
    await admin.query("CREATE FUNCTION raw.agent_test_fail() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'synthetic failure'; END $$");
    await admin.query('CREATE TRIGGER agent_test_fail BEFORE UPDATE ON raw.olos_calls FOR EACH ROW EXECUTE FUNCTION raw.agent_test_fail()');
    await expect(updateAgentCatalog(pool,next)).rejects.toThrow('PERSISTENCE_FAILED');
    expect((await pool.query('SELECT * FROM olos_export.offline_agents ORDER BY agent_id')).rows).toEqual(before);
    expect((await pool.query('SELECT revision::text FROM olos_export.offline_agent_state')).rows[0].revision).toBe('2');
    await admin.query('DROP TRIGGER agent_test_fail ON raw.olos_calls');await admin.query('DROP FUNCTION raw.agent_test_fail()');
    const lock=await pool.connect();try{await lock.query('SELECT pg_advisory_lock(740210,1)');await expect(updateAgentCatalog(pool,next)).rejects.toThrow('BUSY');}finally{await lock.query('SELECT pg_advisory_unlock(740210,1)');lock.release();}
  },30000);
  it('supports sanitized --users without another ingestion checkpoint, and new calls use the persisted global catalog',async()=>{
    const path=join(dir,'sanitized-agents.json');await writeFile(path,JSON.stringify([a('7','Synthetic A2'),a('81','Synthetic B'),a('999','Synthetic C')]),{mode:0o600});
    const replay=await offlineCycle(root,state,pool,{...inputs,users:path});expect(replay.ingestionCheckpoint).toBe('1');expect(replay.rows).toBe('570');
    const checked=await exec('node',['--import','tsx','adapters/offline-homologation.ts','agents-check','--users',path],{env:{...process.env,SYNC_ENABLED:'false'}});
    expect(JSON.parse(checked.stdout)).toMatchObject({ok:true,agents:3,skipped:0,sqlServerQueries:0});expect(checked.stdout).not.toContain('Synthetic');
    const applied=await exec('node',['--import','tsx','adapters/offline-homologation.ts','agents-update','--work-dir',root,'--users',path,'--confirm','APPLY_SANITIZED_GLOBAL_AGENTS'],{env:{...process.env,SYNC_ENABLED:'false'}});
    expect(JSON.parse(applied.stdout)).toMatchObject({ok:true,changed:false,updated:0,revision:'2'});expect(applied.stdout).not.toContain('Synthetic');
    const next=join(dir,'new-call.json');await writeFile(next,JSON.stringify([{AttemptsRawDataID:'571',AgentId:'999'}]),{mode:0o600});
    const result=await offlineCycle(root,state,pool,{...inputs,input:next,expectedCount:1});expect(result).toMatchObject({rows:'571',confirmed:1,ingestionCheckpoint:'2'});
    expect((await pool.query('SELECT nome_agente FROM raw.olos_calls WHERE internal_id_olos=571')).rows[0].nome_agente).toBe('Synthetic C');
    await writeFile(path,JSON.stringify([{...a('7','Synthetic A'),Password:'DO_NOT_ARCHIVE',Email:'private@example.invalid'}]),{mode:0o600});
    await expect(offlineCycle(root,state,pool,{...inputs,users:path})).rejects.toThrow();
    expect((await pool.query('SELECT version FROM olos_export.checkpoints')).rows[0].version).toBe('2');
    const source=await readFile('adapters/offline-homologation.ts','utf8');expect(source).not.toMatch(/from ['"].*(?:tds|mssql|tedious|hubspot|discadora)/i);
    await writeFile(path,JSON.stringify([a('7','Stale catalog')]),{mode:0o600});
    await expect(offlineCycle(root,state,pool,{...inputs,users:path})).rejects.toThrow('AGENT_CATALOG_UPDATE_REQUIRES_APPROVAL');
    const changed=join(dir,'changed-record.json');await writeFile(changed,JSON.stringify([{AttemptsRawDataID:'1',AgentId:'7',Extra:'changed source artifact'}]),{mode:0o600});
    expect(await offlineCycle(root,state,pool,{...inputs,input:changed,expectedCount:1})).toMatchObject({rows:'571',confirmed:1});
    expect((await pool.query('SELECT nome_agente FROM raw.olos_calls WHERE internal_id_olos=1')).rows[0].nome_agente).toBe('Unverified edit');
  },30000);
});
