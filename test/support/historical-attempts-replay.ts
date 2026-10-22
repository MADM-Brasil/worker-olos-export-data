import {createHash} from 'node:crypto';
import {join} from 'node:path';
import {Collation} from 'tedious/lib/collation.js';
import {privateRoot,privateText} from '../../adapters/offline-homologation.js';
import {parseLossless,recordsFromJson,profileSchema,catalog} from '../../src/offline/model.js';
import {attemptsFixturePlan,attemptsFixtureFields,signature} from '../../src/integration/attempts/contracts.js';
import {APPROVED_COLUMNS} from '../../src/offline/tds-writer.js';
import {parserLimits} from '../../src/olos/tds/raw-parser.js';
import {replayTdsResponse} from '../../src/olos/tds/source.js';
import type {LargePlan} from '../../src/large-batch/types.js';
const EXPECTED_SHA='4ed44db3a4d08728f53f975345d929a1b4e2f28e7a9c7df20ea6fd3c99b553ca';
const u16=(n:number)=>{const b=Buffer.alloc(2);b.writeUInt16LE(n);return b;};
/** Test-only reconstruction. No original TDS, TLS, collation or tick precision is claimed. */
export async function historicalAttemptsReplay(directory:string,date:string){
 await privateRoot(directory);const input=await privateText(join(directory,'attempts.json'));
 if(createHash('sha256').update(input).digest('hex')!==EXPECTED_SHA)throw Error('HISTORICAL_INPUT_DIGEST_MISMATCH');
 const rows=recordsFromJson(parseLossless(input)),ids=rows.map(r=>BigInt(String(r.AttemptsRawDataID)));
 if(rows.length!==570 || new Set(ids).size!==570 || ids.reduce((a,b)=>a<b?a:b)!==37150205n || ids.reduce((a,b)=>a>b?a:b)!==37150774n)throw Error('HISTORICAL_INPUT_SCOPE_MISMATCH');
 const profile=profileSchema.parse(JSON.parse(await privateText(join(directory,'profile-16-columns.json'))));
 if(JSON.stringify(profile.columns)!==JSON.stringify(APPROVED_COLUMNS) || profile.tmaPolicy!=='disabled' || profile.phoneMatchKey!=='normalized')throw Error('HISTORICAL_PROFILE_MISMATCH');
 const catalogs={campaigns:catalog(recordsFromJson(parseLossless(await privateText(join(directory,'campaigns.json')))),'CampaignId','Description'),
  dispositions:catalog(recordsFromJson(parseLossless(await privateText(join(directory,'dispositions.json')))),'DispositionId','Description'),agents:new Map<string,string>()};
 const collation=new Collation(1033,0,0,0),hex=collation.toBuffer().toString('hex');
 const fields=attemptsFixtureFields(hex).map(f=>f.sqlLength==='max'?{...f,maxBytes:parserLimits.maxCellBytes}:f);
 const plan:LargePlan={...attemptsFixturePlan(date,hex),fields:fields.map(({sqlLength:_s,collationHex:_c,unicodeMetadataPolicy:_u,...f})=>f) as LargePlan['fields'],
  signature:signature('ReReadData'),parameters:{TableName:'AttemptsRawData',DateExport:date,PKIDStart:'37150205',PKIDEnd:'37150774'},
  run:{table:'AttemptsRawData',contractVersion:'controlled-attempts-fixture/v1',readType:'reread',exportDate:date,pkRange:{start:'37150205',end:'37150774'}}};
 const reverse=new Map<string,number>();for(let n=0;n<256;n++)reverse.set(new TextDecoder('windows-1252').decode(Buffer.from([n])),n);
 let temporalValues=0;
 const bodies=rows.map(row=>fields.map(f=>{
  const value=row[f.name];if(value===null)return null;if(typeof value!=='string')throw Error('HISTORICAL_FIELD_TYPE_MISMATCH');
  if(f.type==='int'){if(!/^-?\d+$/.test(value))throw Error('HISTORICAL_INTEGER_INVALID');const b=Buffer.alloc(4);b.writeInt32LE(Number(value));return b;}
  if(f.type==='datetime'){
   const m=/^(\d{4}-\d{2}-\d{2})[T ](\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,7}))?Z?$/.exec(value);if(!m)throw Error('HISTORICAL_TEMPORAL_FORMAT_UNSUPPORTED');
   const day=(Date.parse(m[1]+'T00:00:00Z')-Date.UTC(1900,0,1))/86400000;
   const micros=(Number(m[2])*3600+Number(m[3])*60+Number(m[4]))*1e6+Number((m[5]??'').padEnd(7,'0'))/10;
   const ticks=Math.round(micros*300/1e6);const b=Buffer.alloc(8);b.writeInt32LE(day+Math.floor(ticks/25920000));b.writeUInt32LE(ticks%25920000,4);temporalValues++;return b;
  }
  const out=[];for(const char of value){const n=reverse.get(char);if(n===undefined)throw Error('HISTORICAL_TEXT_NOT_REPRESENTABLE_IN_TEST_COLLATION');out.push(n);}return Buffer.from(out);
 }));
 if(rows.some(r=>Object.keys(r).length!==18 || fields.some(f=>!Object.hasOwn(r,f.name))))throw Error('HISTORICAL_LAYOUT_MISMATCH');
 const metadata=Buffer.concat([Buffer.from([0x81]),u16(fields.length),...fields.map(f=>Buffer.concat([Buffer.alloc(4),u16(f.nullable?1:0),
  f.type==='int'?Buffer.from(f.nullable?[0x26,4]:[0x38]):f.type==='datetime'?Buffer.from([0x6f,8]):Buffer.concat([Buffer.from([0xa7]),u16(f.sqlLength==='max'?65535:f.sqlLength as number),collation.toBuffer()]),Buffer.from([f.name.length]),Buffer.from(f.name,'utf16le')]))]);
 const encodedRows=bodies.map(values=>Buffer.concat([Buffer.from([0xd1]),...values.map((body,i)=>{
  const f=fields[i]!;if(f.type==='int' && !f.nullable)return body!;
  if(f.type==='int' || f.type==='datetime')return Buffer.concat([Buffer.from([body?.length??0]),body??Buffer.alloc(0)]);
  if(f.sqlLength==='max'){if(body===null)return Buffer.alloc(8,255);const total=Buffer.alloc(8),len=Buffer.alloc(4);total.writeBigUInt64LE(BigInt(body.length));len.writeUInt32LE(body.length);return body.length?Buffer.concat([total,len,body,Buffer.alloc(4)]):Buffer.concat([total,Buffer.alloc(4)]);}
  return Buffer.concat([u16(body?.length??65535),body??Buffer.alloc(0)]);
 })]));
 const output=(name:string)=>Buffer.concat([Buffer.from([0xac]),u16(0),Buffer.from([name.length]),Buffer.from(name,'utf16le'),Buffer.from([1]),Buffer.alloc(4),u16(1),Buffer.from([0xa7]),u16(4000),collation.toBuffer(),u16(65535)]);
 const bytes=Buffer.concat([metadata,...encodedRows,output('Message'),output('ErrorMessage'),Buffer.from([0x79,0,0,0,0,0xfe]),Buffer.alloc(12)]);
 const source=()=>replayTdsResponse(fields,async function*(){yield bytes;},570);
 return {rows,bodies,fields,plan,profile,catalogs,source,sha256:EXPECTED_SHA,temporalValues};
}
