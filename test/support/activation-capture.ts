import {Collation} from 'tedious/lib/collation.js';
import {attemptsFixtureFields} from '../../src/integration/attempts/contracts.js';
import {parserLimits} from '../../src/olos/tds/raw-parser.js';
export const collation=new Collation(1033,0,0,0),u16=(n:number)=>{const b=Buffer.alloc(2);b.writeUInt16LE(n);return b;};
export const fields=attemptsFixtureFields(collation.toBuffer().toString('hex')).map(f=>f.sqlLength==='max'?{...f,maxBytes:parserLimits.maxCellBytes}:f);
const columns=Buffer.concat([Buffer.from([0x81]),u16(fields.length),...fields.map(f=>Buffer.concat([Buffer.alloc(4),u16(f.nullable?1:0),
 f.type==='int'?Buffer.from(f.nullable?[0x26,4]:[0x38]):f.type==='datetime'?Buffer.from([0x6f,8]):Buffer.concat([Buffer.from([0xa7]),u16(f.sqlLength==='max'?65535:f.sqlLength as number),collation.toBuffer()]),
 Buffer.from([f.name.length]),Buffer.from(f.name,'utf16le')]))]);
function row(id:number){const key=Buffer.alloc(4);key.writeInt32LE(id);return Buffer.concat([Buffer.from([0xd1]),...fields.map(f=>f.name==='AttemptsRawDataID'?key:f.type==='int' || f.type==='datetime'?Buffer.from([0]):f.sqlLength==='max'?Buffer.alloc(8,255):u16(65535))]);}
function out(name:string){return Buffer.concat([Buffer.from([0xac]),u16(0),Buffer.from([name.length]),Buffer.from(name,'utf16le'),Buffer.from([1]),Buffer.alloc(4),u16(1),Buffer.from([0xa7]),u16(4000),collation.toBuffer(),u16(65535)]);}
export const responseForIds=(ids:readonly number[])=>Buffer.concat([columns,...ids.map(row),out('Message'),out('ErrorMessage'),Buffer.from([0x79,0,0,0,0,0xfe]),Buffer.alloc(12)]);
export const response=responseForIds([1]);
