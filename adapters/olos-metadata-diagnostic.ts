import {resolve} from 'node:path';
import {pathToFileURL} from 'node:url';
import {privateJson} from './disposition-integration.js';
import {diagnoseOlosMetadata,metadataErrorDetails,metadataFailure,type MetadataAction} from '../src/diagnostics/olos-metadata.js';

export async function runMetadataCli(args:readonly string[],env:NodeJS.ProcessEnv):Promise<number>{
 try{
   const [action,...rest]=args,flags=new Map<string,string>();
   if(!['connect','describe','catalog'].includes(action??'') || env.SYNC_ENABLED!=='false' || env.NODE_TLS_REJECT_UNAUTHORIZED==='0')throw Error('METADATA_COMMAND_REJECTED');
   for(let i=0;i<rest.length;i+=2){const key=rest[i],value=rest[i+1];if(!key || !value || !['--config','--authorize','--confirm'].includes(key) || flags.has(key))throw Error('METADATA_COMMAND_REJECTED');flags.set(key,value);}
   if(flags.get('--authorize')!=='OLOS_NON_CONSUMPTIVE_METADATA' || !flags.get('--config'))throw Error('METADATA_AUTHORIZATION_REQUIRED');
   let config:unknown;try{config=await privateJson(flags.get('--config')!);}catch(error){throw metadataFailure(error,'configuration');}
   const result=await diagnoseOlosMetadata(config,action as MetadataAction,flags.get('--confirm'));
   process.stdout.write(`${JSON.stringify(result)}\n`);
   return ['METADATA_DESCRIBED','PIN_VERIFIED','INSPECTED_UNVERIFIED','CATALOG_CANDIDATES_VISIBLE'].includes(result.code)?0:
     ['CATALOG_OBJECT_NOT_VISIBLE_OR_ABSENT','CATALOG_COLUMNS_NOT_VISIBLE'].includes(result.code)?2:1;
 }catch(error){
   process.stderr.write(`${JSON.stringify({...metadataErrorDetails(error),acquisitionAuthorized:false,retryRemote:false})}\n`);return 1;
 }
}
if(process.argv[1] && import.meta.url===pathToFileURL(resolve(process.argv[1])).href){void runMetadataCli(process.argv.slice(2),process.env).then(code=>{process.exitCode=code;});}
