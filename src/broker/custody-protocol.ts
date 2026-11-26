import {createHash} from 'node:crypto';
import path from 'node:path';
import {z} from 'zod';
import {ownedSocketDirectory} from '../shared/local-paths.js';
import {BridgeError} from '../shared/errors.js';

export const CUSTODY_PROTOCOL=1;
export const CUSTODY_MANIFEST_ENTRY='broker-custody-v'+CUSTODY_PROTOCOL;
export const CUSTODY_ENV='NXBRIDGE_BROKER_CUSTODY';
export const CUSTODY_LIMIT_MS=45000;
const RELEASE_MARGIN_MS=5000;
export const generationSchema=z.object({pid:z.number().int().positive(),startedAt:z.string().min(1).max(128)});
const updateIdSchema=z.string().regex(/^[0-9a-f]{16}$/);
const buildIdSchema=z.string().regex(/^\d+\.\d+\.\d+(?:[-+][\w.-]+)?-[0-9a-f]{12}$/);
export const custodyPrepareSchema=z.object({protocol:z.literal(CUSTODY_PROTOCOL),expected:generationSchema,updater:generationSchema,buildId:buildIdSchema,startupToken:z.string().uuid()});
const descriptorSchema=custodyPrepareSchema.omit({startupToken:true}).extend({updateId:updateIdSchema,ticket:z.string().regex(/^[0-9a-f]{64}$/),expiresAt:z.number().int().positive()});
export type CustodyDescriptor=z.infer<typeof descriptorSchema>;
export type CustodyGeneration=z.infer<typeof generationSchema>;
export type CustodyResult={used:boolean;transferred:number;held:number};
export const sameGeneration=(a:CustodyGeneration,b:CustodyGeneration)=>a.pid===b.pid&&a.startedAt===b.startedAt;
export const emptyCustodyResult=():CustodyResult=>({used:false,transferred:0,held:0});

export function parseCustodyDescriptor(raw:unknown):CustodyDescriptor{
  try{return descriptorSchema.parse(typeof raw==='string'?JSON.parse(raw):raw);}
  catch{throw new BridgeError('handoff_descriptor_invalid','协调器交接凭据无效。');}
}

export function custodyAddress(home:string,updateId:string):string{
  return process.platform==='win32'
    ?'\\\\.\\pipe\\nxb-h-'+createHash('sha256').update(home).digest('hex').slice(0,16)+'-'+updateId
    :path.join(ownedSocketDirectory(home),'h-'+updateId+'.sock');
}

export function custodyDeadlines(expiresAt:number){
  const remaining=Math.min(CUSTODY_LIMIT_MS,expiresAt-Date.now()),deadline=performance.now()+remaining;
  return{deadline,acquisitionDeadline:deadline-RELEASE_MARGIN_MS};
}
