import { z } from 'zod';
import { nativeHealthSchema } from '../../broker/health/foreground-health.js';
import { nativeAutoModeSchema, nativeBackgroundSchema } from '../../hosts/claude/claude-auto-mode.js';
import {BridgeError} from '../../shared/errors.js';

export interface ModContext {sessionId:string;binding?:unknown}

                                                           
export async function modInput(argument:string|undefined,stdin:AsyncIterable<Uint8Array>=process.stdin):Promise<unknown>{
  if(argument!=='-')return JSON.parse(argument??'{}');
  const chunks:Uint8Array[]=[];let bytes=0;
  for await(const chunk of stdin){bytes+=chunk.byteLength;if(bytes>2*1024*1024)throw new BridgeError('input_too_large','Mod 输入超过限制。');chunks.push(chunk);}
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}

export const nativeInboundSchema=z.object({policy:z.enum(['accept','hold','refuse','default','unknown']),source:z.enum(['policy','flag','user','project','local','invalid','default']).optional(),dialogExpiry:z.string().max(20).optional(),dialogTimeoutMs:z.number().int().positive().optional(),observedAt:z.number().finite().nonnegative()});
                                             
const profileValues=z.object({model:z.string().min(1).max(200).optional(),effort:z.union([z.string().min(1).max(40),z.number().finite()]).optional()}).strict();
export const nativeProfileSchema=z.object({model:z.string().min(1).max(200).optional(),flag:profileValues.optional(),effective:profileValues.optional(),effortOverride:z.string().min(1).max(40).optional(),runtimeEffort:profileValues.shape.effort.nullable(),effortUnverified:z.boolean().optional(),observedAt:z.number().finite().nonnegative()}).strict();
export type NativeProfile=z.infer<typeof nativeProfileSchema>;
export const nativeProfileEffort=(profile:NativeProfile)=>profile.effortOverride??(profile.effortUnverified?undefined:profile.runtimeEffort!==undefined?profile.runtimeEffort??undefined:profile.effective?.effort);
export const nativeDecisionSchema=z.object({bash:z.enum(['allow','ask','deny']),rule:z.string().max(200).optional(),observedAt:z.number().finite().nonnegative()});
export const nativeSessionSchema=z.object({transcriptPath:z.string().min(1).max(4096).optional(),interactive:z.boolean().optional()});
export type NativeDecision=z.infer<typeof nativeDecisionSchema>;

const turnId=z.string().min(1).max(256),at=z.number().finite().nonnegative(),count=z.number().int().safe().nonnegative();
export const claudeTurnStatsSchema=z.object({startedAt:at,covered:z.boolean(),actions:count,workTools:count,lastWorkAt:at.optional(),
  partnerDriven:z.boolean().optional(),partnerMessageIds:z.array(z.string().min(1).max(128)).max(16).optional()}).strict();
export const claudeTurnBackgroundSchema=z.object({tasks:count.optional(),crons:count.optional()}).strict();
export const claudeTurnStopSchema=z.object({turn_id:turnId,stop_hook_active:z.boolean(),stats:claudeTurnStatsSchema,background:claudeTurnBackgroundSchema,deadline_at:at.optional()}).strict();
export const claudeTurnCompletedSchema=z.object({turn_id:turnId,completed_at:at,stats:claudeTurnStatsSchema,background:claudeTurnBackgroundSchema,
  interventions:z.array(z.object({id:z.string().min(1).max(128),inert:z.boolean()}).strict()).max(16)}).strict();
export const claudeHumanPromptSchema=z.object({human_origin:z.enum(['composer','desktop','vscode']),human_turn_id:turnId,human_input_at:at}).strict();

                                            
export const modContextSchema=z.object({
  sessionId:z.string(),inbox:z.string().min(1),cwd:z.string().optional(),
  binding:z.object({key:z.string(),epoch:z.string()}).optional(),contextStart:z.boolean().optional(),
  contextReason:z.enum(['reset','switch','fork','unknown']).optional(),
  nativePermission:z.object({mode:z.string(),observedAt:z.number().finite().nonnegative()}).optional(),
  nativeDecision:nativeDecisionSchema.optional(),
  nativeInbound:nativeInboundSchema.optional(),
  nativeProfile:nativeProfileSchema.optional(),
  nativeAutoMode:nativeAutoModeSchema.optional(),
  nativeBackground:nativeBackgroundSchema.optional(),
  nativeHealth:nativeHealthSchema.optional(),
  nativeSession:nativeSessionSchema.optional(),
});

                                        
export function modResponse(input:{contextStart?:unknown;binding?:unknown},value:unknown,context:ModContext):unknown{
  return Object.hasOwn(input,'contextStart')||input.binding!==undefined?{value,context}:value;
}

                                                   
export function modMonitorAdmission(input:{operation:string;mod_revision?:unknown}):boolean{
  return input.operation==='monitor-prepare'||typeof input.mod_revision!=='string'||!/^[a-f0-9]{16}$/.test(input.mod_revision);
}
