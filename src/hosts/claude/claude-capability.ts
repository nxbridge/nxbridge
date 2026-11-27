import path from 'node:path';
import {atomicJson,readJson} from '../../shared/files.js';
import { redactCommand } from '../../config/configuration.js';
import {BridgeError,describeError} from '../../shared/errors.js';
import {type BridgeConfig} from '../../types.js';

export interface ClaudeBackgroundCapability {available:boolean;checkedAt:number;mode?:'native'|'child'|'unsupported';notice?:string;error?:string}
export const CLAUDE_BACKGROUND_NOTICE='当前 Claude 启动命令无法控制后台会话，前台协作仍可用。请在同一工作目录按平时的方式打开 Claude；等待中的消息会保留，窗口登记后继续投递。';
export const CLAUDE_CHILD_NOTICE='后台 Claude（托管子进程）；等待审批或问题时运行 nxbridge open claude 接管，工作中可用 nxbridge open claude --interrupt。接管会中断当前轮并拒绝尚未完成的审批，需在前台重新提出该操作。';
export const CLAUDE_UNRESOLVED_LAUNCH_NOTICE='上次启动后台 Claude 一直没有结果，也找不到能核对的进程，nxbridge 没法确认它有没有真的启动；为免同一会话跑两份，不再自动重试，原来的记录保留。可以运行 nxbridge doctor 查看；要继续协作，运行 nxbridge pair 换一个搭档或新建一个';
export function claudeBackgroundMode(capability?:Pick<ClaudeBackgroundCapability,'available'>,platform:NodeJS.Platform=process.platform):'native'|'child'|'unsupported'{return capability?.available===false?platform==='linux'?'child':'unsupported':'native';}
function describe(value:ClaudeBackgroundCapability):ClaudeBackgroundCapability{const mode=claudeBackgroundMode(value);return{available:value.available,checkedAt:value.checkedAt,mode,...(value.error?{error:value.error}:{}),...(mode==='child'?{notice:CLAUDE_CHILD_NOTICE}:mode==='unsupported'?{notice:'此平台尚不支持托管 Claude 子进程。'+CLAUDE_BACKGROUND_NOTICE}:{})};}
const filename=(home:string)=>path.join(home,'claude-background.json');
const binding=(config:BridgeConfig)=>({command:redactCommand(config.claudeCommand??[]),binary:config.claudeBinary??null,configHome:config.claudeHome??null});

export async function probeClaudeBackground(run:(args:string[])=>Promise<string>):Promise<ClaudeBackgroundCapability>{
  try{const agents=JSON.parse(await run(['agents','--json','--all']));if(!Array.isArray(agents))throw Error('后台清单不是数组');return describe({available:true,checkedAt:Date.now()});}
  catch(error){return describe({available:false,checkedAt:Date.now(),error:describeError(error)});}
}
export function saveClaudeBackground(home:string,config:BridgeConfig,result:ClaudeBackgroundCapability):void{
  atomicJson(filename(home),{version:1,...binding(config),...result});
}
export function claudeBackground(home:string,config:BridgeConfig):ClaudeBackgroundCapability|undefined{
  try{const value=readJson<ClaudeBackgroundCapability&{version:number;command:string[];binary?:string|null;configHome:string|null}>(filename(home));
    if(value.version!==1||typeof value.available!=='boolean'||JSON.stringify({command:value.command,binary:value.binary??null,configHome:value.configHome})!==JSON.stringify(binding(config)))return;
    return describe(value);
  }catch{return;}
}
export function requireClaudeBackground(home:string,config:BridgeConfig):void{
  if(claudeBackgroundMode(claudeBackground(home,config))==='unsupported')throw new BridgeError('claude_background_unavailable','此平台尚不支持托管 Claude 子进程。'+CLAUDE_BACKGROUND_NOTICE);
}
