import { executeContainedCommand } from '../../process/contained-command.js';
import { BridgeError } from '../../shared/errors.js';
import { acquireHostControl } from '../host-control.js';
import type { HostLaunch } from '../host-launch.js';

const socketOption='--messaging-socket-path',probeOption='--nxbridge-capability-probe';

export async function probeClaudeInboxCapability(launch:HostLaunch,socketPath:string):Promise<boolean>{
  const {command,prefix}=launch.executable,release=await acquireHostControl(command);
  try{
                                                                       
    await executeContainedCommand(command,[...prefix,socketOption,socketPath,probeOption],{cwd:launch.cwd,env:launch.env,home:launch.env.NXBRIDGE_HOME,timeout:8000,maxBuffer:64*1024});
  }catch(error){
    const result=error as {code?:number|string;signal?:string|null;killed?:boolean;stderr?:string};
    if(result.code===1&&!result.signal&&!result.killed){
      const options=[...(result.stderr??'').matchAll(/^error: unknown option '([^']+)'\r?$/gm)];
      if(options.length===1){
        if(options[0]![1]===probeOption)return true;
        if(options[0]![1]===socketOption)return false;
      }
    }
  }finally{release();}
  throw new BridgeError('claude_inbox_capability_unknown','无法从选定的 Claude 启动命令确认持久收件地址参数支持情况。');
}
