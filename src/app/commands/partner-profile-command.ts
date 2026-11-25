import { z } from 'zod';
import { administrativeTarget } from '../../broker/pairing/admin-target.js';
import { brokerCall } from '../../ipc/ipc.js';
import {BridgeError} from '../../shared/errors.js';
import {type Host,type ModelProfile} from '../../types.js';
import { hostLabel } from '../../broker/pairing/pair-presentation.js';

export async function partnerProfileCommand(args:string[]):Promise<unknown>{
  const host=z.enum(['codex','claude']).parse(args[0]),target:string[]=[],profile:ModelProfile={};let reset=false;
  for(let index=1;index<args.length;index++){
    const flag=args[index]!;
    if(flag==='--json')continue;
    if(flag==='--reset'){if(reset)throw new BridgeError('invalid_arguments','--reset 不能重复。');reset=true;continue;}
    const value=args[++index];if(!value||value.startsWith('--'))throw new BridgeError('invalid_arguments','参数缺少值：'+flag);
    if(flag==='--model'||flag==='--effort'||flag==='--service-tier'){
      const field=flag==='--model'?'model':flag==='--effort'?'effort':'serviceTier';
      if(field==='serviceTier'&&(host!=='codex'||!['standard','fast','flex'].includes(value)))throw new BridgeError('invalid_arguments','--service-tier 仅用于 Codex，取值为 standard、fast 或 flex。');
      if(profile[field]!==undefined)throw new BridgeError('invalid_arguments','参数不能重复：'+flag);profile[field]=value;}
    else if(['--pair','--member','--session'].includes(flag))target.push(flag,value);
    else throw new BridgeError('invalid_arguments','未知参数：'+flag);
  }
  if(reset&&Object.keys(profile).length)throw new BridgeError('invalid_arguments','--reset 不能与 --model、--effort 或 --service-tier 同用。');
  return brokerCall('partner.profile',{...await administrativeTarget(target),host,...profile,action:reset?'reset':Object.keys(profile).length?'set':'get'},{mutation:reset||Object.keys(profile).length>0});
}

type DisplayProfile=Omit<ModelProfile,'serviceTier'>&{serviceTier?:string|null};
interface ProfileView {host:Host;effective:DisplayProfile;requested?:DisplayProfile;status:'pending'|'applied'|'failed'|'host';delivery_held?:boolean;source?:'defaults'|'explicit';sources?:Record<string,string>;session_id?:string|null;error?:{code:string;message:string}}
const sourceLabels:Record<string,string>={recommended:'nxbridge 推荐',config:'nxbridge 配置',explicit:'此搭档单独指定',host:'宿主配置',frozen:'冻结'};
const describe=(profile:DisplayProfile|undefined,sources?:Record<string,string>)=>[['model','模型'],['effort','推理强度'],['serviceTier','服务档位']].flatMap(([key,label])=>{
  const value=profile?.[key as keyof DisplayProfile];if(value===undefined)return[];
  const source=sourceLabels[sources?.[key!]??''];return[`${label} ${value??'未指定'}${source?'（'+source+'）':''}`];
}).join('，');
const reason=(view:ProfileView)=>(view.error?.message??'原因未知').trim().replace(/[。.]+$/,'');
                          
export function formatPartnerProfile(view:ProfileView):string{
  const current=describe(view.effective,view.status==='applied'&&!view.delivery_held?view.sources:undefined)||'搭档尚未报告生效设置',requested=describe(view.requested,view.sources);
  const state=view.status==='host'?'没有单独指定，按默认设置。':!requested?'没有单独指定；当前会话保持上面的设置。'
    :view.status==='applied'&&view.delivery_held?`正在重新核对${requested}是否仍然生效；核对完之前，新消息先不发给它。一直停在这里时，可以运行 nxbridge partner-profile ${view.host} --reset 放弃这次选择。`
    :view.status==='applied'?(view.source==='explicit'?'这是为它指定的设置，已生效。':'按默认设置，已生效。')
    :view.status==='pending'?(view.session_id?`已记下${requested}，正在等它生效并核对（它正在工作时会等这一轮做完）${view.delivery_held?`；在这之前，新消息先不发给它。一直停在这里时，可以运行 nxbridge partner-profile ${view.host} --reset 放弃这次选择`:''}。`:`已记下${requested}，它启动时就用这个设置。`)
    :view.delivery_held?`上次修改（${requested}）结果不明，或者实际设置和指定的不一致：${reason(view)}。核对清楚之前，新消息先不发给它；要放弃这次修改，运行 nxbridge partner-profile ${view.host} --reset。`
    :`修改（${requested}）没有成功：${reason(view)}。它继续用原来的设置。`;
  return `后台 ${hostLabel(view.host)}：${current}。${state?'\n'+state:''}\n`;
}
