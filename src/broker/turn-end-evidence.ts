import type {Endpoint,Peer} from '../types.js';
import type {RpcMessage} from '../ipc/rpc.js';

export interface TurnEndStats {
  startedAt:number;covered:boolean;actions:number;workTools:number;lastWorkAt?:number;
  partnerDriven?:boolean;partnerMessageIds?:string[];
}
interface TurnTrace extends TurnEndStats {scope:string;turnId:string;items:Set<string>;completed?:boolean}
export const turnNativeScope=(peer:Peer,endpoint:Endpoint)=>JSON.stringify([peer.sessionId,endpoint.address,endpoint.epoch,endpoint.bindingEpoch,endpoint.pid,endpoint.procStart]);
const plain=new Set(['userMessage','agentMessage','reasoning','functionCallOutput']);
const tools=/^(?:commandExecution|fileChange|webSearch|imageGeneration|imageView|computerCall|sleep|toolSearch|.*ToolCall|functionCall|customToolCall)$/;
const bridge=(item:any)=>['claude','codex'].includes(item?.server??item?.namespace)||/^mcp__(?:claude|codex)__/.test(item?.name??'');

                                             
export class TurnEndEvidence {
  private traces=new Map<string,TurnTrace>();
  private partners=new Map<string,{scope:string;turnId:string;ids:string[]}>();
  observe(peer:Peer,endpoint:Endpoint,message:RpcMessage):void {
    const p=message.params??{},turnId=p.turn?.id??p.turnId,method=message.method;
    let value=this.traces.get(peer.id);
    if(method==='turn/started'&&typeof turnId==='string'){
      if(value?.turnId===turnId&&value.scope===turnNativeScope(peer,endpoint))return;
      value={scope:turnNativeScope(peer,endpoint),turnId,startedAt:Date.now(),covered:true,actions:0,workTools:0,items:new Set()};
      const partner=this.partners.get(peer.id);this.partners.delete(peer.id);
      if(partner?.scope===value.scope&&partner.turnId===turnId){value.partnerDriven=true;value.partnerMessageIds=partner.ids;}
      this.traces.set(peer.id,value);return;
    }
    if(!value||value.scope!==turnNativeScope(peer,endpoint)||turnId&&value.turnId!==turnId||value.completed)return;
    if(method==='error'){value.covered=false;return;}
    if(method==='turn/completed'){value.completed=true;return;}
    if(method!=='item/started'&&method!=='item/completed')return;
    const item=p.item,type=item?.type;
    if(plain.has(type))return;
    if(type==='contextCompaction'){value.covered=false;return;}
    if(!tools.test(type??'')){value.covered=false;return;}
    const id=typeof item.id==='string'?item.id:undefined;
    if(!id){value.covered=false;return;}
    if(!value.items.has(id)){
      value.items.add(id);value.actions++;
      if(!bridge(item))value.workTools++;
    }
    if(!bridge(item))value.lastWorkAt=Date.now();
  }
  partner(peer:Peer,endpoint:Endpoint,turnId:string,messageIds:string[]=[]):void {
    const value=this.traces.get(peer.id);
    const scope=turnNativeScope(peer,endpoint);
    if(value?.scope===scope&&value.turnId===turnId){value.partnerDriven=true;value.partnerMessageIds=messageIds;}
    else this.partners.set(peer.id,{scope,turnId,ids:messageIds});
  }
  sent(peer:Peer,endpoint:Endpoint):void {
    const value=this.traces.get(peer.id);
                                        
    if(value?.scope===turnNativeScope(peer,endpoint)&&!value.completed)value.actions++;
  }
  get(peer:Peer,endpoint:Endpoint,turnId?:string,completed=false):TurnEndStats|undefined {
    const value=this.traces.get(peer.id);
    if(!value||value.scope!==turnNativeScope(peer,endpoint)||turnId&&value.turnId!==turnId||completed&&!value.completed)return;
    return{startedAt:value.startedAt,covered:value.covered,actions:value.actions,workTools:value.workTools,
      ...(value.lastWorkAt!==undefined?{lastWorkAt:value.lastWorkAt}:{}),...(value.partnerDriven?{partnerDriven:true,partnerMessageIds:value.partnerMessageIds}: {})};
  }
  disconnected(address:string,peers:Peer[],endpoint:(peer:Peer)=>Endpoint|undefined):void {
    for(const peer of peers){const current=endpoint(peer),trace=this.traces.get(peer.id);if(current?.address===address){this.partners.delete(peer.id);if(trace)trace.covered=false;}}
  }
}
