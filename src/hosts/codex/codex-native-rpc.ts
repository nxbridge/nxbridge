import { RpcClient, type RpcMessage, type RpcRequestOptions } from '../../ipc/rpc.js';
import { BridgeError, RuntimeRecoveryHold } from '../../shared/errors.js';
import { loadedCodexThreads } from './codex-restart.js';

interface Waiting {nextAt:number;checking:boolean;wedged:boolean;pending:Set<object>}

                                               
function controlThread(method:string,params:unknown):string|undefined{
  const id=(params as {threadId?:unknown}|undefined)?.threadId;
  return typeof id==='string'&&/^(?:turn\/(?:start|steer|interrupt|settings\/update)|thread\/(?:read|resume|unsubscribe|delete|inject_items|name\/set|settings\/update|queue\/[^/]+|backgroundTerminals\/[^/]+))$/.test(method)?id:undefined;
}

export class CodexNativeRpc extends RpcClient {
  private waiting=new Map<string,Waiting>();
  threadGuard?: (method:string,id:string,options:RpcRequestOptions)=>object|void;

  constructor(send:(message:RpcMessage)=>void,close:()=>void,endpoint:string,private now=()=>performance.now()){
    super(send,close,endpoint);
    this.on('notification',(message:RpcMessage)=>{
      const id=message.params?.threadId;
      if(typeof id==='string'&&(['thread/unloaded','thread/closed','thread/archived'].includes(message.method??'')
        ||message.method==='thread/status/changed'&&message.params?.status?.type==='notLoaded'))this.waiting.delete(id);
    });
  }

                                           
  confirmThreadAbsent(id:string):void{this.waiting.delete(id);this.emit('threadAbsent',id);}

  private hold(state:Pick<Waiting,'wedged'>):RuntimeRecoveryHold{
    return state.wedged
      ?new RuntimeRecoveryHold('native_thread_wedged','后台 Codex 的这个会话卡住了：进程还在，但不再响应这个会话的请求。消息已保留。nxbridge 无法确认它是否还在干活，所以不会自动结束它，也暂时不能安全重启；你确认它没在工作后，可以让 Claude 帮你结束这个后台 Codex，再按原会话恢复。')
      :new RuntimeRecoveryHold('native_thread_unconfirmed','发给后台 Codex 的请求还没得到确认；消息已保留，正在重新确认。');
  }

  private async independentEvidence(id:string,state:Waiting):Promise<void>{
    try{
      const [loaded,turns]=await Promise.allSettled([
        loadedCodexThreads(this),
        super.request('thread/turns/list',{threadId:id,limit:1,itemsView:'summary'},{timeout:1500}),
      ]);
      if(this.waiting.get(id)!==state||loaded.status!=='fulfilled')return;
      if(!loaded.value.has(id)){this.confirmThreadAbsent(id);return;}
      if(turns.status==='fulfilled'&&Array.isArray(turns.value.data))state.wedged=true;
    }catch{                                }
  }

  override async request<T=any>(method:string,params:unknown={},options:RpcRequestOptions={}):Promise<T>{
    const id=controlThread(method,params);
    if(!id||this.closed)return super.request<T>(method,params,options);
    const permit=this.threadGuard?.(method,id,options);
    let state=this.waiting.get(id);
    if(state){
      if(!state.checking&&this.now()>=state.nextAt){
        state.checking=true;
                                               
        try{await this.independentEvidence(id,state);}
        finally{state.checking=false;state.nextAt=this.now()+30000;}
      }
      throw this.hold(state);
    }
    const evidence={};
    const onLateSuccess=permit?()=>{
      try{if(this.closed||this.threadGuard?.(method,id,options)!==permit)return;}catch{return;}
      const current=this.waiting.get(id);
      if(current?.pending.delete(evidence)&&!current.pending.size)this.waiting.delete(id);
    }:undefined;
    try{return await super.request<T>(method,params,{...options,onLateSuccess});}
    catch(error){
      if((error as BridgeError).code!=='timeout')throw error;
                                           
      state=this.waiting.get(id);
      if(!state){
        state={nextAt:this.now()+30000,checking:true,wedged:false,pending:new Set()};
        this.waiting.set(id,state);
        const current=state;
        void this.independentEvidence(id,current).finally(()=>{current.checking=false;});
      }
      state.pending.add(evidence);
                           
      if(options.mutation)throw error;
      throw this.hold(state);
    }
  }

  override disconnected(reason?:string,code?:string):void{this.waiting.clear();super.disconnected(reason,code);}
}
