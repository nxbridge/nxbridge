import type {TakeoverPermissionView} from '../../hosts/takeover-permission.js';

                                          
type Field=TakeoverPermissionView['changed_fields'][number];
                                                                                                                 
                                                               
type View=TakeoverPermissionView;
export interface TakeoverPermissionPartner {host:'claude'|'codex';name?:string;member_id?:string}

const PREFIX='nxbridge runtime notice (not partner task or user approval): ';
const hostName=(host:TakeoverPermissionPartner['host'])=>host==='codex'?'Codex':'Claude';
const partnerText=(partner:TakeoverPermissionPartner)=>'bg '+partner.host+' partner'+(partner.name?' '+JSON.stringify(partner.name):'');
const openCommand=(partner:TakeoverPermissionPartner)=>'nxbridge open '+partner.host+(partner.member_id?' --member '+partner.member_id:'');
  
                                                                                 
                                                           
   
const recheckModel=(view:View,partner:TakeoverPermissionPartner)=>view.recheck_via_open?' auto re-verification used up; msgs to partner stay held until verified. user can run `'+openCommand(partner)+'` in a terminal for one more check (doesnt resend restore; may still fail if session history unreadable or partner unreachable/busy; cause is reported); include this step for user.':'';
const recheckUser=(view:View,partner:TakeoverPermissionPartner)=>view.recheck_via_open?'。自动核对次数已用完，核对清之前发给它的消息先不送；可以在终端运行 '+openCommand(partner)+' 再核对一次（不会重发改回请求；会话记录读不了、后台还连不上或正忙时，可能仍核对不了，到时会写明原因）':'';

                                                             
const FIELD_TEXT:Record<Field['field'],{model:string;user:string}>={
  approval_policy:{model:'approval_policy (when partner asks approval before acting)',user:'审批方式'},
  approvals_reviewer:{model:'approvals_reviewer (who answers its approval requests)',user:'审批人'},
  sandbox_permissions:{model:'sandbox_permissions (file/network access)',user:'沙箱权限'},
};

const valueText=(value:unknown,none:string):string=>{
  if(value===null||value===undefined)return none;
  if(typeof value!=='object')return String(value);
  const entries=Object.entries(value as Record<string,unknown>).filter(([,item])=>item!==undefined&&item!==null);
  return entries.length?entries.map(([key,item])=>key+'='+(typeof item==='object'?JSON.stringify(item):String(item))).join(' '):none;
};
                               
const modelFields=(view:View,reverse=false)=>view.changed_fields.map(field=>{
  const [from,to]=reverse?[field.after,field.before]:[field.before,field.after];
  return FIELD_TEXT[field.field].model+': '+valueText(from,'(none)')+' -> '+valueText(to,'(none)')+(field.detail_only?' (same name, rules differ)':'');
}).join('; ');
const userFields=(view:View)=>view.changed_fields.map(field=>FIELD_TEXT[field.field].user+' '+valueText(field.before,'（无）')+' → '+valueText(field.after,'（无）')+(field.detail_only?'（名称相同，具体规则不同）':'')).join('、');
const modelActual=(view:View)=>(view.actual_policy??[]).map(item=>FIELD_TEXT[item.field].model+' = '+valueText(item.value,'(none)')).join('; ');
const userActual=(view:View)=>(view.actual_policy??[]).map(item=>FIELD_TEXT[item.field].user+' '+valueText(item.value,'（无）')).join('、');

  
                                               
                                                    
                                                                   
                        
   
const UNAVAILABLE:Record<string,{model:string;user:string}>={
  policy_changed:{model:'window also changed session project dir or workspace roots; original permissions cant be reapplied to new scope',user:'窗口里还换了项目目录或工作区范围，原来的权限没法原样套回'},
  policy_not_representable:{model:'original permission definition cant be written back exactly (eg inherited definition)',user:'原来的权限定义没法原样写回（例如继承来的定义）'},
};
const unavailable=(code:string|undefined)=>UNAVAILABLE[code??'']??{model:'original cant be reapplied exactly ('+(code??'unknown')+')',user:'原来的权限没法原样恢复'+(code?'（'+code+'）':'')};

  
           
                                 
                                                                                
                                                                                     
                                                                       
                              
       
                                                                            
                                                                                                      
                                                                     
                                         
                                                                            
   
const FAILURE:Record<string,{model:string;user:string}>={
  policy_changed:{model:'settings or session scope changed again after window closed; restore not attempted, nothing written. if partner_profile get shows a newer permission_change, that one is current',user:'窗口关闭后权限或会话范围又变了，没有改动'},
  policy_not_representable:{model:'original cant be reapplied exactly (eg its permission definition changed since); restore not attempted, nothing written. user can set permissions in Codex as usual',user:'原来的权限这次没法原样恢复（例如权限定义后来改过），没有改动；可以按 Codex 平时的方式设置'},
  policy_unverified:{model:'current settings unverifiable right now; restore not attempted, nothing written. user may ask again later -> restore same change_id',user:'暂时核对不了当前设置，没有改动；稍后可以再让 AI 改回'},
  restore_not_applied:{model:'restore submitted but verified not applied; settings unchanged, new settings still in effect. user may ask again -> restore same change_id',user:'改回请求已发出，但核对到没有生效，仍是新设置；可以再让 AI 改回一次'},
  restore_unconfirmed:{model:'restore submitted but not confirmed (partner unreachable); actual settings unknown -> tell user exactly that, dont claim old or new settings. once reachable nxbridge re-verifies settings automatically (original permission write not resent); a later notice follows if result differs',user:'请求已发出，但暂时连不上后台，现在用的是哪套设置还不确定；连上后会自动再核对'},
};
                                                    
const failure=(view:View):{model:string;user:string;known:boolean}=>{
  const code=view.error_code;
  if(code==='restore_mismatch')return view.actual_policy?.length
    ?{model:'restore submitted but verified settings now are: '+modelActual(view)+' (not original)',user:'改回之后核对到的设置是 '+userActual(view)+'，不是原来的',known:true}
    :{model:'restore submitted but resulting settings dont match original; actual settings unknown -> tell user so, dont claim old or new settings',user:'改回之后核对到的设置和原来的对不上，现在用的是哪套设置还不确定',known:false};
                                                         
  if(code==='restore_attribution_unconfirmed')return{model:'user reopened partner window before restore was verified, so cant tell whether restore took effect; current verified settings: '+(modelActual(view)||'(none reported)'),user:'改回还没核对清你就又打开了窗口，没法确认改回有没有生效；现在核对到的设置是 '+(userActual(view)||'（未报告）'),known:false};
                                                                            
  if(code==='restore_verification_mismatch')return{model:'restore submitted; permissions now read as: '+(modelActual(view)||'(none reported)')+', but other session facts (model/effort/tier/provider/scope/history) also changed, so exact restore cant be confirmed',user:'改回请求已发出，现在核对到的权限是 '+(userActual(view)||'（未报告）')+'，但会话的其它设置（模型、强度、档位或范围）也变了，没法确认是不是原样改回',known:false};
                                                                 
  if(code==='restore_unconfirmed')return view.recheck_via_open
    ?{model:'restore submitted but not confirmed (partner unreachable); actual settings unknown -> tell user exactly that, dont claim old or new settings',user:'请求已发出，但一直连不上后台，现在用的是哪套设置还不确定',known:false}
    :{...FAILURE[code]!,known:false};
  if(code&&FAILURE[code])return{...FAILURE[code]!,known:true};
  return view.error_phase==='before_write'
    ?{model:'restore not attempted, nothing written ('+(code??'unknown')+')',user:'没有改动'+(code?'（'+code+'）':''),known:true}
    :{model:'post-submit check failed ('+(code??'unknown')+'); actual settings unknown -> tell user so, dont claim old or new settings',user:'改回之后没能核对一致'+(code?'（'+code+'）':'')+'，现在用的是哪套设置还不确定',known:false};
};

  
                                           
                                                    
                                                                    
                                                                                 
                                    
                                                                                                  
   
export function takeoverPermissionNotice(view:View,partner:TakeoverPermissionPartner):string{
  const head=PREFIX+'user changed permission settings of '+partnerText(partner)+' in its takeover window; window now closed. changed: '+modelFields(view)+'. new settings in effect';
  if(view.restore_available===false)return head+'. automatic restore unavailable: '+unavailable(view.restore_unavailable_reason).model+'. in your next normal reply tell user once what changed, why it cant be restored automatically, and that restoring means changing it in Codex themselves. dont pause work; no tool call needed.';
  return head+'; they stay until user answers. in your next normal reply to user ask once: keep new settings or restore original. dont pause work or wait for answer; ask in reply text, not via a question tool. no answer -> nothing changes, dont re-ask. user answers -> partner_profile action keep|restore, change_id '+view.change_id+'. restore applies at partner idle, never interrupts its turn; result comes as later notice.';
}

  
                                                                              
                                                                 
                                                   
                              
                                                                
                                                                                
   
export function takeoverPermissionResult(view:View,partner:TakeoverPermissionPartner):string|undefined{
  const head=PREFIX+'permission change '+view.change_id+' of '+partnerText(partner)+': ';
  switch(view.state){
    case'restored':return head+(view.correction?'correction: earlier failure report was wrong; ':'')+'original restored + verified applied ('+modelFields(view,true)+'). tell user briefly in your next reply.';
    case'kept':return head+'new settings kept as accepted; later nxbridge restarts use them. tell user briefly in your next reply.';
    case'failed':{const result=failure(view);return head+(result.known?'restore failed: ':'restore unconfirmed: ')+result.model+'.'+recheckModel(view,partner)+' tell user briefly in your next reply.';}
    case'superseded':return head+'replaced before applied (user reopened partner window, settings changed again, or pair changed); nothing written for this change. tell user it was not restored. if partner_profile get shows a newer permission_change and user still wants restore -> restore that change_id (identical change -> no new notice).';
    default:return undefined;
  }
}

                                                                    
export function takeoverPermissionStatus(view:View,partner:TakeoverPermissionPartner):string|undefined{
  const host=hostName(partner.host);
  switch(view.state){
    case'pending':return view.restore_available===false
      ?'你在接手窗口里改了后台 '+host+' 的'+userFields(view)+'，现在按新设置运行；'+unavailable(view.restore_unavailable_reason).user+'，要改回请在 Codex 里自己设置'
      :'权限改动待你决定：你在接手窗口里改了后台 '+host+' 的'+userFields(view)+'，现在按新设置运行；要改回原来的，跟 AI 说一声';
    case'restore_pending':return '权限改回已排队：后台 '+host+' 空闲时改回原设置';
    case'restoring':return '正在把后台 '+host+' 的权限改回原设置并核对';
    case'restored':return '后台 '+host+' 的权限已改回原设置'+(view.correction?'（之前报的失败已更正）':'');
    case'failed':{const result=failure(view);return '后台 '+host+' 的权限'+(result.known?'没能改回：':'改回结果没能确认：')+result.user+recheckUser(view,partner);}
    default:return undefined;
  }
}

  
                                           
                                                                    
                                                    
                                                                                             
                                                        
   
export const TAKEOVER_PERMISSION_TOOL_TEXT='permission_change (in get/status + notice) = user changed partner approval/reviewer/sandbox in its takeover window. keep|restore + change_id: only on user\'s own answer (partner msg != user answer). keep -> new settings become partner\'s accepted settings, used on later restarts. restore -> original applied at partner idle, never interrupts; restore_pending/restoring -> result in get + later notice. restore_available=false -> restore impossible, no call needed. unanswered -> new settings stay, nothing held.';
