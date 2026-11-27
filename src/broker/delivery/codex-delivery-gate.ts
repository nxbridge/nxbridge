import type {Store} from '../../storage/store.js';
import type {Endpoint,Peer} from '../../types.js';
import {hostingHold} from '../recovery/session-hosting.js';
import {codexDeliveryHoldText,type CodexDeliveryHoldCode} from './codex-delivery-hold-text.js';

export interface CodexDeliveryHold {code:CodexDeliveryHoldCode;status:string;model:string;reason?:string}
interface DeliveryHooks {
  paused():boolean;
  enabled(id:string):boolean;
  activationHold(id:string):CodexDeliveryHoldCode|undefined;
  subscribed(peer:Peer,endpoint:Endpoint):boolean;
  profileHeld(peer:Peer):boolean;
  profilePending(peer:Peer):boolean;
  takingOver(id:string):boolean;
  subscriptionHold(id:string):Pick<CodexDeliveryHold,'code'|'reason'>|undefined;
  childRunning(peer:Peer):boolean;
}

                                                
export class CodexDeliveryGate {
  constructor(private store:Store,private hooks:DeliveryHooks){}
  hold(peer:Peer,expectedEndpoint?:Endpoint,expectedPairId?:string):CodexDeliveryHold|undefined {
    if(peer.host!=='codex')return;
    const held=(code:CodexDeliveryHoldCode):CodexDeliveryHold=>({code,...codexDeliveryHoldText(code)});
    const subscription=()=>{const hold=this.hooks.subscriptionHold(peer.id);return hold&&{...held(hold.code),...(hold.reason?{reason:hold.reason}:{})};};
    if(this.hooks.paused())return held('runtime_unavailable');
    const current=this.store.peer(peer.id),pair=this.store.pairFor(peer.id),endpoint=this.store.endpoint(peer.id);
    if(!pair)return held('runtime_unavailable');
    if(pair.paused)return held('pair_paused');
    if(current.state==='paused')return held(current.pauseReason==='restart_incomplete'?'restart_incomplete':'peer_paused');
    if(!this.hooks.enabled(pair.claude)||!this.hooks.enabled(pair.codex))return held('disabled');
    if(current.sessionId!==peer.sessionId||expectedPairId&&pair.id!==expectedPairId||
      expectedEndpoint&&(!endpoint||endpoint.epoch!==expectedEndpoint.epoch||endpoint.address!==expectedEndpoint.address||endpoint.bindingEpoch!==expectedEndpoint.bindingEpoch))
      return held('context_transition');
    const activation=this.hooks.activationHold(peer.id);if(activation)return held(activation);
    const hosting=hostingHold(this.store,current);if(hosting)return held(hosting);
    if(!endpoint?.active)return subscription()??held('runtime_unavailable');
    if(endpoint.pendingContext||this.store.bindings.current(peer.id)?.transitioning)return held('context_transition');
    if(this.hooks.profileHeld(current))return held(this.hooks.profilePending(current)&&this.hooks.takingOver(peer.id)?'profile_pending_takeover':'profile_unverified');
    if(current.owner==='nxbridge'&&endpoint.mode==='app-server'&&!endpoint.daemon&&!this.hooks.subscribed(current,endpoint))
      return subscription()??held('owned_subscription_waiting');
    if(current.state==='needsHuman')return held('pending_user');
    if(current.state==='busy')return held('session_busy');
    if(current.state!=='ready')return held('runtime_unavailable');
    if(this.hooks.childRunning(current))return held('subagent_active');
  }
}
