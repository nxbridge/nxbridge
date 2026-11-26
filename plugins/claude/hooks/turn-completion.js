import { call, captureContext } from './mod-control.js';

const idOf = value => typeof value === 'string' && value.length > 0 ? value : undefined;
const count = value => Array.isArray(value) ? value.length : undefined;
const workTool = (tool,bridgeSend) => typeof tool === 'string' && tool !== 'ListAgents' && !/^mcp__(?:nxbridge|plugin_nxbridge_codex)__/.test(tool) && !bridgeSend;
const STOP_BUDGET_MS=1500;
const covered = turn => turn.covered && turn.pendingTools.size === 0;
const stats = turn => ({startedAt:turn.startedAt,covered:covered(turn),actions:turn.actions+turn.pendingTools.size,workTools:turn.workTools,lastWorkAt:turn.lastWorkAt,
  ...(turn.partnerMessageId?{partnerDriven:true,partnerMessageIds:[turn.partnerMessageId]}:{})});

                                    
export class TurnCompletion {
  turns = new WeakMap();
  wake = new WeakMap();
  participation = new WeakMap();

  current(context, id) {
    const turn = this.turns.get(context);
    return turn && (id === undefined || turn.id === id) ? turn : undefined;
  }
  start(context, event, startedAt) {
    if (!idOf(event.turnId)) { this.turns.delete(context); this.wake.delete(context); return; }
    if (this.current(context,event.turnId)) return;
    const turn = {id:event.turnId,startedAt,covered:true,actions:0,workTools:0,interventions:new Map(),tools:new Set(),pendingTools:new Set()};
    this.turns.set(context,turn);
    const wake = this.wake.get(context);
    this.wake.delete(context);
                                                      
    if (wake?.ids && wake.text === event.text) {
      wake.turn = turn;
      this.claim(context,wake);
    }
  }
  prompt(context) {
    const input={ambiguous:this.wake.has(context)};
    this.wake.set(context,input);
    return input;
  }
  status(context,value) {
    const guard=value?.completion_guard??value?.self?.completion_guard;
    if(value?.paired===false||guard?.state==='not_applicable')this.participation.set(context,Date.now());
    else this.participation.delete(context);
  }
  shouldCheck(context,turn) {
    const ruledOut=this.participation.get(context);
    return !!turn.partnerMessageId||ruledOut===undefined||Date.now()-ruledOut>=15_000;
  }
  dropped(context,input) {
    if(this.wake.get(context)===input)this.wake.delete(context);
  }
  prepareWake(context,event,notice,input) {
    if (!event.isMeta || event.turnId || notice.wake !== true) return;
    const ids = notice.interventionIds ?? (notice.interventionId ? [notice.interventionId] : []);
    if (!Array.isArray(ids) || !ids.length || input?.ambiguous || this.wake.get(context)!==input) return;
    const turn = this.current(context);
    if (turn && !turn.published) return;
    const wake = {text:event.text,ids};
    this.wake.set(context,wake);
    return wake;
  }
  settleWake(context,wake,submitted) {
    if (!wake) return;
    if (!submitted) {
      if (this.wake.get(context) === wake) this.wake.delete(context);
      return;
    }
    wake.submitted = true;
    this.claim(context,wake);
  }
  claim(context,wake) {
    const turn = wake.turn;
    if (!wake.submitted || !turn || this.current(context) !== turn || turn.published) return;
    for (const id of wake.ids) if (idOf(id)) turn.interventions.set(id,{actions:0});
  }
  action(turn, tool, id, bridgeSend=false) {
    if (!turn || turn.published || id && turn.tools.has(id)) return;
    if (id) { turn.pendingTools.delete(id); turn.tools.add(id); }
    turn.actions++;
    if (typeof tool !== 'string') turn.covered = false;
    else if (workTool(tool,bridgeSend)) {
      turn.workTools++;
      turn.lastWorkAt = Date.now();
    }
  }
  finished(turn,tool,bridgeSend=false) {
                                           
    if(turn&&!turn.published&&workTool(tool,bridgeSend))turn.lastWorkAt=Date.now();
  }
  step(turn, chunk, inputChunks) {
    if (!turn || turn.published) return;
    const event = chunk?.type === 'stream_event' ? chunk.event : chunk;
    const blocks = event?.content_block ? [event.content_block] : event?.type === 'assistant' && Array.isArray(event.message?.content) ? event.message.content : [];
    for (const block of blocks) {
      if (block?.type === 'server_tool_use' || block?.type === 'mcp_tool_use') this.action(turn,block.name,block.id);
      else if (block?.type === 'tool_use') this.pendingTool(turn,block.id);
      else if (block?.type?.endsWith('_tool_use')) turn.covered = false;
    }
                                                    
    if (chunk?.kind === 'input' && chunk.json?.length && !inputChunks.has(chunk.index)) inputChunks.set(chunk.index,false);
    if (chunk?.kind === 'tool') {
      inputChunks.set(chunk.index,true);
      this.pendingTool(turn,chunk.id);
    }
  }
  pendingTool(turn,id) {
    if (!idOf(id)) turn.covered=false;
    else if (!turn.tools.has(id)) turn.pendingTools.add(id);
  }
  stop(context,event) {
    const turn = this.current(context);
    if (!turn || event.agent_id || event.agent_transcript_path || event.session_id !== context.sessionId) return;
    const prompt = idOf(event.prompt_id);
    let confirmed;
    if(turn.pendingIntervention){
      if(event.stop_hook_active===true){
        const {id,actions}=turn.pendingIntervention;
        turn.interventions.set(id,{actions});
        confirmed=id;
      }
      turn.pendingIntervention=undefined;
    }
    const background={tasks:count(event.background_tasks),crons:count(event.session_crons)};
    if(confirmed)turn.pendingConfirmation??={facts:{turn_id:turn.id,stop_hook_active:true,stats:stats(turn),background}};
    const attempt = {turn,prompt,confirmed,background};
    turn.stopPending = attempt;
    turn.stop = undefined;
    return attempt;
  }
  clear(context) {
    this.turns.delete(context);
    this.wake.delete(context);
    this.participation.delete(context);
  }
}

                                                     
export function closeTurnStop(attempt) {
  if(!attempt||attempt.turn.stopPending!==attempt)return;
  attempt.turn.stopPending=undefined;
}
                                            
export async function publishStopConfirmation($,tracker,context,turn){
  const pending=turn.pendingConfirmation;
  if(!pending||pending.inFlight)return;
  pending.inFlight=true;
  try{
    if(tracker.current(context)!==turn||await captureContext($)!==context)return;
    const result=await call($,'turn-stop-check',{context,...pending.facts});
    if((result?.confirmed===true||result?.stale===true)&&tracker.current(context)===turn&&turn.pendingConfirmation===pending)turn.pendingConfirmation=undefined;
  }catch{                              }
  finally{pending.inFlight=false;}
}
export async function checkTurnStop($,tracker,context,attempt,event,result,signal) {
  const {turn,prompt,background} = attempt;
  const current = () => tracker.current(context) === turn && turn.stopPending === attempt;
  if (!current() || await captureContext($) !== context || !current()) return result;
                                                    
  if(attempt.confirmed){
    await publishStopConfirmation($,tracker,context,turn);
    if(!current()||await captureContext($)!==context||!current())return result;
  }
                                   
  if (result?.preventContinuation || signal.aborted) {
    closeTurnStop(attempt);
    return result;
  }
  if (result?.block) {
    closeTurnStop(attempt);
    turn.completed=undefined;
    return result;
  }
  if (prompt && background.tasks === 0 && background.crons === 0 && event.stop_hook_active === false && !turn.published && !turn.aborted && context.nativeHealth?.state !== 'failed' && tracker.shouldCheck(context,turn)) {
    const baseline = turn.actions;
    const deadline_at=Date.now()+STOP_BUDGET_MS;
    try {
      const decision = await call($,'turn-stop-check',{context,turn_id:turn.id,stop_hook_active:false,stats:stats(turn),background,deadline_at});
      if (await captureContext($) !== context || !current()) return result;
      if (!signal.aborted && !turn.aborted && Date.now()<deadline_at && typeof decision?.block === 'string' && decision.block.trim()) {
        const id = idOf(decision.intervention_id);
        if (id) turn.pendingIntervention={id,actions:baseline};
        closeTurnStop(attempt);
        turn.completed = undefined;
        return {...result,block:decision.block};
      }
    } catch {                        }
  }
  if (current()) {
    closeTurnStop(attempt);
    if(!signal.aborted)turn.stop = {prompt,background,observedAt:Date.now()};
  }
  return result;
}

export function turnCompletionFacts(turn) {
  const stop = turn.stop;
  if (turn.completed===undefined || turn.aborted || turn.stopPending || !stop?.prompt || stop.background.tasks !== 0 || stop.background.crons !== 0) return;
  return {turn_id:turn.id,completed_at:turn.completed,stats:stats(turn),background:stop.background,
    interventions:[...turn.interventions].slice(-16).map(([id,value])=>({id,inert:covered(turn)&&turn.actions===value.actions}))};
}
