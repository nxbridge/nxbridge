                                
                                   
export class HumanTurns {
  pending = new WeakMap();
  latest = new WeakMap();
  input = new WeakMap();
  begin(context) {
    const entry={before:this.latest.get(context)};
    this.input.set(context,entry);
    return entry;
  }
  matches(candidate, text) {
    return candidate.text === text || candidate.command && text.includes('<command-name>/' + candidate.command + '</command-name>');
  }
  consume(context, pending, index) {
                                                        
    const after = pending[index].after;
    const remaining = pending.filter((value, position) => position > index || value.after !== after);
    if (remaining.length) this.pending.set(context, remaining); else this.pending.delete(context);
  }
  evidence(context,candidate,turn) {
    const entry=candidate.entry;
                                             
    if(entry && this.input.get(context)===entry && entry.first===turn && candidate.origin && turn.turnId && candidate.text===turn.text) {
      this.input.delete(context);
      return {human_origin:candidate.origin,human_turn_id:turn.turnId,human_input_at:turn.startedAt};
    }
    return {};
  }
  submit(context, event, result, before, entry, origin) {
    if (result?.drop !== undefined || typeof result?.text !== 'string') return false;
    const current = this.latest.get(context);
                                               
    const candidate = { after: event.turnId ?? before?.turnId, text: result.text, command: event.command?.name,entry,origin };
    const pending = this.pending.get(context) ?? [];
    pending.push(candidate);
    if (current && current !== before && current.turnId !== candidate.after && this.matches(candidate, current.text)) {
      this.consume(context, pending, pending.length - 1);
      return { turnId: current.turnId,...this.evidence(context,candidate,current) };
    }
                                        
    if (pending.length > 64) pending.splice(0, pending.length - 64);
    this.pending.set(context, pending);
    return false;
  }
  start(context, event, startedAt) {
    const turn={ turnId: event.turnId, text: event.text ?? '', startedAt };
    this.latest.set(context,turn);
    const entry=this.input.get(context);
    if(entry && !entry.first)entry.first=turn;
    const pending = this.pending.get(context);
    const index = pending?.findIndex(value => value.after !== event.turnId && this.matches(value, event.text ?? '')) ?? -1;
    if (index < 0) return false;
    this.consume(context, pending, index);
    return this.evidence(context,pending[index],turn);
  }
  current(context, turnId) {
    const current = this.latest.get(context);
    return !current || current.turnId === turnId;
  }
  clear(context) { this.pending.delete(context); this.latest.delete(context); this.input.delete(context); }
}
