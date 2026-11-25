import type { Peer, RegisterInput } from '../../types.js';

                                          
export function runtimeProfile(previous:Partial<Peer>,input:Pick<RegisterInput,'model'|'effort'|'serviceTier'>):Pick<Peer,'model'|'effort'|'serviceTier'>{
  const model=input.model===undefined?previous.model:input.model??undefined;
  const effort=input.effort===undefined?(model===previous.model?previous.effort:undefined):input.effort??undefined;
  const serviceTier=input.serviceTier===undefined?previous.serviceTier:input.serviceTier;
  return{...(model===undefined?{}:{model}),...(effort===undefined?{}:{effort}),...(serviceTier===undefined?{}:{serviceTier})};
}
