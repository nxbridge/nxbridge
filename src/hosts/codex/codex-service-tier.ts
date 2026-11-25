                                              
export const nativeServiceTier=(value:string)=>value==='standard'?'default':value==='fast'?'priority':value;
export const observedServiceTier=(value:unknown):string|null|undefined=>
  value==='default'?'standard':value==='priority'?'fast':typeof value==='string'||value===null?value:undefined;
