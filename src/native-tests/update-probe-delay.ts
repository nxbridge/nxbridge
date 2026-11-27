import net from 'node:net';

                                       
                                                       
const emit=net.Socket.prototype.emit;
let pendingReplies=0;
net.Socket.prototype.emit=function(this:net.Socket,event:string|symbol,...args:any[]):boolean{
  if(event==='data'&&!this.destroyed){
    const pending=String(args[0]).trim().split(/\r?\n/).some(line=>{
      try{return JSON.parse(line).result?.handoff?.pending===true;}catch{return false;}
    });
    if(pending&&++pendingReplies===3){
      process.stderr.write(JSON.stringify({type:'fixture.readiness_probe_delayed',pendingReplies,delayMs:2000})+'\n');
      setTimeout(()=>{if(!this.destroyed)Reflect.apply(emit,this,[event,...args]);},2000).unref();
      return true;
    }
  }
  return Reflect.apply(emit,this,[event,...args]);
};
