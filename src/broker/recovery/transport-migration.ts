import path from 'node:path';
import { randomUUID } from 'node:crypto';
import { homedir } from 'node:os';
import { createHash } from 'node:crypto';
import type { HealthStorage } from '../health/contracts.js';
import type { Endpoint, Peer,ModelProfile } from '../../types.js';
import type { ClaudeChildRecord } from '../../hosts/claude/claude-child.js';
import type { OwnedProcessRecord } from '../../process/owned-processes.js';
import type { ManagedRuntime } from '../../hosts/managed-runtimes.js';
import { BridgeError, describeError } from '../../shared/errors.js';

export interface TransportSource {
  epoch:string;address:string;pid:number|null;procStart:string|null;runtimeId?:string;
}
export interface TransportMigration {
  version:1;id:string;peerId:string;sessionId:string;pairId:string;source:TransportSource;paths:string[];
  state:'pending'|'blocked'|'restarting'|'ready'|'unsupported'|'not_migratable'|'unknown';
  attempts:number;phase?:'stopping'|'launching';openedAt:number;updatedAt:number;nextAt:number;
  code?:string;reason?:string;replacement?:TransportSource;
  profile?:ModelProfile;
}
export const transportMigrationKey=(id:string)=>'transportMigration:'+id;
const source=(endpoint:Endpoint):TransportSource=>({epoch:endpoint.epoch,address:endpoint.address,pid:endpoint.pid,procStart:endpoint.procStart,...(endpoint.runtimeId?{runtimeId:endpoint.runtimeId}:{})});
const same=(a:TransportSource,b:TransportSource)=>a.epoch===b.epoch&&a.address===b.address&&a.pid===b.pid&&a.procStart===b.procStart;

                                                  
export function persistentTransportPath(home:string,file:string,userHome=homedir()):boolean{
  const root=path.resolve(home),short=path.resolve(userHome,'.nxb-'+createHash('sha256').update(root).digest('hex').slice(0,12));
  return [path.join(root,'run'),short].some(directory=>{const relative=path.relative(directory,path.resolve(file));return relative!==''&&!relative.startsWith('..'+path.sep)&&relative!=='..'&&!path.isAbsolute(relative);});
}
export function ownedTransportPaths(home:string,peer:Peer,endpoint:Endpoint,owners:OwnedProcessRecord[],child?:ClaudeChildRecord,runtime?:ManagedRuntime,platform=process.platform):string[]{
  if(platform==='win32'||peer.owner!=='nxbridge')return[];
  const paths:string[]=[];
  if(endpoint.mode==='claude-inbox'&&path.isAbsolute(endpoint.address)&&!persistentTransportPath(home,endpoint.address))paths.push(endpoint.address);
  if(child?.sessionId===peer.sessionId&&!['handoff','user','released','stopped'].includes(child.phase)&&!persistentTransportPath(home,child.address))paths.push(child.address);
  for(const record of owners){
    const sameNative=record.native?.pid===endpoint.pid&&record.native?.startedAt===endpoint.procStart;
    const sameWorker=!!child?.worker&&record.identity.pid===child.worker.pid&&record.identity.startedAt===child.worker.startedAt;
    const belongs=record.peerId===peer.id&&(sameNative||sameWorker)||runtime&&record.proof.launchId===runtime.id;
    if(belongs&&!record.releasedToUser&&record.posixControl&&!persistentTransportPath(home,record.posixControl))paths.push(record.posixControl);
  }
  return [...new Set(paths)].sort();
}

                                                
export class TransportMigrations {
  private checked=new Set<string>();
  constructor(private storage:HealthStorage,private now=Date.now){}
  get(peerId:string):TransportMigration|undefined{return this.storage.setting(transportMigrationKey(peerId));}
  private save(value:TransportMigration):void{value.updatedAt=this.now();this.storage.setSetting(transportMigrationKey(value.peerId),value);}
  reconcile(peerId:string):TransportMigration|undefined{
    const value=this.get(peerId);
    if(value&&!this.checked.has(value.id)){
      this.checked.add(value.id);
      if(value.state==='restarting'){value.state='unknown';value.code='migration_interrupted';value.reason='协调器在迁移结算前退出，正在核对原进程和替代进程，不会重复启动。';this.save(value);}
    }
    return value;
  }
  observe(peer:Peer,pairId:string,endpoint:Endpoint,paths:string[]):TransportMigration|undefined{
    let value=this.reconcile(peer.id);
    if(value&&value.sessionId!==peer.sessionId)value=undefined;
    const current=source(endpoint);
    if(value&&value.pairId!==pairId&&value.attempts===0){value.pairId=pairId;this.save(value);}
    if(value&&!same(value.source,current)){
      if(value.attempts&&paths.length){value.state='not_migratable';value.code='transport_still_temporary';value.reason='已接回原会话，但宿主仍发布旧目录中的连接，停止重复迁移。';value.replacement=current;this.save(value);return value;}
      if(!paths.length){if(value.state!=='ready'){value.state='ready';value.replacement=current;delete value.code;delete value.reason;this.save(value);}return value;}
      value=undefined;
    }
    if(!paths.length)return value;
    if(value)return value;
    value={version:1,id:randomUUID(),peerId:peer.id,sessionId:peer.sessionId!,pairId,source:current,paths,state:'pending',attempts:0,openedAt:this.now(),updatedAt:this.now(),nextAt:0};
    this.checked.add(value.id);this.save(value);return value;
  }
  due(value:TransportMigration):boolean{return ['pending','blocked','unsupported'].includes(value.state)&&value.attempts===0&&value.nextAt<=this.now();}
  rememberProfile(value:TransportMigration,profile:ModelProfile):void{
    const current=this.get(value.peerId);if(!current||current.id!==value.id||current.attempts)return;
    current.profile=profile;this.save(current);Object.assign(value,current);
  }
  hold(value:TransportMigration,code:string,reason:string,terminal=false):void{
    const current=this.get(value.peerId);if(!current||current.id!==value.id)return;
    if(['unknown','not_migratable','ready'].includes(current.state))return;
    current.state=terminal?'unsupported':'blocked';current.code=code;current.reason=reason;current.nextAt=this.now()+30000;this.save(current);Object.assign(value,current);
  }
  claim(value:TransportMigration,peer:Peer,endpoint:Endpoint,pairId:string):void{
    this.storage.transaction(()=>{
      const current=this.get(value.peerId);
      if(!current||current.id!==value.id||current.sessionId!==peer.sessionId||current.pairId!==pairId||!same(current.source,source(endpoint)))throw new BridgeError('migration_changed','迁移期间原会话或连接代次已经改变，未停止。');
      if(current.attempts||!['pending','blocked','unsupported'].includes(current.state))throw new BridgeError('migration_uncertain','本次迁移已提交，不会再次停止或启动会话。');
      current.attempts=1;current.state='restarting';current.phase='stopping';delete current.code;delete current.reason;this.save(current);Object.assign(value,current);
    });
  }
  launching(value:TransportMigration):void{
    const current=this.get(value.peerId);if(current?.id!==value.id||current.state!=='restarting')throw new BridgeError('migration_changed','迁移预约已经改变，未再次启动。');
    current.phase='launching';this.save(current);Object.assign(value,current);
  }
  launchGuard(value:TransportMigration):void{
    const current=this.get(value.peerId);if(current?.id!==value.id||current.state!=='restarting'||current.phase!=='launching')throw new BridgeError('migration_uncertain','迁移启动预约已经改变，未重复启动。');
  }
  failed(value:TransportMigration,error:unknown):void{
    const current=this.get(value.peerId);if(current?.id!==value.id)return;
    if(current.attempts){current.state='unknown';current.code='migration_incomplete';current.reason=describeError(error).slice(0,1200);this.save(current);Object.assign(value,current);}
    else this.hold(value,(error as BridgeError).code??'migration_preflight_failed',describeError(error).slice(0,1200));
  }
}
