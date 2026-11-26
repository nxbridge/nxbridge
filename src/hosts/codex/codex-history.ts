import { createReadStream, fstatSync, statSync, openSync, read as readAt, readSync, closeSync, type BigIntStats } from 'node:fs';

export interface HistoryReceipt {messageId:string;turnId:string;itemId:string;callId:string}
export interface HistoryCursor {dev:number;ino:number;offset:number;exactDev?:string;exactIno?:string}
export interface HistoryEvidence {status:'supported'|'unsupported'|'unavailable';receipts:HistoryReceipt[];calls:string[];complete?:boolean;reason?:string;cursor?:HistoryCursor;bytesRead?:number;checkpoint?:HistoryCheckpoint}
interface Call {count:number;receive:boolean;turnId:string}
interface Output {count:number;ids:string[];itemId:string;afterCall:boolean}
export interface HistoryCheckpoint {cursor:HistoryCursor;turnId:string;calls:[string,Call][];outputs:[string,Output][]}
interface ReadOptions {cursor?:HistoryCursor;tail?:boolean;since?:number;receiptSuffix?:true;checkpoint?:boolean;resume?:HistoryCheckpoint}
interface HistorySeek {before:number;headerEnd:number;suffix:Buffer}
interface Cache {stat:BigIntStats;offset:number;readOffset:number;tail:Buffer;skipping:boolean;skippedRelevant?:boolean;verified:boolean;invalid:boolean;turnId:string;calls:Map<string,Call>;outputs:Map<string,Output>;seek?:HistorySeek;seam?:{offset:number;bytes:Buffer};result?:HistoryEvidence}
const READ_BYTES=8*1024*1024,LINE_BYTES=2*1024*1024,READ_MS=75;
const SEEK_CHUNK=256*1024,SEEK_SUFFIX=16*1024,SEAM_BYTES=128;
const CATCH_UP_BATCHES=4,CATCH_UP_MS=1500;
const caches=new Map<string,Cache>(),pending=new Map<string,{stat:BigIntStats;work:Promise<HistoryEvidence>}>();
const sameFile=(a:BigIntStats,b:BigIntStats)=>a.dev===b.dev&&a.ino===b.ino&&a.birthtimeNs===b.birthtimeNs;
const sameVersion=(a:BigIntStats,b:BigIntStats)=>sameFile(a,b)&&a.size===b.size&&a.mtimeNs===b.mtimeNs&&a.ctimeNs===b.ctimeNs;
const matchingNumber=(legacy:number,exact:string|undefined,actual:bigint)=>exact!==undefined?exact===actual.toString():Number.isSafeInteger(legacy)&&BigInt(legacy)===actual;
const staleCursor=():HistoryEvidence=>({status:'unavailable',receipts:[],calls:[],complete:false,reason:'持久历史已轮换、截断或无法确认旧游标的文件编号，旧游标不能证明提交结果'});
const unreadable=(error:unknown):HistoryEvidence=>({status:'unavailable',receipts:[],calls:[],reason:'原生持久历史不可读取：'+(error as NodeJS.ErrnoException).code});

async function readBytes(fd:number,start:number,length:number):Promise<Buffer>{
  const buffer=Buffer.allocUnsafe(length);
  let offset=0;
  while(offset<length){
    const count=await new Promise<number>((resolve,reject)=>readAt(fd,buffer,offset,length-offset,start+offset,(error,bytes)=>error?reject(error):resolve(bytes)));
    if(!count)break;
    offset+=count;
  }
  return buffer.subarray(0,offset);
}

                                       
async function seekSince(fd:number,cache:Cache,since:number,started:number):Promise<number>{
  const seek=cache.seek!;
  let bytesRead=0;
  while(seek.before>seek.headerEnd&&bytesRead<READ_BYTES&&performance.now()-started<READ_MS){
    const right=seek.before,left=Math.max(seek.headerEnd,right-Math.min(READ_BYTES-bytesRead,SEEK_CHUNK));
    const bytes=await readBytes(fd,left,right-left);
    if(bytes.length!==right-left)throw Object.assign(Error('原生持久历史在回看时已截断'),{code:'history_changed'});
    bytesRead+=bytes.length;
    const window=seek.suffix.length?Buffer.concat([bytes,seek.suffix]):bytes;
    let end=window.lastIndexOf(10);
    while(end>=0){
      const previous=end?window.lastIndexOf(10,end-1):-1,line=previous+1;
      if(previous<0&&left!==seek.headerEnd)break;
      if(line<bytes.length&&end-line<=SEEK_SUFFIX){
        const text=window.subarray(line,end);
        if(text.includes('task_started')||text.includes('turn_started')){
          let row:any;try{row=JSON.parse(text.toString('utf8'));}catch{                    }
                                          
          if(row?.type==='event_msg'&&['task_started','turn_started'].includes(row.payload?.type)&&Date.parse(row.timestamp)<since){
            cache.offset=cache.readOffset=left+line;delete cache.seek;return bytesRead;
          }
        }
      }
      end=previous;
    }
    seek.before=left;seek.suffix=Buffer.from(window.subarray(0,SEEK_SUFFIX));
  }
  if(seek.before===seek.headerEnd){cache.offset=cache.readOffset=seek.headerEnd;delete cache.seek;}
  return bytesRead;
}
                                        
function irrelevantLargeLine(head:string,cache:Cache):boolean{
  let depth=0,payloadDepth=0,rowType:string|undefined;
  const fields:Record<string,string>={};
  for(let i=0;i<head.length;i++){
    const char=head[i];if(char==='{'){depth++;continue;}if(char==='}'){depth--;continue;}if(char!=='"')continue;
    const start=i++;for(;i<head.length;i++){if(head[i]==='\\'){i++;continue;}if(head[i]==='"')break;}if(i>=head.length)return false;
    let key:string;try{key=JSON.parse(head.slice(start,i+1));}catch{return false;}
    let next=i+1;while(/\s/.test(head[next]??'')&&next<head.length)next++;if(head[next]!==':')continue;next++;while(/\s/.test(head[next]??'')&&next<head.length)next++;
    if(depth===1&&key==='payload'&&head[next]==='{')payloadDepth=2;
    if(depth!==1&&depth!==payloadDepth||head[next]!=='"')continue;
    let end=next+1;for(;end<head.length;end++){if(head[end]==='\\'){end++;continue;}if(head[end]==='"')break;}if(end>=head.length)return false;
    let type:string;try{type=JSON.parse(head.slice(next,end+1));}catch{return false;}
    if(depth===1&&key==='type'){rowType=type;if(type!=='response_item')return true;}
    else if(depth===payloadDepth)fields[key]=type;
    if(rowType==='response_item'&&fields.type){
      if(!['function_call','function_call_output'].includes(fields.type))return true;
      if(fields.call_id?.startsWith('nxbridge-'))return false;
      if(fields.type==='function_call'&&fields.name)return !['receive_messages','mcp__claude__receive_messages'].includes(fields.name);
      if(fields.type==='function_call_output'&&fields.call_id){const call=cache.calls.get(fields.call_id);if(call)return !call.receive;}
    }
  }
  return false;
}
function outputMessages(output:unknown,skipPlainText=false):any[]{
  if(typeof output==='string'){
                                                 
    if(skipPlainText&&!/^\s*(?:\{|\[|")/.test(output))return[];
    try{return outputMessages(JSON.parse(output),skipPlainText);}catch{return[];}
  }
  if(Array.isArray(output))return output.filter(item=>item?.type==='text'||item?.type==='input_text').flatMap(item=>outputMessages(item.text,skipPlainText));
  if(!output||typeof output!=='object')return[];
  const value=output as any;
  if(Array.isArray(value.messages)||Array.isArray(value.notices))return[...(value.messages??[]),...(value.notices??[]).map((notice:any)=>({message_id:notice?.notice_id}))];
  return Array.isArray(value.content)?value.content.filter((item:any)=>item?.type==='text').flatMap((item:any)=>outputMessages(item.text,skipPlainText)):[];
}
const receiveCall=(item:any)=>item.type==='function_call'&&(item.name==='mcp__claude__receive_messages'||item.name==='receive_messages'&&item.namespace==='claude');
function accept(cache:Cache,line:string,sessionId:string):void{
  if(!line.trim())return;
                                              
  if(cache.verified&&!/function_call|task_started|turn_started|session_meta/.test(line)){if(!line.trimEnd().endsWith('}'))cache.invalid=true;return;}
  let row:any;try{row=JSON.parse(line);}catch{cache.invalid=true;return;}
  if(!cache.verified){if(row.type!=='session_meta'||row.payload?.id!==sessionId){cache.invalid=true;return;}cache.verified=true;return;}
                                        
  if(row.type==='session_meta')return;
  if(row.type==='event_msg'&&['task_started','turn_started'].includes(row.payload?.type)&&typeof row.payload.turn_id==='string')cache.turnId=row.payload.turn_id;
  if(row.type!=='response_item')return;
  const item=row.payload;if(!item||typeof item!=='object'){cache.invalid=true;return;}
  if(item.type!=='function_call'&&item.type!=='function_call_output'||typeof item.call_id!=='string'||!item.call_id)return;
  if(item.type==='function_call'){const old=cache.calls.get(item.call_id);cache.calls.set(item.call_id,{count:(old?.count??0)+1,receive:receiveCall(item),turnId:cache.turnId});return;}
  const old=cache.outputs.get(item.call_id),call=cache.calls.get(item.call_id),valid=(item.name===undefined||['receive_messages','mcp__claude__receive_messages'].includes(item.name))&&(item.namespace===undefined||item.namespace==='claude');
  cache.outputs.set(item.call_id,{count:(old?.count??0)+1,afterCall:cache.calls.has(item.call_id),itemId:typeof item.id==='string'?item.id:item.call_id,ids:valid?outputMessages(item.output,call?.count===1&&!call.receive&&!old).flatMap(message=>typeof message?.message_id==='string'&&message.message_id?[message.message_id]:[]):[]});
}
                                                         
export async function readCodexHistory(filename:unknown,sessionId:string,options:ReadOptions={}):Promise<HistoryEvidence>{
  if(typeof filename!=='string'||!filename)return{status:'unavailable',receipts:[],calls:[],reason:'官方线程没有提供可读取的持久历史路径'};
  let fd:number|undefined,stat:BigIntStats;
  try{fd=openSync(filename,'r');stat=fstatSync(fd,{bigint:true});}catch(error){if(fd!==undefined)closeSync(fd);return unreadable(error);}
  const cursor=options.cursor??options.resume?.cursor;
  if(cursor&&(!matchingNumber(cursor.dev,cursor.exactDev,stat.dev)||!matchingNumber(cursor.ino,cursor.exactIno,stat.ino)
    ||!Number.isSafeInteger(cursor.offset)||cursor.offset<0||BigInt(cursor.offset)>stat.size)){
    closeSync(fd);return staleCursor();
  }
  const key=JSON.stringify([filename,sessionId,options.checkpoint?'repair':options.cursor??(options.since===undefined?options.receiptSuffix?'receipt-suffix':options.tail?'tail':'full':{since:options.since})]);
  const running=pending.get(key);
  if(running){
    closeSync(fd);
    if(sameVersion(running.stat,stat))return running.work;
    await running.work;
    return readCodexHistory(filename,sessionId,options);
  }
  const work=read(filename,sessionId,key,options,fd,stat).finally(()=>pending.delete(key));pending.set(key,{stat,work});return work;
}

                                          
export async function catchUpCodexHistory(filename:unknown,sessionId:string,since:number):Promise<HistoryEvidence>{
  const started=performance.now(),options={since};
  let result=await readCodexHistory(filename,sessionId,options);
  for(let batch=1;batch<CATCH_UP_BATCHES&&performance.now()-started<CATCH_UP_MS&&result.status==='supported'&&!result.complete&&result.bytesRead;batch++)
    result=await readCodexHistory(filename,sessionId,options);
  if(result.status==='supported'&&!result.complete&&!result.receipts.length){
    const suffix=await readCodexHistory(filename,sessionId,{receiptSuffix:true});
    if(suffix.receipts.length)return suffix;
  }
  return result;
}

async function read(filename:string,sessionId:string,key:string,options:ReadOptions,fd:number,stat:BigIntStats):Promise<HistoryEvidence>{
  let stream:ReturnType<typeof createReadStream>|undefined;
  try{
    const size=Number(stat.size);let cache=caches.get(key);
    if(!Number.isSafeInteger(size))return staleCursor();
    const cursor=options.cursor??options.resume?.cursor;
    if(cache&&sameVersion(cache.stat,stat)&&cache.readOffset>=size&&cache.result)return sameVersion(stat,statSync(filename,{bigint:true}))?{...cache.result,bytesRead:0}:staleCursor();
    if(cache?.seam&&sameFile(cache.stat,stat)&&stat.size>cache.stat.size){
      const {offset,bytes}=cache.seam;
      if(!(await readBytes(fd,offset,bytes.length)).equals(bytes))cache=undefined;
    }
    if(!cache||!sameFile(cache.stat,stat)||stat.size<cache.stat.size||stat.size===cache.stat.size&&!sameVersion(cache.stat,stat)){
      cache={stat,offset:0,readOffset:0,tail:Buffer.alloc(0),skipping:false,verified:false,invalid:false,turnId:'persisted-history',calls:new Map(),outputs:new Map()};
      if(cursor||options.tail||options.since!==undefined||options.receiptSuffix){
          const head=Buffer.alloc(Math.min(size,65536)),length=readSync(fd,head,0,head.length,0),newline=head.subarray(0,length).indexOf(10);
          if(newline<0)return{status:'unavailable',receipts:[],calls:[],complete:false,reason:'持久历史首条身份记录尚不完整'};
          accept(cache,head.subarray(0,newline).toString('utf8'),sessionId);
          if(!cache.verified||cache.invalid)return{status:'unsupported',receipts:[],calls:[],complete:false,reason:'持久历史首条身份与目标不符'};
          if(cursor){cache.offset=cursor.offset;if(options.resume){cache.turnId=options.resume.turnId;cache.calls=new Map(options.resume.calls);cache.outputs=new Map(options.resume.outputs);}}
          else if(options.since!==undefined){
            cache.offset=newline+1;cache.seek={before:size,headerEnd:cache.offset,suffix:Buffer.alloc(0)};
          }
          else if(options.receiptSuffix){
            cache.offset=Math.max(newline+1,size-READ_BYTES/2);
            const previous=head.subarray(0,1);
            cache.skipping=readSync(fd,previous,0,1,cache.offset-1)!==1||previous[0]!==10;
          }
          else if(size){
            const tail=Buffer.alloc(Math.min(size,65536));readSync(fd,tail,0,tail.length,size-tail.length);const last=tail.lastIndexOf(10);
            if(last<0)return{status:'unavailable',receipts:[],calls:[],complete:false,reason:'持久历史尾部尚无可核验的完整行边界'};
            cache.offset=size-tail.length+last+1;
          }
          cache.readOffset=cache.offset;
      }
    }
    caches.delete(key);caches.set(key,cache);while(caches.size>16)caches.delete(caches.keys().next().value!);
    const started=performance.now(),seekBytes=cache.seek?await seekSince(fd,cache,options.since!,started):0,before=cache.readOffset;
    if(seekBytes){
      const offset=Math.max(0,size-SEAM_BYTES);cache.seam={offset,bytes:await readBytes(fd,offset,size-offset)};
    }
    let current=stat;
    if(!cache.seek&&size>cache.readOffset&&seekBytes<READ_BYTES&&(seekBytes===0||performance.now()-started<READ_MS)){
      stream=createReadStream(filename,{fd,start:cache.readOffset,end:Math.min(size,cache.readOffset+READ_BYTES-seekBytes)-1,highWaterMark:65536});
      for await(const chunk of stream){
        const bytes=chunk as Buffer;cache.readOffset+=bytes.length;let tail=Buffer.concat([cache.tail,bytes]),newline:number;
        if(options.since!==undefined||options.receiptSuffix){const seam=Buffer.from(bytes.subarray(-SEAM_BYTES));cache.seam={offset:cache.readOffset-seam.length,bytes:seam};}
        if(cache.skipping){
          const end=tail.indexOf(10);
          if(end<0){if(performance.now()-started>=READ_MS)break;continue;}
          if(cache.skippedRelevant)cache.invalid=true;
          cache.skipping=false;delete cache.skippedRelevant;tail=tail.subarray(end+1);cache.offset=cache.readOffset-tail.length;
        }
        while((newline=tail.indexOf(10))>=0){
          const line=tail.subarray(0,newline);tail=tail.subarray(newline+1);
          if(line.length>LINE_BYTES){if(!irrelevantLargeLine(line.subarray(0,65536).toString('utf8'),cache))cache.invalid=true;}else accept(cache,line.toString('utf8'),sessionId);
          cache.offset=cache.readOffset-tail.length;
        }
        if(tail.length>LINE_BYTES){cache.skipping=true;cache.skippedRelevant=!irrelevantLargeLine(tail.subarray(0,65536).toString('utf8'),cache);tail=Buffer.alloc(0);}
        cache.tail=Buffer.from(tail);if(performance.now()-started>=READ_MS)break;
      }
    }
    current=statSync(filename,{bigint:true});
    if(!sameFile(stat,current)||current.size<stat.size||current.size===stat.size&&!sameVersion(stat,current)){
      caches.delete(key);return staleCursor();
    }
    cache.stat=stat;
    if(cache.seek)return cache.result={status:'supported',receipts:[],calls:[],complete:false,bytesRead:seekBytes};
    const extra={complete:!options.receiptSuffix&&cache.readOffset>=size&&current.size===stat.size&&!cache.tail.length&&!cache.skipping,
      cursor:options.receiptSuffix?undefined:{dev:Number(stat.dev),ino:Number(stat.ino),exactDev:stat.dev.toString(),exactIno:stat.ino.toString(),offset:cache.offset},bytesRead:seekBytes+cache.readOffset-before};
    if(!cache.verified||cache.invalid)return cache.result={status:'unsupported',receipts:[],calls:[],...extra,complete:false,reason:'原生持久历史格式、行大小或目标线程无法完整核验'};
    const receipts:HistoryReceipt[]=[];
    for(const [callId,value] of cache.outputs){const call=cache.calls.get(callId);if(!call?.receive||call.count!==1||value.count!==1||!value.afterCall)continue;for(const messageId of value.ids)receipts.push({messageId,callId,turnId:call.turnId,itemId:value.itemId});}
    return cache.result={status:'supported',receipts,calls:[...new Set([...cache.calls.keys(),...cache.outputs.keys()])],...extra,...(options.checkpoint&&extra.cursor&&!extra.complete?{checkpoint:{cursor:extra.cursor,turnId:cache.turnId,calls:[...cache.calls],outputs:[...cache.outputs]}}:{})};
  }catch(error){caches.delete(key);return unreadable(error);}
  finally{if(stream)stream.destroy();else closeSync(fd);}
}
