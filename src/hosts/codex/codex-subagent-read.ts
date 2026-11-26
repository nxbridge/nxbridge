import { closeSync, fstatSync, openSync, readSync, statSync, type BigIntStats } from 'node:fs';
import { open, stat } from 'node:fs/promises';

export interface CodexSubagentReadFloor {dev:number;ino:number;offset:number;exactIno?:string}
export interface CodexSubagentReadCandidate {key:string;agentId:string;childTurnId:string;agentPath?:string;floor:CodexSubagentReadFloor}
export interface CodexSubagentContinuation {verified:boolean;read:ReadonlySet<string>;continued:ReadonlySet<string>;openTurnId?:string;closedTurnId?:string;lastProgressAt?:number}

const HEADER_BYTES=64*1024,TAIL_BYTES=1024*1024;

function transcriptFloor({dev,ino,size}:BigIntStats):CodexSubagentReadFloor{
                                          
  return{dev:Number(dev),ino:Number(ino),offset:Number(size),exactIno:ino.toString()};
}

function sameTranscript(saved:CodexSubagentReadFloor,current:CodexSubagentReadFloor,allowRoundedIno=false):boolean{
  return saved.dev===current.dev&&(saved.exactIno===undefined
    ?(allowRoundedIno||Number.isSafeInteger(saved.ino))&&saved.ino===current.ino:saved.exactIno===current.exactIno);
}

const transcriptIdentity=({dev,ino,birthtimeNs}:BigIntStats)=>[dev,ino,birthtimeNs].join(':');
const transcriptStamp=(value:BigIntStats)=>[transcriptIdentity(value),value.size,value.mtimeNs,value.ctimeNs].join(':');

function readBytes(fd:number,offset:number,size:number):Buffer{
  const buffer=Buffer.allocUnsafe(size);
  let read=0;
  while(read<size){
    const count=readSync(fd,buffer,read,size-read,offset+read);
    if(!count)break;
    read+=count;
  }
  return buffer.subarray(0,read);
}

function withTranscript<T>(file:unknown,sessionId:string,work:(fd:number,floor:CodexSubagentReadFloor)=>T):T|undefined{
  if(typeof file!=='string'||!file||!sessionId)return;
  let fd:number|undefined;
  try{
    fd=openSync(file,'r');
    const snapshot=fstatSync(fd,{bigint:true}),floor=transcriptFloor(snapshot),{offset}=floor;
    const head=readBytes(fd,0,Math.min(offset,HEADER_BYTES)),end=head.indexOf(10);
    if(end<0)return;
    const row=JSON.parse(head.subarray(0,end).toString('utf8'));
    if(row.type!=='session_meta'||row.payload?.id!==sessionId)return;
    const value=work(fd,floor);
    if(transcriptStamp(statSync(file,{bigint:true}))===transcriptStamp(snapshot))return value;
  }catch{return;}
  finally{if(fd!==undefined)closeSync(fd);}
}

                                    
export function captureCodexSubagentReadFloor(file:unknown,sessionId:string):CodexSubagentReadFloor|undefined{
  return withTranscript(file,sessionId,(_fd,floor)=>floor);
}

const sequence=(value:unknown):number|undefined=>typeof value==='number'&&Number.isSafeInteger(value)&&value>0?value:undefined;

function v1Result(item:any):string|undefined{
  if(item.type!=='message'||item.role!=='user'||!Array.isArray(item.content))return;
  const kinds=item.internal_chat_message_metadata_passthrough?.content_item_kinds;
  if(!Array.isArray(kinds))return;
  const index=kinds.indexOf('multi_agent.subagent_notification');
  if(index<0||index!==kinds.lastIndexOf('multi_agent.subagent_notification'))return;
  const content=item.content[index];
  if(content?.type!=='input_text'||typeof content.text!=='string')return;
  const match=/^<subagent_notification>\n([\s\S]+)\n<\/subagent_notification>$/.exec(content.text);
  if(!match)return;
  const notice=JSON.parse(match[1]!);
  if(typeof notice?.agent_path==='string'&&notice.status&&Object.hasOwn(notice.status,'completed'))return notice.agent_path;
}

function v2Result(item:any):string|undefined{
  if(item.type!=='agent_message'||typeof item.author!=='string'||!Array.isArray(item.content)||item.content.length!==1)return;
  const separator=item.author.lastIndexOf('/');
  if(separator<1||item.recipient!==item.author.slice(0,separator))return;
  const [content]=item.content;
  if(content?.type==='input_text'&&typeof content.text==='string'&&content.text.startsWith(`Message Type: FINAL_ANSWER\nTask name: ${item.recipient}\nSender: ${item.author}\nPayload:\n`))return item.author;
}

export const codexQueuedAgentMetadata=(row:any):boolean=>row.type==='inter_agent_communication_metadata'&&row.payload?.trigger_turn===false;
export const codexChildResult=(item:any,queued:boolean):string|undefined=>v1Result(item)??(queued?v2Result(item):undefined);

const SAMPLE_WINDOW_BYTES=32*1024*1024,SAMPLE_CHUNK_BYTES=256*1024,MAX_SAMPLE_LINE_BYTES=4*1024*1024,APPEND_SEAM_BYTES=128;
const MODEL_TOOL_TYPES=new Set(['function_call','custom_tool_call','local_shell_call','web_search_call','image_generation_call','computer_call']);
const START_EVENTS=new Set(['task_started','turn_started']),END_EVENTS=new Set(['task_complete','turn_complete','turn_aborted']);
const COMPLETION_PREFIX='subagent-completed-';
const continuationCache=new Map<string,{descriptor:BigIntStats;input:string;cursor:ContinuationCursor;seam:Buffer;proof:CodexSubagentContinuation}>();

interface SampledChildResult {key:string;sequence:number;turnId?:string}
interface ChildCompletionInterval {
  candidate?:CodexSubagentReadCandidate;
  result?:SampledChildResult;
  afterSequence?:number;
  ambiguous:boolean;
}

interface ContinuationCursor {
  offset:number;start:number;skippingLine:boolean;queued:boolean;mayInfer:boolean;inferredOpen:boolean;
  openTurnId?:string;closedTurnId?:string;lastProgressAt?:number;
  read:Set<string>;continued:Set<string>;waitingForTool:Map<string,SampledChildResult>;
  unsampled:Map<string,SampledChildResult>;intervals:Map<string,ChildCompletionInterval>;
  taintedAgents:Set<string>;
}

function nativeCompletedChild(row:any,sessionId:string):{path:string;agentId:string;turnId:string}|undefined{
  const event=row.payload,item=event?.item;
  if(row.type!=='event_msg'||event?.type!=='item_completed'||event.thread_id!==sessionId
    ||item?.type!=='SubAgentActivity'||item.kind!=='completed'||typeof item.id!=='string'||!item.id.startsWith(COMPLETION_PREFIX)
    ||typeof item.agent_thread_id!=='string'||typeof item.agent_path!=='string')return;
  return{path:item.agent_path,agentId:item.agent_thread_id,turnId:item.id.slice(COMPLETION_PREFIX.length)};
}

function nativeV2Final(row:any,queued:boolean):string|undefined{
  const item=row.payload;
  if(row.type!=='response_item'||row.metadata?.compaction_output
    ||item?.internal_chat_message_metadata_passthrough?.content_item_metadata?.[0]?.provenance?.type!=='tool'
    ||item.internal_chat_message_metadata_passthrough.content_item_metadata[0].provenance.namespace!=='collaboration'||!queued)return;
  return v2Result(item);
}

function modelActivity(row:any):{turnId:string;tool:boolean;sampled?:number}|undefined{
  const item=row.payload;
  if(row.type!=='response_item'||row.metadata?.compaction_output||!item)return;
  const turnId=item.internal_chat_message_metadata_passthrough?.turn_id??row.metadata?.retained_source?.id?.turn_id;
  if(typeof turnId!=='string'||!turnId)return;
  const tool=MODEL_TOOL_TYPES.has(item.type);
  if(tool||item.type==='reasoning'||item.type==='message'&&item.role==='assistant')return{turnId,tool,sampled:sequence(row.metadata?.sampled_through_sequence)};
}

                                              
export async function readCodexSampledSubagentCompletions(file:unknown,sessionId:string,parentTurnId:string,pending:readonly CodexSubagentReadCandidate[]):Promise<ReadonlySet<string>>{
  return parentTurnId&&pending.length?(await readContinuation(file,sessionId,pending,parentTurnId)).read:new Set();
}

                                
export function readCodexSubagentContinuation(file:unknown,sessionId:string,pending:readonly CodexSubagentReadCandidate[]):Promise<CodexSubagentContinuation>{
  return readContinuation(file,sessionId,pending);
}

async function readContinuation(file:unknown,sessionId:string,pending:readonly CodexSubagentReadCandidate[],expectedTurn?:string):Promise<CodexSubagentContinuation>{
  const empty=():CodexSubagentContinuation=>({verified:false,read:new Set(),continued:new Set()});
  if(typeof file!=='string'||!file||!sessionId)return empty();
  const fd=await open(file,'r').catch(()=>undefined);
  if(!fd)return empty();
  try{
    const descriptor=await fd.stat({bigint:true}),snapshot=transcriptFloor(descriptor),size=snapshot.offset;
    const stamp=transcriptStamp(descriptor),cacheKey=JSON.stringify([file,sessionId,expectedTurn]);
    const input=JSON.stringify(pending.map(({key,agentId,childTurnId,agentPath,floor})=>[key,agentId,childTurnId,agentPath,floor]));
    const cached=continuationCache.get(cacheKey);
    const stillCurrent=async()=>transcriptStamp(await stat(file,{bigint:true}))===stamp;
    if(cached&&cached.input===input&&transcriptStamp(cached.descriptor)===stamp)return await stillCurrent()?cached.proof:empty();
    const seamAt=async(offset:number)=>{
      const seam=Buffer.alloc(Math.min(APPEND_SEAM_BYTES,offset));
      const {bytesRead}=await fd.read(seam,0,seam.length,offset-seam.length);
      return bytesRead===seam.length?seam:undefined;
    };
                                       
    const prior=cached&&cached.input===input&&transcriptIdentity(cached.descriptor)===transcriptIdentity(descriptor)
      &&descriptor.size>cached.descriptor.size&&size-cached.cursor.start<=SAMPLE_WINDOW_BYTES+1
      &&(await seamAt(cached.cursor.offset))?.equals(cached.seam)?cached.cursor:undefined;
    const candidates=new Map<number,CodexSubagentReadCandidate[]>();
    let end=-1;
    if(!prior){
      const header=Buffer.alloc(Math.min(size,HEADER_BYTES)),head=await fd.read(header,0,header.length,0);
      end=header.subarray(0,head.bytesRead).indexOf(10);
      if(end<0)return empty();
      const meta=JSON.parse(header.subarray(0,end).toString('utf8'));
      if(meta.type!=='session_meta'||meta.payload?.id!==sessionId)return empty();
    }
    let start=prior?.start??Math.max(end+1,size-TAIL_BYTES)-1;
    for(const candidate of pending){
      const {floor}=candidate;
      if(sameTranscript(floor,snapshot,true)&&Number.isSafeInteger(floor.offset)&&floor.offset>end&&floor.offset<size&&size-floor.offset<=SAMPLE_WINDOW_BYTES){
        const atFloor=candidates.get(floor.offset);
        if(atFloor)atFloor.push(candidate);else candidates.set(floor.offset,[candidate]);
        if(!prior)start=Math.min(start,floor.offset-1);
      }
    }
    const cursor:ContinuationCursor=prior?structuredClone(prior):{offset:start,start,skippingLine:true,queued:false,mayInfer:start>end,inferredOpen:false,
      read:new Set(),continued:new Set(),waitingForTool:new Map(),unsampled:new Map(),intervals:new Map(),taintedAgents:new Set()};
    let position=cursor.offset,lineOffset=position,tail=Buffer.alloc(0);
    const {read,continued,waitingForTool,intervals,unsampled,taintedAgents}=cursor;
    const candidateFloors=[...candidates].sort(([left],[right])=>left-right);
    let candidateIndex=0;
    const taintCandidatesBefore=(resumeOffset:number):void=>{
      for(let entry;(entry=candidateFloors[candidateIndex])&&entry[0]<resumeOffset;candidateIndex++){
                                        
        if(entry[0]>=cursor.start)for(const candidate of entry[1])taintedAgents.add(candidate.agentId);
      }
      if(!intervals.size&&!taintedAgents.size)cursor.start=resumeOffset;
    };
    const resetEvidence=(canInfer:boolean):void=>{
      read.clear();continued.clear();waitingForTool.clear();unsampled.clear();
      cursor.openTurnId=cursor.closedTurnId=cursor.lastProgressAt=undefined;
      cursor.queued=cursor.inferredOpen=false;cursor.mayInfer=canInfer;
    };
                                          
    const evidenceGap=(resumeOffset:number):void=>{
      resetEvidence(true);
                                             
      for(const author of intervals.keys())intervals.set(author,{ambiguous:true});
      taintCandidatesBefore(resumeOffset);
    };
    const activity=(turnId:string,options:{sampled?:number;tool:boolean;canInfer:boolean;at:number}):void=>{
      const {sampled,tool,canInfer,at}=options;
      if(cursor.openTurnId===undefined&&cursor.mayInfer&&canInfer){cursor.openTurnId=turnId;cursor.mayInfer=false;cursor.inferredOpen=true;}
      else if(cursor.inferredOpen&&turnId!==cursor.openTurnId&&canInfer){
        cursor.openTurnId=undefined;cursor.lastProgressAt=undefined;cursor.inferredOpen=false;continued.clear();
      }
      if(turnId!==cursor.openTurnId)return;
      if(Number.isFinite(at))cursor.lastProgressAt=Math.max(cursor.lastProgressAt??at,at);
      if(tool)for(const [key,result] of waitingForTool){
        if(result.turnId===turnId){continued.add(key);waitingForTool.delete(key);}
      }
      if(sampled!==undefined&&(!expectedTurn||turnId===expectedTurn))for(const [key,result] of unsampled){
        if(result.turnId===turnId&&sampled>=result.sequence){read.add(key);unsampled.delete(key);}
      }
    };
    const accept=(row:any,offset:number):void=>{
      const previousQueued=cursor.queued,at=typeof row.timestamp==='string'?Date.parse(row.timestamp):NaN;
      cursor.queued=codexQueuedAgentMetadata(row);
      if(row.type==='compacted'){unsampled.clear();waitingForTool.clear();return;}
      const eventPayload=row.type==='event_msg'?row.payload:undefined;
      if(eventPayload&&(eventPayload.thread_id===undefined||eventPayload.thread_id===sessionId)){
        const {type,turn_id}=eventPayload;
        if(type==='thread_rolled_back'){
          resetEvidence(false);intervals.clear();taintedAgents.clear();cursor.start=offset+1;return;
        }
        if(START_EVENTS.has(type)){
          if(cursor.openTurnId!==turn_id)continued.clear();
          cursor.openTurnId=typeof turn_id==='string'&&turn_id?turn_id:undefined;
          cursor.closedTurnId=cursor.lastProgressAt=undefined;
          cursor.mayInfer=cursor.inferredOpen=false;return;
        }
        if(END_EVENTS.has(type)){
          cursor.openTurnId=cursor.lastProgressAt=undefined;cursor.mayInfer=cursor.inferredOpen=false;
          cursor.closedTurnId=typeof turn_id==='string'&&turn_id?turn_id:undefined;
          continued.clear();unsampled.clear();waitingForTool.clear();return;
        }
        if((type==='mcp_tool_call_begin'||type==='mcp_tool_call_end')&&typeof turn_id==='string'&&turn_id){
          const began=type==='mcp_tool_call_begin';activity(turn_id,{tool:began,canInfer:began,at});return;
        }
      }
      const event=nativeCompletedChild(row,sessionId);
      if(event){
        const previous=intervals.get(event.path);
        const matches=candidates.get(offset)?.filter(candidate=>candidate.agentId===event.agentId&&candidate.childTurnId===event.turnId
          &&(!candidate.agentPath||candidate.agentPath===event.path));
        const candidate=matches?.[0];
        const exact=matches?.every(match=>match.key===candidate?.key)?candidate:undefined;
                                         
        const ambiguous=taintedAgents.has(event.agentId)||!!previous&&(previous.ambiguous||!previous.result);
        intervals.set(event.path,{ambiguous,afterSequence:previous?.result?.sequence,candidate:exact});
        return;
      }
      const author=nativeV2Final(row,previousQueued);
      if(author){
        const interval=intervals.get(author);
        if(!interval)return;
        const text=row.payload.content[0].text as string,payload=text.slice(text.indexOf('\nPayload:\n')+'\nPayload:\n'.length);
                                                    
        if(payload.startsWith('Agent errored:')||payload.startsWith('Agent interrupted by Guardian:')||['Agent shut down.','Agent was not found.'].includes(payload))return;
        if(interval.result||interval.ambiguous){
          interval.ambiguous=true;
          if(interval.result){
            const {key}=interval.result;
            read.delete(key);continued.delete(key);unsampled.delete(key);waitingForTool.delete(key);
          }
          return;
        }
        const resultSequence=sequence(row.metadata?.history_sequence);
        if(resultSequence===undefined||resultSequence<=(interval.afterSequence??0)){interval.ambiguous=true;return;}
        const candidate=interval.candidate,key=candidate?.key??'',turnId=row.payload.internal_chat_message_metadata_passthrough.turn_id;
        interval.result={key,sequence:resultSequence,turnId:typeof turnId==='string'&&turnId?turnId:undefined};
        if(candidate&&interval.result.turnId){
          waitingForTool.set(key,interval.result);
          if(sameTranscript(candidate.floor,snapshot))unsampled.set(key,interval.result);
        }
        return;
      }
      const model=modelActivity(row);
      if(model&&(model.sampled!==undefined||model.turnId===cursor.openTurnId))activity(model.turnId,{...model,canInfer:model.sampled!==undefined,at});
    };
    while(position<size){
      const chunk=Buffer.allocUnsafe(Math.min(SAMPLE_CHUNK_BYTES,size-position));
      const {bytesRead}=await fd.read(chunk,0,chunk.length,position);
      if(!bytesRead)return empty();
      const buffer=tail.length?Buffer.concat([tail,chunk.subarray(0,bytesRead)]):chunk.subarray(0,bytesRead);
      let offset=0;
                                     
      if(cursor.skippingLine){
        const newline=buffer.indexOf(10);
        cursor.skippingLine=newline<0;offset=cursor.skippingLine?buffer.length:newline+1;
        taintCandidatesBefore(lineOffset+offset);
      }
      for(let next;(next=buffer.indexOf(10,offset))>=0;offset=next+1){
        let row:any;
        if(next-offset<=MAX_SAMPLE_LINE_BYTES)try{row=JSON.parse(buffer.subarray(offset,next).toString('utf8'));}catch{                   }
        if(typeof row?.type==='string')accept(row,lineOffset+offset);
        else evidenceGap(lineOffset+next+1);
      }
      if(buffer.length-offset>MAX_SAMPLE_LINE_BYTES){
        offset=buffer.length;evidenceGap(lineOffset+offset);cursor.skippingLine=true;
      }
      tail=Buffer.from(buffer.subarray(offset));
      position+=bytesRead;lineOffset+=offset;
    }
    cursor.offset=lineOffset;
    const seam=await seamAt(cursor.offset);
    if(!seam||!await stillCurrent())return empty();
    const {openTurnId,closedTurnId,lastProgressAt}=cursor;
    const proof=tail.length||cursor.skippingLine?empty():{verified:true,read,continued,...(openTurnId?{openTurnId}:{}),...(closedTurnId?{closedTurnId}:{}),
      ...(lastProgressAt===undefined?{}:{lastProgressAt})};
    continuationCache.delete(cacheKey);continuationCache.set(cacheKey,{descriptor,input,cursor,seam,proof});
    if(continuationCache.size>16)continuationCache.delete(continuationCache.keys().next().value!);
    return proof;
  }catch{return empty();}
  finally{await fd.close();}
}

                                           
export function readCodexSubagentCompletions(file:unknown,sessionId:string,parentTurnId:string,pending:readonly CodexSubagentReadCandidate[]):ReadonlySet<string>{
  if(!parentTurnId||!pending.length)return new Set();
  return withTranscript(file,sessionId,(fd,end)=>{
    const candidates=pending.filter(({floor})=>sameTranscript(floor,end)&&Number.isSafeInteger(floor.offset)&&floor.offset>=0&&floor.offset<end.offset);
    if(!candidates.length)return new Set<string>();
    const start=Math.max(0,end.offset-TAIL_BYTES),buffer=readBytes(fd,start,end.offset-start);
    const matches=new Map<string,{historySequence?:number;read:boolean}>(),ambiguous=new Set<string>();
    let turn:string|undefined,queuedAgentMetadata=false;
                                  
    let offset=start?buffer.indexOf(10)+1:0;
    if(start&&!offset)return new Set<string>();
    for(let next;(next=buffer.indexOf(10,offset))>=0;offset=next+1){
      const row=JSON.parse(buffer.subarray(offset,next).toString('utf8'));
      const payload=row.payload,queued=queuedAgentMetadata;
      queuedAgentMetadata=codexQueuedAgentMetadata(row);
      if(row.type==='event_msg'){
        if(['task_started','turn_started'].includes(payload?.type)){
          turn=payload.turn_id;
        }
        if(['task_complete','turn_complete','turn_aborted','thread_rolled_back'].includes(payload?.type))turn=undefined;
        if(payload?.type==='thread_rolled_back'){matches.clear();ambiguous.clear();}
      }
      if(row.type==='compacted'){matches.clear();ambiguous.clear();continue;}
      if(row.type!=='response_item'||!payload)continue;
      const reference=codexChildResult(payload,queued);
      if(reference){
        const compatible=candidates.filter(candidate=>start+offset>=candidate.floor.offset&&(candidate.agentId===reference||candidate.agentPath===reference));
        for(const candidate of compatible){
          if(compatible.length!==1||matches.has(candidate.key))ambiguous.add(candidate.key);
          else matches.set(candidate.key,{historySequence:sequence(row.metadata?.history_sequence),read:false});
        }
      }else if(turn===parentTurnId&&payload.type==='message'&&payload.role==='assistant'&&!row.metadata?.compaction_output&&Array.isArray(payload.content)&&payload.content.some((entry:any)=>entry?.type==='output_text'&&typeof entry.text==='string'&&entry.text.trim())){
        const sampled=sequence(row.metadata?.sampled_through_sequence);
        for(const match of matches.values()){
                                                    
          if(match.historySequence===undefined?sampled===undefined:sampled!==undefined&&sampled>=match.historySequence)match.read=true;
        }
      }
    }
    return new Set(turn===parentTurnId?[...matches].filter(([key,match])=>match.read&&!ambiguous.has(key)).map(([key])=>key):[]);
  })??new Set();
}
