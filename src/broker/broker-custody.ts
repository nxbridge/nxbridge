import type {OwnedCodexLease} from '../hosts/codex/codex-owned-routes.js';
import type {SessionProfile} from '../hosts/codex/codex-session-profile.js';

                                
export interface CustodyOffer extends OwnedCodexLease {
  id:string;
  groupId:string;
  configHome?:string;
  profile?:SessionProfile;
}

export interface ParkedCustody {
  offers:readonly CustodyOffer[];
  live(id:string):boolean;
  release(id:string):void;
  close():void;
}

export interface CustodyAcquisition {
  offers:readonly CustodyOffer[];
  signal:AbortSignal;
  verify(id:string):Promise<boolean>;
  resolve(ids:readonly string[],outcome:'transferred'|'held'):Promise<void>;
  complete?(counts:{transferred:number;held:number}):void;
}

export interface CustodySeal {
  updateId:string;
  signal:AbortSignal;
                                
  commit():void;
}
