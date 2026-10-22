import { AttemptsCycleRunner } from './cycle-runner.js';
export type AcquisitionState = 'disabled'|'blocked'|'running'|'needs_reconciliation'|'stopped';
export interface CycleCounts { received:number;persisted:number;duplicates:number;rejected:number;acquisitionPerformed?:boolean; }
export interface WorkerRuntime {
  start():Promise<void>;stop():Promise<void>;
  snapshot():Record<string,unknown>;ready():boolean;
}
export class AttemptsRuntimeControl {
  private runner?:AttemptsCycleRunner;
  private state:AcquisitionState='disabled';
  private blockers:readonly string[]=[];
  private lastStartedAt:string|null=null;
  private lastSuccessAt:string|null=null;
  private lastFailureAt:string|null=null;
  private lastAcquisitionAt:string|null=null;
  private completedCycles=0;
  private inFlight=false;
  private counts:CycleCounts={received:0,persisted:0,duplicates:0,rejected:0};
  constructor(private readonly intervalMs=60000){}
  block(codes:readonly string[]) { if(this.runner)throw Error('ACQUISITION_ALREADY_INITIALIZED');this.state='blocked';this.blockers=[...codes]; }
  requireReconciliation(){const first=this.state!=='needs_reconciliation';this.state='needs_reconciliation';this.lastFailureAt=new Date().toISOString();this.blockers=['ACQUISITION_OR_DELIVERY_REQUIRES_RECONCILIATION'];
    if(first)process.stderr.write(`${JSON.stringify({level:50,event:'worker_needs_reconciliation',at:this.lastFailureAt})}\n`);
    void this.runner?.stop();}
  start(cycle:(signal:AbortSignal)=>Promise<CycleCounts|void>):void {
    if(this.runner || this.state==='stopped')throw Error('ACQUISITION_ALREADY_INITIALIZED');
    this.runner=new AttemptsCycleRunner(async signal=>{
      this.inFlight=true;this.lastStartedAt=new Date().toISOString();
      try {const counts=await cycle(signal);if(counts)this.counts={received:counts.received,persisted:counts.persisted,duplicates:counts.duplicates,rejected:counts.rejected,acquisitionPerformed:counts.acquisitionPerformed===true};this.completedCycles++;this.lastSuccessAt=new Date().toISOString();if(counts?.acquisitionPerformed)this.lastAcquisitionAt=this.lastSuccessAt;}
      catch {this.requireReconciliation();throw Error('RECOVERY_REQUIRED');}
      finally {this.inFlight=false;}
    },this.intervalMs);
    this.state='running';this.blockers=[];this.runner.start();
  }
  async stop():Promise<void>{await this.runner?.stop();if(this.state!=='needs_reconciliation')this.state='stopped';}
  snapshot(){return {nextExecutionAt:this.runner?.nextExecutionAt??null,state:this.state,blockers:this.blockers,armed:this.state==='running',enabled:this.state==='running' && this.lastAcquisitionAt!==null,implemented:true,intervalMs:this.intervalMs,inFlight:this.inFlight,lastAcquisitionAt:this.lastAcquisitionAt,
    completedCycles:this.completedCycles,lastCycleStartedAt:this.lastStartedAt,lastCycleCompletedAt:this.lastSuccessAt,lastSuccessAt:this.lastSuccessAt,
    lastFailure:this.lastFailureAt?{at:this.lastFailureAt,code:'ACQUISITION_OR_DELIVERY_REQUIRES_RECONCILIATION'}:null,...this.counts};}
}
