#!/usr/bin/env python3
"""Official-main immutable code producer; existing native handoff lifecycle."""
from upstream import SourceCache
import argparse,fcntl,json,os,plistlib,select,signal,subprocess,sys,time,hashlib,uuid
from pathlib import Path

def digest(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def atomic(path,value):
 path=Path(path);tmp=path.with_name(path.name+'.tmp-'+uuid.uuid4().hex)
 with open(tmp,'x',encoding='utf8') as f:
  os.chmod(tmp,0o600);json.dump(value,f,sort_keys=True);f.write('\n');f.flush();os.fsync(f.fileno())
 os.replace(tmp,path)
 fd=os.open(path.parent,os.O_RDONLY)
 try:os.fsync(fd)
 finally:os.close(fd)
def command(argv,timeout=20):return subprocess.run(argv,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,timeout=timeout)

class Lifecycle:
 def __init__(self,a,run=command):
  self.a=a;self.run=run;self.state=Path(a.state_dir);self.state.mkdir(parents=True,exist_ok=True,mode=0o700)
  self.journal=self.state/'handoff.json';self.target=f'gui/{os.getuid()}/{a.label}'
 def verify(self):
  for path,want in [(self.a.binary,self.a.binary_sha256),(self.a.job_plist,self.a.plist_sha256),(self.a.config,self.a.config_sha256)]:
   if digest(path)!=want:raise RuntimeError('pinned producer/job/config changed: '+path)
  spec=plistlib.loads(Path(self.a.job_plist).read_bytes())
  if spec.get('Label')!=self.a.label:raise RuntimeError('job label mismatch')
  args=spec.get('ProgramArguments',[])
  if not args or args[0]!=self.a.collector_binary or 'analytics' not in args or '--watch' not in args or self.a.config not in args:raise RuntimeError('unexpected analytics job')
 def loaded(self):return self.run(['launchctl','print',self.target]).returncode==0
 def restore(self,record):
  # A prior disabled/unloaded job is never enabled by this controller.
  if record['was_loaded']:
   if digest(self.a.job_plist)!=record['plist_sha256'] or digest(self.a.config)!=record['config_sha256']:raise RuntimeError('original job/config changed during handoff; restoration requires owner review')
   if not self.loaded():
    result=self.run(['launchctl','bootstrap',f'gui/{os.getuid()}',self.a.job_plist])
    if result.returncode and not self.loaded():raise RuntimeError('original analytics bootstrap failed')
   if not self.loaded():raise RuntimeError('original analytics job not loaded after restoration')
  record['restored_at']=time.time();record['phase']='restored';atomic(self.state/'last-handoff.json',record)
  if self.journal.exists():self.journal.unlink()
 def recover(self):
  if not self.journal.exists():return False
  record=json.loads(self.journal.read_text());self.restore(record);atomic(self.state/'last-result.json',{'status':'interrupted_handoff_restored','index_committed':'unknown','previous':record});return True
 def update(self):
  if self.recover():return
  self.verify()
  runid=uuid.uuid4().hex;gate=self.state/(runid+'.gate');record=None;child=None
  captured=None
  env=dict(os.environ,GIT_OPTIONAL_LOCKS='0',GIT_NO_LAZY_FETCH='1',GIT_TERMINAL_PROMPT='0',GOMAXPROCS='2')
  argv=[self.a.binary,'--db',self.a.db,'--commit-gate',str(gate)]
  try:
   captured,proof=SourceCache(self.state,self.a.source).capture(runid)
   argv += ['--source',captured['git_dir'],'--commit',captured['commit'],'--upstream-receipt',str(proof),'--write-timeout',f'{self.a.write_seconds}s']
   child=subprocess.Popen(argv,stdout=subprocess.PIPE,stderr=subprocess.PIPE,text=True,env=env)
   if not select.select([child.stdout],[],[],65)[0]:raise RuntimeError('native prepare timed out')
   line=child.stdout.readline()
   if not line:
    _,err=child.communicate(timeout=5);raise RuntimeError('native prepare failed: '+err[-2000:])
   prepared=json.loads(line)
   if prepared.get('git_sha')!=captured['commit'] or prepared.get('tracked_dirty') is not False or prepared.get('source_root')!=captured['git_dir'] or prepared.get('upstream',{}).get('receipt_sha256')!=digest(proof):raise RuntimeError('prepared upstream binding mismatch')
   if prepared.get('phase')=='unchanged':
    out,err=child.communicate(timeout=5)
    if child.returncode:raise RuntimeError('native no-op failed: '+err[-1000:])
    atomic(self.state/(runid+'-code-result.json'),prepared);atomic(self.state/'last-code-result.json',prepared);atomic(self.state/'last-result.json',prepared);print(json.dumps(prepared));return
   if prepared.get('phase')!='prepared':raise RuntimeError('unexpected native producer phase')
   self.verify();was_loaded=self.loaded();record={'phase':'settling','was_loaded':was_loaded,'plist_sha256':self.a.plist_sha256,'config_sha256':self.a.config_sha256,'prepared':prepared,'started_at':time.time()};atomic(self.journal,record)
   if was_loaded:
    result=self.run(['launchctl','bootout',self.target])
    if result.returncode:raise RuntimeError('ordinary analytics settlement failed')
   # Wait for graceful native exit; never steal/delete the existing lock.
   until=time.monotonic()+20
   while True:
    with open(Path(self.a.db).parent/'runner.lock','rb') as probe:
     try:fcntl.flock(probe,fcntl.LOCK_EX|fcntl.LOCK_NB);break
     except BlockingIOError:
      if time.monotonic()>=until:raise RuntimeError('native owner did not settle within20s')
      time.sleep(.1)
   # Child checks source identity and takes the original runner.lock itself.
   with open(gate,'x',encoding='ascii') as f:
    os.chmod(gate,0o600);f.write(prepared['content_digest']);f.flush();os.fsync(f.fileno())
   out,err=child.communicate(timeout=self.a.write_seconds+20)
   if child.returncode:raise RuntimeError('native atomic commit failed: '+err[-2000:])
   lines=[x for x in out.splitlines() if x.strip()]
   if not lines:raise RuntimeError('native commit receipt missing')
   result=json.loads(lines[-1])
   if result.get('phase')!='committed':raise RuntimeError('native commit receipt invalid')
   atomic(self.state/(runid+'-commit.json'),result);atomic(self.state/'last-code-result.json',result);record['commit_receipt']=runid+'-commit.json';record['phase']='committed';atomic(self.journal,record)
   self.restore(record);record=None;atomic(self.state/'last-result.json',result);print(json.dumps(result))
  except BaseException as e:
   atomic(self.state/'last-failure.json',{'at':time.time(),'error':str(e),'phase':record['phase'] if record else 'prepare','run_id':runid,'upstream':captured})
   raise
  finally:
   if child is not None and child.poll() is None:
    child.terminate()
    try:child.wait(timeout=5)
    except subprocess.TimeoutExpired:child.kill();child.wait(timeout=5)
   if gate.exists():gate.unlink()
   if record is not None:self.restore(record)

def main():
 p=argparse.ArgumentParser()
 for name in ['binary','binary-sha256','db','source','state-dir','job-plist','plist-sha256','config','config-sha256','collector-binary'] :p.add_argument('--'+name,required=True)
 p.add_argument('--label',default='com.hrudolph.gitcrawl-analytics');p.add_argument('--write-seconds',type=int,default=90);a=p.parse_args()
 if not 1<=a.write_seconds<=120:raise SystemExit('write deadline must be1..120s')
 os.umask(0o077);control=Lifecycle(a);lock=open(control.state/'controller.lock','a');os.chmod(control.state/'controller.lock',0o600)
 try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 except BlockingIOError:return
 def stop(sig,frame):raise KeyboardInterrupt('controller interrupted')
 signal.signal(signal.SIGTERM,stop);signal.signal(signal.SIGINT,stop)
 control.update()
if __name__=='__main__':main()
