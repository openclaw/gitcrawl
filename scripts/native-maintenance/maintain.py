#!/usr/bin/env python3
"""Native-owned pending-first embedding/cluster maintenance. No model credentials."""
import argparse,datetime,fcntl,hashlib,json,os,pathlib,sqlite3,subprocess,time,uuid
import retention

def sha(raw):return hashlib.sha256(raw).hexdigest()
def now():return datetime.datetime.now(datetime.timezone.utc).isoformat()
def atomic(path,raw):
 path=pathlib.Path(path);tmp=path.with_name(path.name+'.tmp-'+uuid.uuid4().hex)
 with open(tmp,'xb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
 os.replace(tmp,path)
 fd=os.open(path.parent,os.O_RDONLY)
 try:os.fsync(fd)
 finally:os.close(fd)
def save(path,obj):atomic(path,(json.dumps(obj,sort_keys=True)+'\n').encode())
def invoke(argv,data=None,timeout=90):
 r=subprocess.run(argv,input=data,capture_output=True,timeout=timeout)
 if r.returncode:raise RuntimeError('command failed: '+r.stderr.decode(errors='replace')[-1200:])
 return r.stdout

class Cycle:
 def __init__(self,config,call=invoke):
  self.c=config;self.call=call;self.base=pathlib.Path(config['state_dir']);self.base.mkdir(mode=0o700,parents=True,exist_ok=True)
  self.statepath=self.base/'state.json';self.state=json.loads(self.statepath.read_bytes()) if self.statepath.exists() else {'retained':[]}
  self.policy=self.base/'consumer-exclusions.json'
 def checkpoint(self):save(self.statepath,self.state)
 def broker(self,mode,raw=b''):return json.loads(self.call(self.c['broker']+[mode],raw))
 def policy_refresh(self):
  raw=self.call(self.c['broker']+['exclusions'],b'');p=json.loads(raw)
  if len(raw)>1<<20 or p.get('format')!='gitcrawl.embedding.exclusions.v1' or p.get('dataset')!='gitcrawl/history/threads' or not isinstance(p.get('excluded_thread_ids'),list):raise RuntimeError('invalid installed consumer policy')
  for x in p['excluded_thread_ids']:
   if not isinstance(x,str) or not x.isascii() or not x.isdigit() or str(int(x))!=x or int(x)<=0:raise RuntimeError('invalid consumer native ID')
  atomic(self.policy,raw);return sha(raw)
 def source_ready(self):
  db=self.c['db'];handoff=pathlib.Path(self.c['handoff_state'])/'last-handoff.json';last=json.loads(handoff.read_bytes()).get('restored_at',0) if handoff.exists() else 0
  with sqlite3.connect('file:'+db+'?mode=ro',uri=True,timeout=5) as con:
   row=con.execute("select through,observed_at,complete from analytics_coverage where repository='openclaw/openclaw'").fetchone()
  if not row:return False
  at=datetime.datetime.fromisoformat(row[1].replace('Z','+00:00')).timestamp()
  self.state['core']={'through':row[0],'observed_at':row[1],'complete':row[2]}
  return row[2]==1 and at>last and time.time()-at<=300
 def new_packet(self):
  self.policy_refresh();token=uuid.uuid4().hex;request=self.base/(token+'-request.json')
  if sha(pathlib.Path(self.c['binary']).read_bytes())!=self.c['binary_sha256']:raise RuntimeError('native exporter binary changed')
  limit=2000
  while True:
   try:
    export=json.loads(self.call([self.c['binary'],'embedding-export','--db',self.c['db'],'--consumer-exclusions',str(self.policy),'--limit',str(limit),'--output',str(request)],timeout=45));break
   except RuntimeError as e:
    if 'request exceeds16MiB' not in str(e) or limit==1:raise
    limit=max(1,limit//2)
  export['task_cap']=limit
  if export['tasks']==0:
   self.state['last_empty_export']=export;self.checkpoint();request.unlink(missing_ok=True);return False
  raw=request.read_bytes()
  if json.loads(raw).get('scope',{}).get('max_tasks')!=limit:raise RuntimeError('native task cap binding mismatch')
  if sha(raw)!=export['request_sha256'] or len(raw)>16<<20:raise RuntimeError('native export digest/bound mismatch')
  self.state['pending']={'request':request.name,'sha256':sha(raw),'export':export,'created_at':now()};self.checkpoint();return True
 def retirement_scope(self):return retention.capture_scope(self)
 def zero_export(self):
  # One bounded read-only eligibility check after publication, never model work.
  policy=self.policy_refresh();before=retention.archive_identity(self);path=self.base/(uuid.uuid4().hex+'-zero-request.json')
  if sha(pathlib.Path(self.c['binary']).read_bytes())!=self.c['binary_sha256']:raise RuntimeError('native exporter binary changed')
  try:
   result=json.loads(self.call([self.c['binary'],'embedding-export','--db',self.c['db'],'--consumer-exclusions',str(self.policy),'--limit','1','--output',str(path)],timeout=45))
   after=retention.archive_identity(self)
   if before!=after:raise RuntimeError('zero-export source identity changed')
   result.update(source_identity=after,consumer_exclusions_sha256=policy,exporter_sha256=self.c['binary_sha256']);return result
  finally:path.unlink(missing_ok=True)
 def checked_broker(self,p,mode,raw):
  try:return self.broker(mode,raw)
  except RuntimeError:
   if self.policy_changed(p):self.invalidate_policy(p);return None
   raise
 def record_for(self,pending):
  # Durable native receipts survive a killed controller/host orchestration.
  candidates=[pathlib.Path(pending['native_receipt'])] if pending.get('native_receipt') else list(pathlib.Path(self.c['handoff_state']).glob('*-native-import.json'))+list(pathlib.Path(self.c['handoff_state']).glob('*-native-combined.json'))
  for p in candidates:
   r=json.loads(p.read_bytes())
   if r.get('request_sha256')==pending['sha256'] and (r.get('committed_at') or (r.get('imported')==0 and r.get('unchanged',0)>0)):return p,r
  return None,None
 def verify_import(self,pending,result):
  path,receipt=self.record_for(pending)
  if not receipt:return False
  req=json.loads((self.base/pending['request']).read_bytes());outputs={x['task_id']:x for x in result['results']}
  with sqlite3.connect('file:'+self.c['db']+'?mode=ro',uri=True,timeout=5) as con:
   for t in req['tasks']:
    v=outputs[t['task_id']];row=con.execute('select content_hash,dimensions,vector_json from thread_vectors where thread_id=? and basis=? and model=?',(t['thread_binding']['thread_id'],req['basis'],req['model'])).fetchone()
    if v['input_sha256']!=t['input_sha256'] or not row or row[0]!=t['native_content_hash'] or row[1]!=1536 or json.loads(row[2])!=v['vector']:raise RuntimeError('durable import no longer matches current native vector; owner review required')
  pending['native_receipt']=str(path);pending['native_receipt_sha256']=sha(path.read_bytes());pending['matched']=len(req['tasks'])
  if receipt.get('format')=='gitcrawl.combined-producer.v1':pending['native_publication']=receipt
  self.checkpoint();return True
 def release_proof(self,p):
  request=(self.base/p['request']).read_bytes();result=(self.base/p['result']).read_bytes();receipt=pathlib.Path(p['native_receipt']).read_bytes();r=json.loads(receipt)
  if sha(request)!=p['sha256'] or sha(result)!=p['result_sha256'] or sha(receipt)!=p['native_receipt_sha256']:raise RuntimeError('release-intent local proof checksum mismatch')
  if r.get('request_sha256')!=p['sha256'] or r.get('response_sha256')!=p['result_sha256'] or r.get('imported',0)+r.get('unchanged',0)!=p['matched'] or len(json.loads(request)['tasks'])!=p['matched']:raise RuntimeError('release-intent native receipt binding mismatch')
 def finish(self,pending):
  if pending.get('stage')!='released':
   self.release_proof(pending)
   if pending.get('stage')!='release_intent':
    req=json.loads((self.base/pending['request']).read_bytes());pending['native_bindings']=[{k:v for k,v in t.items() if k!='input'} for t in req['tasks']]
    pending['request_header']={k:v for k,v in req.items() if k!='tasks'};pending['stage']='release_intent';self.checkpoint()
   # A killed process here resumes from durable local import proof, bypassing
   # broker result. Release is idempotent, including an already released request.
   released=json.loads(self.call(self.c['broker']+['release',pending['sha256']],b''));pending['release']=released;pending['finished_at']=now();pending['stage']='released';self.checkpoint()
  save(self.base/(pending['sha256']+'-receipt.json'),pending)
  # Keep released completion ownership until retirement intent is durable.
  # Restart re-enters finish without fetching/resubmitting the broker result.
  if pending.get('request_header') and pending.get('native_publication',{}).get('format')=='gitcrawl.combined-producer.v1':
   retention.retire_completed(self,self.base/(pending['sha256']+'-receipt.json'))
   if pending.get('retirement_scope_before') and self.state['retained']:
    retention.retire_zero_scope(self,pending['retirement_scope_before'],pending['native_publication'],self.zero_export())
  for name in [pending['request'],pending.get('result')]:
   if name and pathlib.Path(name).name==name:(self.base/name).unlink(missing_ok=True)
  self.state.pop('pending',None);self.state['last_success']=now();self.checkpoint()
 def policy_changed(self,p):
  current=self.policy_refresh();raw=(self.base/p['request']).read_bytes();request=json.loads(raw)
  return current!=request.get('source_binding',{}).get('consumer_exclusions_sha256')
 def invalidate_policy(self,p):
  if len(self.state['retained'])>=16:raise RuntimeError('retained replacement cap reached; operator review')
  request=json.loads((self.base/p['request']).read_bytes());policy=json.loads(self.policy.read_bytes());blocked=set(policy['excluded_thread_ids'])
  p['invalidated']='consumer_policy_changed';p['native_bindings']=[{k:v for k,v in t.items() if k!='input'} for t in request['tasks']]
  p.pop('retirement_scope_before',None)
  self.state['retained'].append(p);self.state.pop('pending');self.checkpoint()
  # Newly removed input is never relabelled or sent again. Drop only exclusive
  # local copies; retain request digest so still-allowed broker jobs can be reused.
  if any(t.get('thread_binding',{}).get('thread_id') in blocked for t in request['tasks']):
   for name in [p.get('request'),p.get('result')]:
    if name and pathlib.Path(name).name==name:(self.base/name).unlink(missing_ok=True)
 def finish_scope_retirement(self):
  work=self.state.get('scope_retirement_pending')
  if work:
   if self.state['retained']:retention.retire_zero_scope(self,work['before'],work['publication'],self.zero_export())
   self.state.pop('scope_retirement_pending');self.checkpoint()
 def run(self):
  retention.resume(self)
  self.finish_scope_retirement()
  # At most one successful handoff. Stale preview gets one fresh replacement;
  # pending provider work always resumes before a new export.
  for refresh in range(2):
   p=self.state.get('pending')
   if p and p.get('stage') in ('released','release_intent'):self.finish(p);return {'status':'recovered_release'}
   if not p:
    if not self.new_packet():
     retention.resume(self)
     if not self.source_ready():self.checkpoint();return {'status':'waiting_core'}
     before=self.retirement_scope() if self.state['retained'] else None
     raw=self.call(self.c['cluster_invocation'],timeout=420)
     self.state['cluster']=json.loads(raw) if raw.strip() else {'status':'controller_busy'}
     if before and self.state['cluster'].get('phase') in ('committed','unchanged'):self.state['scope_retirement_pending']={'before':before,'publication':self.state['cluster']}
     self.checkpoint();self.finish_scope_retirement()
     return {'status':'vectors_current','cluster':self.state['cluster']}
    p=self.state['pending']
   if self.policy_changed(p):self.invalidate_policy(p);continue
   raw=(self.base/p['request']).read_bytes()
   if sha(raw)!=p['sha256']:raise RuntimeError('persisted request checksum mismatch')
   if 'inspect' not in p:
    result=self.checked_broker(p,'inspect',raw)
    if result is None:continue
    p['inspect']=result;self.checkpoint()
   if 'submit' not in p:
    result=self.checked_broker(p,'submit',raw)
    if result is None:continue
    p['submit']=result;self.checkpoint()
   until=time.monotonic()+120
   while True:
    result=self.checked_broker(p,'result',raw)
    if result is None:break
    p['broker_status']={k:v for k,v in result.items() if k!='results'};self.checkpoint()
    if result.get('status')=='complete':break
    if result.get('failed'):return {'status':'provider_failure','receipt':p['broker_status']}
    if time.monotonic()>=until:return {'status':'provider_pending','receipt':p['broker_status']}
    time.sleep(3)
   if self.state.get('pending') is not p:continue
   resultName=p['sha256']+'-result.json';save(self.base/resultName,result);p['result']=resultName;p['result_sha256']=sha((self.base/resultName).read_bytes());self.checkpoint()
   if self.verify_import(p,result):self.finish(p);return {'status':'recovered_import','tasks':p['matched']}
   if not self.source_ready():self.checkpoint();return {'status':'waiting_core','request_sha256':p['sha256']}
   self.policy_refresh();log=self.base/(p['sha256']+'-import.log');p['import_log']=log.name;self.checkpoint()
   cap=json.loads(raw)['scope']['max_tasks']
   if type(cap) is not int or not 1<=cap<=2000:raise RuntimeError('invalid native packet task cap')
   p['export']['task_cap']=cap;self.checkpoint()
   catchup=p['export']['tasks']>=cap
   command=self.c['import_invocation'] if catchup else self.c['combined_invocation']
   mode='embedding-import' if catchup else 'combined'
   argv=command+['--mode',mode,'--request',str(self.base/p['request']),'--response',str(self.base/resultName),'--consumer-exclusions',str(self.policy)]
   before=self.retirement_scope() if mode=='combined' and self.state['retained'] else None
   if before:p['retirement_scope_before']=before;self.checkpoint()
   try:
    data=self.call(argv,timeout=420 if mode=='combined' else 150);atomic(log,data)
   except RuntimeError as e:
    p['error']=str(e);self.checkpoint()
    # Preview must have failed before settlement. Transaction/restore failures
    # receive normal cadence/operator attention, never immediate crash retry.
    failurepath=pathlib.Path(self.c['handoff_state'])/'last-failure.json'
    failure=json.loads(failurepath.read_bytes()) if failurepath.exists() else {}
    if (failure.get('phase')=='prepare' or 'consumer-excluded task' in str(e)) and self.policy_changed(p):self.invalidate_policy(p);continue
    if failure.get('phase')=='prepare' and any(reason in str(e) for reason in ['stale native identity/clock task','native combined input/curation changed during compute','native cluster coverage incomplete']):
     if len(self.state['retained'])>=16:raise RuntimeError('retained replacement cap reached; operator review')
     p.pop('retirement_scope_before',None)
     self.state['retained'].append(p);self.state.pop('pending');self.checkpoint();continue
    raise
   if not data.strip():return {'status':'controller_busy'}
   summary=json.loads(data.splitlines()[-1]);p['native_receipt']=summary.get('receipt');p['native_publication']=summary;self.checkpoint()
   if not self.verify_import(p,result):raise RuntimeError('native durable receipt missing; broker retained')
   self.finish(p)
   return {'status':'catchup_imported' if catchup else 'combined_published','tasks':p['matched'],'native':summary}
  return {'status':'replacement_pending'}

def main():
 os.umask(0o077);p=argparse.ArgumentParser();p.add_argument('--config',required=True);p.add_argument('--config-sha256',required=True);p.add_argument('--retire-completed');p.add_argument('--retained-digest');a=p.parse_args();
 if bool(a.retire_completed)!=bool(a.retained_digest):raise SystemExit('retirement requires completion receipt and exact retained digest')
 raw=pathlib.Path(a.config).read_bytes()
 if sha(raw)!=a.config_sha256:raise SystemExit('installed config checksum mismatch')
 c=Cycle(json.loads(raw));lock=open(c.base/'orchestrator.lock','a')
 try:fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB)
 except BlockingIOError:return
 c.state=json.loads(c.statepath.read_bytes()) if c.statepath.exists() else {'retained':[]}
 try:
  if a.retire_completed:result=retention.retire_completed(c,a.retire_completed,target=a.retained_digest,legacy=True);print(json.dumps(result));return
  result=c.run();save(c.base/'last-result.json',{'at':now(),**result});print(json.dumps(result))
 except BaseException as e:save(c.base/'last-failure.json',{'at':now(),'error':str(e)});raise
if __name__=='__main__':main()
