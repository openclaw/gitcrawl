import importlib.util,json,pathlib,tempfile,unittest,os
spec=importlib.util.spec_from_file_location('maintain',pathlib.Path(__file__).with_name('maintain.py'));m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
class Tests(unittest.TestCase):
 def make(self):
  t=tempfile.TemporaryDirectory();self.addCleanup(t.cleanup);p=pathlib.Path(t.name);calls=[]
  def call(argv,data=None,timeout=90):calls.append(argv);return b'{}'
  c=m.Cycle({'state_dir':str(p),'handoff_state':str(p/'handoff'),'broker':['fixture-broker'],'db':str(p/'absent'),'cluster_invocation':['fixture-cluster'],'import_invocation':['fixture-import'],'combined_invocation':['fixture-combined']},call);c.validate_pending=lambda p:None;c.retirement_scope=lambda:None;c.await_core_boundary=lambda:{};c.controller_idle=lambda:True;c.archive_superseded=lambda:{'archived':[]};return c,p,calls
 def pending(self,c,p):
  raw=b'{"scope":{"max_tasks":2000},"source_binding":{"consumer_exclusions_sha256":"policy"},"tasks":[{"task_id":"t","input":"native","thread_binding":{"thread_id":"1"}}]}';(p/'request.json').write_bytes(raw);c.state['pending']={'request':'request.json','sha256':m.sha(raw),'export':{'tasks':1},'inspect':{'tasks':1},'submit':{'status':'submitted'}};c.checkpoint();c.policy_refresh=lambda:'policy'
 def test_pending_resumed_before_export_or_submit(self):
  c,p,calls=self.make();self.pending(c,p);c.new_packet=lambda:self.fail('exported despite pending');c.broker=lambda mode,raw:({'status':'pending','pending':1,'failed':1} if mode=='result' else self.fail('resubmitted'))
  self.assertEqual(c.run()['status'],'provider_failure');self.assertIn('pending',c.state);self.assertEqual(calls,[])
 def test_ready_waits_for_core_without_settlement(self):
  c,p,calls=self.make();self.pending(c,p);c.broker=lambda mode,raw:{'status':'complete','results':[]};c.source_ready=lambda:False
  self.assertEqual(c.run()['status'],'waiting_core');self.assertTrue((p/c.state['pending']['result']).exists());self.assertEqual(calls,[])
 def test_ready_pending_waits_for_overlapping_controller_before_import(self):
  c,p,calls=self.make();self.pending(c,p);waits=[]
  c.broker=lambda mode,raw:{'status':'complete','results':[]};c.controller_idle=lambda:False;c.await_core_boundary=lambda:waits.append(True);c.source_ready=lambda:True
  verified=iter([False,True]);c.verify_import=lambda pending,result:(pending.update({'matched':1}) or next(verified));c.finish=lambda p:None
  c.call=lambda argv,data=None,timeout=90:b'{"phase":"committed","receipt":"native"}'
  self.assertEqual(c.run()['status'],'combined_published');self.assertEqual(waits,[True])
 def test_crash_after_release_resumes_cleanup_without_provider(self):
  c,p,calls=self.make();c.state['pending']={'stage':'released','sha256':'frozen','request':'already-cleaned.json','result':'already-cleaned-result.json','native_bindings':[{'input_sha256':'hash'}]};c.checkpoint()
  self.assertEqual(c.run()['status'],'recovered_release');self.assertNotIn('pending',c.state);self.assertEqual(calls,[]);self.assertTrue((p/'frozen-receipt.json').exists())
 def test_zero_scope_keeps_unproven_retained_then_native_noop(self):
  c,p,calls=self.make();c.state['retained']=[{'sha256':'old','request':'old.json'}];(p/'old.json').write_text('{}');c.new_packet=lambda:False;c.source_ready=lambda:True
  def call(argv,data=None,timeout=90):calls.append(argv);return b'{"phase":"unchanged"}' if argv==['fixture-cluster'] else b'{"status":"released"}'
  c.call=call;self.assertEqual(c.run()['status'],'vectors_current');self.assertEqual(calls,[['fixture-cluster']]);self.assertTrue((p/'old.json').exists());self.assertEqual(len(c.state['retained']),1)
 def test_live_policy_bytes_are_stable_and_exact(self):
  c,p,calls=self.make();raw=b'{"format":"gitcrawl.embedding.exclusions.v1","dataset":"gitcrawl/history/threads","excluded_thread_ids":["12"],"revision":"stable"}\n';c.call=lambda argv,data=None,timeout=90:raw
  self.assertEqual(c.policy_refresh(),m.sha(raw));self.assertEqual(c.policy.read_bytes(),raw);self.assertEqual(c.policy_refresh(),m.sha(raw))
 def test_stale_preview_retains_old_before_replacement(self):
  c,p,calls=self.make();self.pending(c,p);old=c.state['pending']['sha256'];c.source_ready=lambda:True;c.policy_refresh=lambda:'policy';(p/'handoff').mkdir();(p/'handoff'/'last-failure.json').write_text('{"phase":"prepare"}')
  def broker(mode,raw):
   if json.loads(raw).get('new'):return {'status':'pending','failed':1}
   return {'status':'complete','results':[]}
  def call(argv,data=None,timeout=90):raise RuntimeError('native prepare failed: stale native identity/clock task abc')
  def new():
   self.assertEqual(c.state['retained'][0]['sha256'],old);newraw=b'{"new":true,"source_binding":{"consumer_exclusions_sha256":"policy"},"tasks":[]}';(p/'new.json').write_bytes(newraw);c.state['pending']={'request':'new.json','sha256':m.sha(newraw),'inspect':{},'submit':{}};c.checkpoint();return True
  c.broker=broker;c.call=call;c.new_packet=new;self.assertEqual(c.run()['status'],'provider_failure');self.assertEqual(len(c.state['retained']),1);self.assertTrue((p/'request.json').exists())

class RecoveryTests(Tests):
 def test_crash_exactly_after_release_before_checkpoint(self):
  c,p,calls=self.make();self.pending(c,p);x=c.state['pending'];result=b'{"results":[]}';(p/'result.json').write_bytes(result);native=p/'native.json';m.save(native,{'request_sha256':x['sha256'],'response_sha256':m.sha(result),'imported':1,'unchanged':0,'committed_at':'t'})
  x.update({'result':'result.json','result_sha256':m.sha(result),'native_receipt':str(native),'native_receipt_sha256':m.sha(native.read_bytes()),'matched':1});c.checkpoint();released=[]
  def die(argv,data=None,timeout=90):released.append(argv);raise KeyboardInterrupt('crash after remote release')
  c.call=die;self.assertRaises(KeyboardInterrupt,c.finish,x);disk=json.loads(c.statepath.read_bytes());self.assertEqual(disk['pending']['stage'],'release_intent')
  def again(argv,data=None,timeout=90):self.assertEqual(argv[-2],'release');released.append(argv);return json.dumps({'status':'released','request_sha256':argv[-1],'removed':0}).encode()
  resumed=m.Cycle(c.c,again);self.assertEqual(resumed.run()['status'],'recovered_release');self.assertEqual(len(released),2);self.assertNotIn('pending',resumed.state)
 def test_unbound_release_keeps_intent_and_local_proof_for_restart(self):
  for response in [{'status':'pending'}, {'status':'released','request_sha256':'wrong'}, {}]:
   with self.subTest(response=response):
    c,p,calls=self.make();self.pending(c,p);x=c.state['pending'];result=b'{"results":[]}';(p/'result.json').write_bytes(result);native=p/'native.json';m.save(native,{'request_sha256':x['sha256'],'response_sha256':m.sha(result),'imported':1,'unchanged':0,'committed_at':'t'})
    x.update(result='result.json',result_sha256=m.sha(result),native_receipt=str(native),native_receipt_sha256=m.sha(native.read_bytes()),matched=1);c.checkpoint()
    c.call=lambda argv,data=None,timeout=90:json.dumps(response).encode()
    with self.assertRaisesRegex(RuntimeError,'release response binding invalid'):c.finish(x)
    disk=json.loads(c.statepath.read_bytes());self.assertEqual(disk['pending']['stage'],'release_intent');self.assertTrue((p/'request.json').exists());self.assertTrue((p/'result.json').exists());self.assertFalse((p/(x['sha256']+'-receipt.json')).exists())
    def released(argv,data=None,timeout=90):
     self.assertEqual(argv[-2:],[ 'release',x['sha256']]);calls.append(argv);return json.dumps({'status':'released','request_sha256':x['sha256'],'removed':0}).encode()
    resumed=m.Cycle(c.c,released);self.assertEqual(resumed.run()['status'],'recovered_release');self.assertNotIn('pending',resumed.state);self.assertEqual(len(calls),1)
 def test_new_exclusion_drops_old_bytes_and_exports_clean(self):
  c,p,calls=self.make();self.pending(c,p);old=c.state['pending']['sha256'];policy={'format':'gitcrawl.embedding.exclusions.v1','dataset':'gitcrawl/history/threads','excluded_thread_ids':['1']};m.save(c.policy,policy);c.policy_refresh=lambda:'new-policy';exports=[]
  def new():
   self.assertFalse((p/'request.json').exists());self.assertEqual(c.state['retained'][0]['sha256'],old);exports.append(True);return False
  c.new_packet=new;c.source_ready=lambda:False;c.call=lambda argv,data=None,timeout=90:b'{"status":"released"}'
  self.assertEqual(c.run()['status'],'waiting_core');self.assertEqual(exports,[True]);self.assertNotIn('pending',c.state)
 def test_same_set_new_binding_replaces_without_relabelling(self):
  c,p,calls=self.make();self.pending(c,p);raw=(p/'request.json').read_bytes();m.save(c.policy,{'excluded_thread_ids':[]});c.policy_refresh=lambda:'new-binding'
  def new():self.assertEqual((p/'request.json').read_bytes(),raw);self.assertEqual(c.state['retained'][0]['invalidated'],'consumer_policy_changed');return False
  c.new_packet=new;c.source_ready=lambda:False;c.call=lambda argv,data=None,timeout=90:b'{"status":"released"}';self.assertEqual(c.run()['status'],'waiting_core')
 def test_transient_broker_error_keeps_pending_untouched(self):
  c,p,calls=self.make();self.pending(c,p);before=c.state['pending']['sha256'];c.broker=lambda mode,raw:(_ for _ in ()).throw(RuntimeError('temporary connection reset'))
  self.assertRaises(RuntimeError,c.run);self.assertEqual(c.state['pending']['sha256'],before);self.assertTrue((p/'request.json').exists());self.assertEqual(c.state['retained'],[])


class CapTests(Tests):
 def test_halved_cap_uses_vector_catchup(self):
  c,p,calls=self.make();binary=p/'exporter';binary.write_bytes(b'native');c.c['binary']=str(binary);c.c['binary_sha256']=m.sha(b'native');c.policy_refresh=lambda:'policy';attempts=[]
  def call(argv,data=None,timeout=90):
   calls.append(argv)
   if 'embedding-export' in argv:
    cap=int(argv[argv.index('--limit')+1]);attempts.append(cap)
    if cap==2000:raise RuntimeError('request exceeds16MiB; retry a smaller explicit task cap')
    raw=json.dumps({'scope':{'max_tasks':cap},'source_binding':{'consumer_exclusions_sha256':'policy'},'tasks':[{'task_id':str(i),'input':'x','thread_binding':{'thread_id':str(i+1)}} for i in range(cap)]}).encode();pathlib.Path(argv[argv.index('--output')+1]).write_bytes(raw);return json.dumps({'tasks':cap,'request_sha256':m.sha(raw)}).encode()
   self.assertEqual(argv[0],'fixture-import');self.assertEqual(argv[argv.index('--mode')+1],'embedding-import');return b'{"phase":"committed","receipt":"native"}'
  c.call=call;self.assertTrue(c.new_packet());self.assertEqual(attempts,[2000,1000]);self.assertEqual(c.state['pending']['export']['task_cap'],1000)
  c.broker=lambda mode,raw:{'status':'complete','results':[]};c.source_ready=lambda:True;verified=iter([False,True]);c.verify_import=lambda pending,result:(pending.update({'matched':1000}) or next(verified));c.finish=lambda p:None
  self.assertEqual(c.run()['status'],'catchup_imported')
if __name__=='__main__':unittest.main()

class AvailabilityTests(Tests):
 def test_changed_availability_replaces_before_broker(self):
  c,p,calls=self.make();self.pending(c,p);digest=c.state['pending']['sha256']
  c.validate_pending=lambda pending:(_ for _ in ()).throw(RuntimeError('native availability changed'))
  c.broker=lambda *args:self.fail('provider admission with stale availability')
  c.new_packet=lambda:False;c.source_ready=lambda:False
  self.assertEqual(c.run()['status'],'waiting_core');self.assertNotIn('pending',c.state);self.assertEqual(c.state['retained'][0]['sha256'],digest)
 def test_unqualified_scope_retains_pending_without_broker(self):
  c,p,calls=self.make();self.pending(c,p)
  c.validate_pending=lambda pending:(_ for _ in ()).throw(RuntimeError('cluster availability blocked: auth'))
  c.broker=lambda *args:self.fail('provider admission with unqualified failure')
  self.assertEqual(c.run()['status'],'waiting_scope');self.assertIn('pending',c.state);self.assertEqual(c.state['retained'],[])

class IdentityQuarantineTests(Tests):
 def test_retained_only_noop_is_not_publication_or_full_retirement(self):
  c,p,calls=self.make();c.state['retained']=[{'sha256':'old','request':'old.json'}];(p/'old.json').write_text('{}');c.new_packet=lambda:False;c.source_ready=lambda:True
  def call(argv,data=None,timeout=90):
   calls.append(argv);self.assertEqual(argv,['fixture-cluster']);return b'{"phase":"unchanged","cluster_scope":{"mode":"available-subset","publication_status":"retained_only"}}'
  c.call=call;result=c.run();self.assertEqual(result['status'],'identity_quarantined');self.assertTrue((p/'old.json').exists());self.assertEqual(len(c.state['retained']),1)
 def test_combined_retained_only_reports_identity_wait(self):
  c,p,calls=self.make();self.pending(c,p);c.source_ready=lambda:True;c.broker=lambda mode,raw:{'status':'complete','results':[]};verified=iter([False,True]);c.verify_import=lambda pending,result:(pending.update({'matched':1}) or next(verified));c.finish=lambda p:None
  c.call=lambda argv,data=None,timeout=90:b'{"phase":"committed","receipt":"native","cluster_scope":{"mode":"available-subset","publication_status":"retained_only"}}'
  self.assertEqual(c.run()['status'],'identity_quarantined')
 def test_partial_core_receipt_is_ready_without_complete_fiction(self):
  import sqlite3,datetime,time
  c,p,calls=self.make();db=p/'source.db';c.c['db']=str(db)
  with sqlite3.connect(db) as con:
   con.execute('create table analytics_coverage(repository,through,observed_at,complete)');now=datetime.datetime.now(datetime.timezone.utc);con.execute('insert into analytics_coverage values(?,?,?,0)',('openclaw/openclaw',(now-datetime.timedelta(seconds=2)).isoformat(),(now-datetime.timedelta(seconds=1)).isoformat()))
  self.assertTrue(c.source_ready());self.assertEqual(c.state['core']['complete'],0)
