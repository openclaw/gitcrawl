import datetime,fcntl,importlib.util,pathlib,tempfile,unittest
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('maintain',pathlib.Path(__file__).with_name('maintain.py'))
m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)

class CadenceTests(unittest.TestCase):
 def scenario(self,busy,fresh):
  temp=tempfile.TemporaryDirectory();self.addCleanup(temp.cleanup)
  root=pathlib.Path(temp.name);clock=[0.0];epoch=1800000000
  c=m.Cycle({'state_dir':str(root),'handoff_state':str(root)},lambda *args,**kwargs:self.fail('unexpected external work'))
  c.controller_idle=lambda:not busy(clock[0])
  def ready():
   if not fresh(clock[0]):return False
   c.state['core']={'complete':0,'observed_at':datetime.datetime.fromtimestamp(epoch+clock[0],datetime.timezone.utc).isoformat(),'through':datetime.datetime.fromtimestamp(epoch+clock[0]-20,datetime.timezone.utc).isoformat()};return True
  c.source_ready=ready
  for name,value in [('monotonic',lambda:clock[0]),('time',lambda:epoch+clock[0]),('sleep',lambda seconds:clock.__setitem__(0,clock[0]+seconds))]:
   active=patch.object(m.time,name,value);active.start();self.addCleanup(active.stop)
  return c,clock
 def test_measured_code_overlap_then_real_partial_core_receipt(self):
  # 20:27:23 code starts;20:28:29 restored;20:29:45 core completes.
  c,clock=self.scenario(lambda t:t<66,lambda t:t>=142)
  result=c.await_core_boundary()
  self.assertEqual(clock[0],142);self.assertEqual(result['complete'],0)
 def test_code_can_start_after_maintenance_wait(self):
  c,clock=self.scenario(lambda t:10<=t<66,lambda t:t>=142)
  self.assertEqual(c.await_core_boundary()['complete'],0);self.assertEqual(clock[0],142)
 def test_no_handoff_keeps_original120_second_budget(self):
  c,clock=self.scenario(lambda t:False,lambda t:False)
  with self.assertRaises(m.CoreBoundaryPending):c.await_core_boundary()
  self.assertEqual(clock[0],120)
 def test_stuck_controller_has_bounded_wait(self):
  c,clock=self.scenario(lambda t:True,lambda t:True)
  with self.assertRaises(m.CoreBoundaryPending):c.await_core_boundary()
  self.assertEqual(clock[0],120)
 def test_total_wait_never_exceeds240_seconds(self):
  c,clock=self.scenario(lambda t:100<=t<210,lambda t:False)
  with self.assertRaises(m.CoreBoundaryPending):c.await_core_boundary()
  self.assertEqual(clock[0],240)
 def test_second_handoff_cannot_reset_budget_again(self):
  c,clock=self.scenario(lambda t:t<10 or t>=20,lambda t:False)
  with self.assertRaises(m.CoreBoundaryPending):c.await_core_boundary()
  self.assertLessEqual(clock[0],120)
 def test_idle_probe_never_creates_lock_and_fences_journal(self):
  with tempfile.TemporaryDirectory() as temp:
   root=pathlib.Path(temp);c=m.Cycle({'state_dir':temp,'handoff_state':temp})
   with self.assertRaises(FileNotFoundError):c.controller_idle()
   self.assertFalse((root/'controller.lock').exists())
   (root/'controller.lock').write_bytes(b'')
   self.assertTrue(c.controller_idle())
   with (root/'controller.lock').open('rb') as lock:
    fcntl.flock(lock,fcntl.LOCK_EX|fcntl.LOCK_NB);self.assertFalse(c.controller_idle())
   (root/'handoff.json').write_text('{}');self.assertFalse(c.controller_idle())

if __name__=='__main__':unittest.main()
