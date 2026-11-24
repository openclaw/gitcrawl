import io
import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

import run_code_update as m


class CommitGateTests(unittest.TestCase):
    def setUp(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.gate = Path(tmp.name) / 'cycle.gate'
        self.digest = 'ab' * 32

    def test_first_visible_read_is_the_full_synced_digest(self):
        fsync, link = m.os.fsync, m.os.link
        events = []
        def sync(fd):
            self.assertFalse(self.gate.exists())
            fsync(fd)
            events.append('synced')
        def publish(tmp, final):
            self.assertEqual(events, ['synced'])
            self.assertFalse(self.gate.exists())
            link(tmp, final)
            self.assertEqual(self.gate.read_bytes(), self.digest.encode('ascii'))
            events.append('visible')
        with patch.object(m.os, 'fsync', side_effect=sync), patch.object(m.os, 'link', side_effect=publish):
            with m.commit_gate(self.gate, self.digest):
                self.assertEqual(events, ['synced', 'visible'])
                self.assertEqual(self.gate.stat().st_mode & 0o777, 0o600)
        self.assertEqual(list(self.gate.parent.iterdir()), [])

    def test_sync_failure_or_collision_never_publishes_or_removes_another_gate(self):
        with patch.object(m.os, 'fsync', side_effect=OSError('failed sync')):
            with self.assertRaisesRegex(OSError, 'failed sync'), m.commit_gate(self.gate, self.digest):
                self.fail('published after sync failure')
        self.assertEqual(list(self.gate.parent.iterdir()), [])
        self.gate.write_bytes(b'original')
        with self.assertRaises(FileExistsError), m.commit_gate(self.gate, self.digest):
            self.fail('overwrote existing gate')
        self.assertEqual(self.gate.read_bytes(), b'original')
        self.assertEqual(list(self.gate.parent.iterdir()), [self.gate])


class LifecycleTests(unittest.TestCase):
    def fixture(self, phase='unchanged', bad_binding=False):
        t = tempfile.TemporaryDirectory();self.addCleanup(t.cleanup)
        p = Path(t.name).resolve()
        (p/'job').write_text('job');(p/'config').write_text('config')
        (p/'db').touch();(p/'runner.lock').touch()
        source = {'format':'gitcrawl.code-upstream.v1','commit':'a'*40,'git_dir':str(p/'upstream/source.git')}
        proof = p/'source.json';m.atomic(proof, source)
        prepared = {'phase':phase,'git_sha':source['commit'],'tracked_dirty':False,'source_root':source['git_dir'],
                    'content_digest':'fixture','upstream':{'receipt_sha256':m.digest(proof)}}
        if bad_binding:prepared['git_sha'] = 'b'*40
        executable = p/'producer'
        executable.write_text('#!'+sys.executable+'\nimport json,sys,time,pathlib\nprint('+repr(json.dumps(prepared))+',flush=True)\n'+
            ("gate=pathlib.Path(sys.argv[sys.argv.index('--commit-gate')+1])\nwhile not gate.exists():time.sleep(.01)\nraise SystemExit(1)\n" if phase=='prepared' else ''))
        executable.chmod(0o700)
        calls=[];state={'loaded':True}
        def run(argv, timeout=20):
            calls.append(argv)
            if argv[1]=='print':return types.SimpleNamespace(returncode=0 if state['loaded'] else 1)
            if argv[1]=='bootout':state['loaded']=False;return types.SimpleNamespace(returncode=0)
            if argv[1]=='bootstrap':state['loaded']=True;return types.SimpleNamespace(returncode=0)
            self.fail('unexpected lifecycle call')
        a=types.SimpleNamespace(state_dir=str(p),label='fixture',binary=str(executable),source=source['git_dir'],db=str(p/'db'),write_seconds=1,
                                job_plist=str(p/'job'),config=str(p/'config'),plist_sha256=m.digest(p/'job'),config_sha256=m.digest(p/'config'))
        c=m.Lifecycle(a,run);c.verify=lambda:None
        return p,c,calls,state,source,proof

    def test_noop_keeps_owner_running_and_records_observed_upstream(self):
        p,c,calls,state,source,proof=self.fixture()
        with patch.object(m.SourceCache,'capture',return_value=(source,proof)):
            c.update()
        self.assertEqual(calls,[]);self.assertTrue(state['loaded'])
        self.assertEqual(json.loads((p/'last-code-result.json').read_bytes())['git_sha'],source['commit'])

    def test_fetch_failure_never_settles_owner_or_relabels_old_success(self):
        p,c,calls,state,source,proof=self.fixture();m.atomic(p/'last-code-result.json',{'old':True})
        with patch.object(m.SourceCache,'capture',side_effect=RuntimeError('upstream unavailable')):
            with self.assertRaisesRegex(RuntimeError,'upstream unavailable'):c.update()
        self.assertEqual(calls,[]);self.assertTrue(state['loaded']);self.assertFalse(c.journal.exists())
        self.assertEqual(json.loads((p/'last-code-result.json').read_bytes()),{'old':True})

    def test_prepared_wrong_sha_refused_before_settlement(self):
        popen = m.subprocess.Popen
        for phase in ('unchanged', 'prepared'):
            with self.subTest(phase=phase):
                p,c,calls,state,source,proof=self.fixture(phase=phase, bad_binding=True)
                children = []
                def spawn(*args, **kwargs):
                    child = popen(*args, **kwargs)
                    children.append(child)
                    self.addCleanup(child.stdout.close)
                    self.addCleanup(child.stderr.close)
                    if phase == 'unchanged':
                        # Exercise an exited producer as well as one waiting for its gate.
                        child.wait(timeout=5)
                    return child
                with patch.object(m.SourceCache,'capture',return_value=(source,proof)), \
                        patch.object(m.subprocess,'Popen',side_effect=spawn):
                    with self.assertRaisesRegex(RuntimeError,'binding mismatch'):c.update()
                self.assertEqual(calls,[]);self.assertTrue(state['loaded'])
                self.assertEqual(len(children), 1)
                self.assertIsNotNone(children[0].poll())
                self.assertTrue(children[0].stdout.closed)
                self.assertTrue(children[0].stderr.closed)

    def test_failed_atomic_write_restores_original_owner(self):
        p,c,calls,state,source,proof=self.fixture(phase='prepared')
        with patch.object(m.SourceCache,'capture',return_value=(source,proof)):
            with self.assertRaisesRegex(RuntimeError,'commit failed'):c.update()
        self.assertTrue(state['loaded']);self.assertFalse(c.journal.exists())
        self.assertEqual(sum(x[1]=='bootout' for x in calls),1)
        self.assertEqual(sum(x[1]=='bootstrap' for x in calls),1)

    def test_post_commit_unlink_failure_preserves_success_and_restores_owner(self):
        p,c,calls,state,source,proof=self.fixture(phase='prepared')
        producer = Path(c.a.binary)
        producer.write_text(producer.read_text().replace('raise SystemExit(1)',
            "assert gate.read_text() == 'fixture'\nprint(json.dumps({'phase':'committed','snapshot_id':42}))"))
        unlink = m.os.unlink
        def fail_gate_cleanup(path, *args, **kwargs):
            if str(path).endswith('.gate'):
                raise PermissionError('fixture cleanup denied')
            return unlink(path, *args, **kwargs)
        with patch.object(m.SourceCache,'capture',return_value=(source,proof)), \
                patch.object(m.os,'unlink',side_effect=fail_gate_cleanup), \
                patch.object(m.sys,'stderr',new_callable=io.StringIO) as warnings:
            c.update()
        self.assertIn('commit gate cleanup warning',warnings.getvalue())
        self.assertEqual(json.loads((p/'last-code-result.json').read_bytes())['snapshot_id'],42)
        self.assertFalse((p/'last-failure.json').exists())
        self.assertTrue(state['loaded']);self.assertFalse(c.journal.exists())

    def test_recovery_precedes_fetch_and_preserves_previously_unloaded_owner(self):
        p,c,calls,state,source,proof=self.fixture();state['loaded']=False
        m.atomic(c.journal,{'was_loaded':False})
        with patch.object(m.SourceCache,'capture',side_effect=AssertionError('fetch during recovery')):
            c.update()
        self.assertEqual(calls,[]);self.assertFalse(state['loaded']);self.assertFalse(c.journal.exists())


if __name__ == '__main__':
    unittest.main()
