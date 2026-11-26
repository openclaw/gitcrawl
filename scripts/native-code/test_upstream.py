import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import time
import unittest
from unittest.mock import patch
import uuid

import upstream


class UpstreamTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name).resolve()
        self.remote = self.base/'remote'
        self.remote.mkdir()
        self.git(self.remote, 'init', '-b', 'main')
        (self.remote/'old.txt').write_text('old\n')
        self.commit('first')
        self.state = self.base/'state'
        self.state.mkdir()
        self.calls = []

    def git(self, repo, *args):
        return subprocess.check_output(['/usr/bin/git', '-C', str(repo), *args], stderr=subprocess.DEVNULL).decode().strip()

    def commit(self, title):
        self.git(self.remote, 'add', '.')
        self.git(self.remote, '-c', 'user.name=Fixture', '-c', 'user.email=fixture@example.invalid', 'commit', '-m', title)

    def local_git(self, repo, args, timeout=120):
        self.calls.append(list(args))
        if args[0] == 'fetch':
            args = list(args)
            args[args.index('origin')] = self.remote.as_uri()
        return upstream.git_command(repo, args, timeout)

    def cache(self, call=None):
        return upstream.SourceCache(self.state, self.state/'upstream/source.git', call or self.local_git)

    def test_tracks_remote_main_without_checkout_and_keeps_previous_receipt(self):
        c = self.cache()
        first, path = c.capture(uuid.uuid4().hex)
        before = path.read_bytes()
        self.assertEqual(first['commit'], self.git(self.remote, 'rev-parse', 'HEAD'))
        self.assertEqual(first['repository_url'], upstream.REMOTE)
        self.assertFalse((c.repo/'old.txt').exists())
        (self.remote/'old.txt').unlink()
        (self.remote/'new.txt').write_text('new\n')
        self.commit('replacement')
        second, _ = c.capture(uuid.uuid4().hex)
        self.assertNotEqual(first['commit'], second['commit'])
        self.assertEqual(second['commit'], self.git(self.remote, 'rev-parse', 'HEAD'))
        self.assertEqual(path.read_bytes(), before)
        self.assertEqual(self.git(c.repo, 'show', first['commit']+':old.txt'), 'old')
        self.assertEqual(self.git(c.repo, 'show', second['commit']+':new.txt'), 'new')
        self.assertTrue(all('checkout' not in x and 'reset' not in x for x in self.calls))

    def test_fetch_failure_is_not_reported_as_current_cached_success(self):
        c = self.cache();first, path = c.capture(uuid.uuid4().hex)
        def fail(repo, args, timeout=120):
            if args[0] == 'fetch':
                raise RuntimeError('simulated transport failure')
            return self.local_git(repo, args, timeout)
        with self.assertRaisesRegex(RuntimeError, 'transport failure'):
            self.cache(fail).capture(uuid.uuid4().hex)
        self.assertEqual(json.loads(path.read_bytes()), first)
        self.assertEqual(len(list(c.root.glob('*-upstream.json'))), 1)

    def test_initialization_interruption_resumes_owned_staging(self):
        def interrupt(repo, args, timeout=120):
            if args[:2] == ['config', 'remote.origin.fetch']:
                raise RuntimeError('interrupted initialization')
            return self.local_git(repo, args, timeout)
        with self.assertRaisesRegex(RuntimeError, 'interrupted initialization'):
            self.cache(interrupt).capture(uuid.uuid4().hex)
        self.assertFalse((self.state/'upstream/source.git').exists())
        c = self.cache();receipt, _ = c.capture(uuid.uuid4().hex)
        self.assertEqual(receipt['commit'], self.git(self.remote, 'rev-parse', 'HEAD'))

    def test_refuses_external_tree_and_changed_remote(self):
        with self.assertRaisesRegex(RuntimeError, 'dedicated'):
            upstream.SourceCache(self.state, self.remote)
        c = self.cache();c.capture(uuid.uuid4().hex)
        self.git(c.repo, 'config', 'remote.origin.url', 'https://example.invalid/wrong.git')
        with self.assertRaisesRegex(RuntimeError, 'configuration'):
            c.capture(uuid.uuid4().hex)

    def test_refuses_symlink_and_unowned_cache(self):
        root = self.state/'upstream'
        root.symlink_to(self.remote, target_is_directory=True)
        with self.assertRaisesRegex(RuntimeError, 'dedicated|symlink'):
            self.cache().capture(uuid.uuid4().hex)
        root.unlink();root.mkdir(mode=0o700)
        with self.assertRaisesRegex(RuntimeError, 'ownership'):
            self.cache().capture(uuid.uuid4().hex)

    def test_refuses_external_common_directory_before_git_mutation(self):
        c = self.cache(); c.capture(uuid.uuid4().hex)
        external = self.base/'external.git'
        self.git(self.base, 'clone', '--bare', str(c.repo), str(external))
        (external/'config').write_bytes((c.repo/'config').read_bytes())
        (c.repo/'commondir').write_text(str(external)+'\n')
        self.assertEqual(Path(self.git(c.repo, 'rev-parse', '--git-common-dir')), external)
        self.calls.clear()
        with patch.object(upstream, 'CACHE_COMPACT_BYTES', 1):
            with self.assertRaisesRegex(RuntimeError, 'common directory redirection'):
                c.capture(uuid.uuid4().hex)
        self.assertFalse(self.calls)

    def test_refuses_staging_symlinks_before_git_mutation(self):
        def stop_at_init(_repo, _args, timeout=120):
            raise RuntimeError('interrupted before git init')
        with self.assertRaisesRegex(RuntimeError, 'interrupted before git init'):
            self.cache(stop_at_init).capture(uuid.uuid4().hex)
        c = self.cache()
        stage = c.root/'initializing.git'
        external = self.base/'external.git'
        self.git(self.base, 'init', '--bare', str(external))
        external_config = (external/'config').read_bytes()
        external_objects = sorted((external/'objects').iterdir())
        for name in ('config', 'objects'):
            with self.subTest(name=name):
                (stage/name).symlink_to(external/name)
                with self.assertRaisesRegex(RuntimeError, 'unsafe metadata'):
                    c.capture(uuid.uuid4().hex)
                self.assertFalse(self.calls)
                self.assertEqual((external/'config').read_bytes(), external_config)
                self.assertEqual(sorted((external/'objects').iterdir()), external_objects)
                (stage/name).unlink()

    def test_git_timeout_and_interrupt_stop_descendant(self):
        # The grandchild closes inherited pipes, so killing and waiting on
        # only the top-level Git process would return while it was still alive.
        worker = '''from pathlib import Path
import signal, sys, time
ready, late, clean = map(Path, sys.argv[1:4])
def stop(*_):
    clean.touch()
    raise SystemExit(0)
signal.signal(signal.SIGTERM, stop if sys.argv[4] != 'timeout' else signal.SIG_IGN)
ready.touch()
time.sleep(4)
late.touch()
'''
        leader = 'import subprocess,sys; p=subprocess.Popen(sys.argv[1:],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); p.wait()'
        popen = subprocess.Popen
        killpg = os.killpg
        def interrupt(_sig, _frame):
            raise KeyboardInterrupt('synthetic controller stop')
        for phase in ('timeout', 'startup', 'communicate', 'cleanup'):
            ready, late = self.base/f'{phase}-ready', self.base/f'{phase}-late'
            clean = self.base/f'{phase}-clean'
            def send_stop():
                until = time.monotonic()+1.5
                while not ready.exists() and time.monotonic()<until:
                    time.sleep(.01)
                os.kill(os.getpid(), signal.SIGTERM)
            def replace_command(_argv, **kwargs):
                child = popen([sys.executable, '-c', leader, sys.executable, '-c', worker,
                               str(ready), str(late), str(clean), phase], **kwargs)
                if phase == 'startup':
                    send_stop()
                elif phase in ('communicate', 'cleanup'):
                    original = child.communicate
                    def stop_then_communicate(*args, **kwargs):
                        child.communicate = original
                        send_stop()
                        return original(*args, **kwargs)
                    child.communicate = stop_then_communicate
                return child
            def stop_during_cleanup(group, signum):
                if phase == 'cleanup' and signum == signal.SIGTERM:
                    os.kill(os.getpid(), signal.SIGINT)
                return killpg(group, signum)
            previous = signal.signal(signal.SIGTERM, interrupt)
            try:
                with self.subTest(phase=phase), \
                     patch.object(upstream.subprocess, 'Popen', side_effect=replace_command), \
                     patch.object(upstream.os, 'killpg', side_effect=stop_during_cleanup):
                    expected = subprocess.TimeoutExpired if phase == 'timeout' else KeyboardInterrupt
                    with self.assertRaises(expected):
                        upstream.git_command(self.base, ['gc'], timeout=1)
                    self.assertTrue(ready.exists(), 'synthetic descendant did not start')
                    self.assertEqual(clean.exists(), phase != 'timeout')
                    self.assertIs(signal.getsignal(signal.SIGTERM), interrupt)
            finally:
                signal.signal(signal.SIGTERM, previous)
        time.sleep(4)
        self.assertFalse(list(self.base.glob('*-late')), 'synthetic descendant survived')

    def test_gc_diagnostic_fails_closed_even_with_zero_exit(self):
        popen = subprocess.Popen
        def replace_command(_argv, **kwargs):
            return popen([sys.executable, '-c', 'import sys; print("fixture", file=sys.stderr)'], **kwargs)
        with patch.object(upstream.subprocess, 'Popen', side_effect=replace_command):
            with self.assertRaisesRegex(RuntimeError, 'gc reported a diagnostic'):
                upstream.git_command(self.base, ['gc'])

    def test_compacts_owned_cache_over_threshold_and_preserves_receipts(self):
        c = self.cache()
        first_receipts = []
        for i in range(4):
            (self.remote/'payload.bin').write_bytes(os.urandom(32768))
            self.commit(f'blob-{i}')
            _, receipt_path = c.capture(uuid.uuid4().hex)
            first_receipts.append((receipt_path, receipt_path.read_bytes()))
        before_size = c._allocated()
        self.assertGreater(before_size, 100000)
        # Replace with a tiny commit so only the latest tip is reachable.
        (self.remote/'payload.bin').write_bytes(b'small\n')
        self.commit('shrink')
        # Set compact threshold below before_size and budget below before_size
        # (but above the compacted size) to verify recovery from an over-budget cache.
        with patch.object(upstream, 'CACHE_COMPACT_BYTES', 50000), \
             patch.object(upstream, 'CACHE_BUDGET_BYTES', before_size - 1):
            receipt, new_path = c.capture(uuid.uuid4().hex)
        self.assertEqual(receipt['commit'], self.git(self.remote, 'rev-parse', 'HEAD'))
        self.assertLess(c._allocated(), before_size)
        self.assertTrue(any(call[:1] == ['gc'] for call in self.calls))
        # All prior receipts remain untouched.
        for p, prior in first_receipts:
            self.assertEqual(p.read_bytes(), prior)
        self.assertTrue(new_path.exists())
        self.assertEqual(self.git(c.repo, 'show', receipt['commit']+':payload.bin'), 'small')

    def test_fails_closed_when_budget_still_exceeded_after_compaction(self):
        c = self.cache()
        (self.remote/'payload.bin').write_bytes(os.urandom(65536))
        self.commit('large')
        c.capture(uuid.uuid4().hex)
        with patch.object(upstream, 'CACHE_COMPACT_BYTES', 1024), \
             patch.object(upstream, 'CACHE_BUDGET_BYTES', 1024):
            with self.assertRaisesRegex(RuntimeError, 'disk budget unavailable'):
                c.capture(uuid.uuid4().hex)
        self.assertTrue(any(call[:1] == ['gc'] for call in self.calls))

    def test_refuses_compaction_when_free_disk_below_minimum(self):
        c = self.cache()
        c.capture(uuid.uuid4().hex)
        self.calls.clear()
        low = upstream.shutil.disk_usage(self.state)._replace(free=upstream.MIN_FREE_BYTES - 1)
        with patch.object(upstream.shutil, 'disk_usage', return_value=low), \
             patch.object(upstream, 'CACHE_COMPACT_BYTES', 1):
            with self.assertRaisesRegex(RuntimeError, 'disk budget unavailable'):
                c.capture(uuid.uuid4().hex)
        self.assertFalse(any(call[:1] == ['gc'] for call in self.calls))


if __name__ == '__main__':
    unittest.main()
