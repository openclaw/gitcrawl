import json
import os
from pathlib import Path
import subprocess
import tempfile
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

    def test_compacts_owned_cache_over_threshold_and_preserves_receipts(self):
        c = self.cache()
        first_receipts = []
        for i in range(4):
            (self.remote/'payload.bin').write_bytes(os.urandom(32768))
            self.commit(f'blob-{i}')
            _, receipt_path = c.capture(uuid.uuid4().hex)
            first_receipts.append(receipt_path)
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
        for p in first_receipts + [new_path]:
            self.assertTrue(p.exists())
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
