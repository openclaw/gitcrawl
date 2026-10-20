from contextlib import closing
import copy
import datetime
import json
import os
from pathlib import Path
import sqlite3
import unittest
from unittest.mock import patch

import archive_handoff
import maintain
import retention
import test_retention


class ArchiveTests(unittest.TestCase):
    def fixture(self, count=1):
        c, original, done, completed, calls = test_retention.RetentionTests.fixture(self)
        with closing(sqlite3.connect(c.c['db'])) as db, db:
            db.execute('CREATE TABLE thread_exclusions(repository TEXT,number INTEGER,kind TEXT,original_thread_id INTEGER)')
        c.policy.write_text('{"excluded_thread_ids":[]}')
        source = json.loads((c.base / original['request']).read_bytes())
        entries = []
        for i in range(count):
            request = copy.deepcopy(source); request['request_id'] = str(i)
            raw = retention.encoded(request); digest = retention.digest(raw)
            task = request['tasks'][0]
            result = {'format': 'clawmmunity.native-embeddings.v1', 'request_sha256': digest,
                      'model': request['model'], 'dimensions': 1536, 'tasks': 1, 'ready': 1,
                      'pending': 0, 'failed': 0, 'status': 'complete',
                      'results': [{'task_id': task['task_id'], 'input_sha256': task['input_sha256'], 'vector': [1.0] + [0.0]*1535}]}
            request_name, result_name = 'request-'+str(i)+'.json', 'result-'+str(i)+'.json'
            (c.base / request_name).write_bytes(raw); value = retention.encoded(result); (c.base / result_name).write_bytes(value)
            entries.append({'sha256': digest, 'request': request_name, 'result': result_name,
                            'result_sha256': retention.digest(value), 'created_at': '2026-01-01T00:00:00Z',
                            'error': 'native prepare failed: stale native identity/clock task fixture'})
        source['request_id'] = 'successor'; raw = retention.encoded(source)
        (c.base / 'successor.json').write_bytes(raw)
        c.state = {'retained': entries, 'pending': {'request': 'successor.json', 'sha256': retention.digest(raw),
                   'submit': {'status': 'submitted', 'request_sha256': retention.digest(raw)}, 'inspect': {}, 'export': {'tasks': 1}}}
        c.checkpoint()
        return c, entries, calls

    def test_exact_private_archive_precedes_release_and_preserves_pending(self):
        c, entries, calls = self.fixture()
        entry = entries[0]; pending = copy.deepcopy(c.state['pending'])
        request = (c.base / entry['request']).read_bytes(); result = (c.base / entry['result']).read_bytes()
        original = c.call
        def release(argv, data=None, timeout=90):
            directory = c.base / 'completed-broker-archive' / entry['sha256']
            self.assertEqual((directory / 'request.json').read_bytes(), request)
            self.assertEqual((directory / 'result.json').read_bytes(), result)
            self.assertEqual(directory.stat().st_mode & 0o777, 0o700)
            self.assertEqual((directory / 'request.json').stat().st_mode & 0o777, 0o600)
            disk = json.loads(c.statepath.read_bytes())
            self.assertEqual(disk['retained'][0]['archive_handoff']['stage'], 'release_intent')
            return original(argv, data, timeout)
        c.call = release
        self.assertEqual(archive_handoff.archive_superseded(c)['archived'], [entry['sha256']])
        self.assertEqual(c.state['pending'], pending)
        self.assertEqual(c.state['retained'], [])
        self.assertTrue((c.base / 'completed-broker-archive' / entry['sha256'] / 'retirement-receipt.json').exists())

    def test_unsubmitted_successor_keeps_reusable_jobs(self):
        c, entries, calls = self.fixture()
        c.state['pending'].pop('submit')
        self.assertEqual(archive_handoff.archive_superseded(c)['archived'], [])
        self.assertEqual(calls, [])
        self.assertTrue((c.base / entries[0]['request']).exists())

    def test_incomplete_or_excluded_work_is_not_archived(self):
        c, entries, calls = self.fixture(2)
        p = c.base / entries[0]['result']; value = json.loads(p.read_bytes()); value.update(status='pending', ready=0, pending=1)
        raw = retention.encoded(value); p.write_bytes(raw); entries[0]['result_sha256'] = retention.digest(raw)
        c.policy.write_text('{"excluded_thread_ids":["1"]}')
        self.assertEqual(archive_handoff.archive_superseded(c)['archived'], [])
        self.assertEqual(calls, [])
        self.assertEqual(len(c.state['retained']), 2)

    def test_native_owner_exclusion_prevents_new_archive_copy(self):
        c, entries, calls = self.fixture()
        with closing(sqlite3.connect(c.c['db'])) as db, db:
            db.execute("INSERT INTO thread_exclusions VALUES('openclaw/openclaw',1,'issue',1)")
        self.assertEqual(archive_handoff.archive_superseded(c)['archived'], [])
        self.assertEqual(calls, [])

    def test_active_lease_and_remote_release_crash_resume(self):
        c, entries, calls = self.fixture(); entry = entries[0]; original = c.call
        c.call = lambda *a, **k: (_ for _ in ()).throw(RuntimeError('native_request_inflight'))
        self.assertEqual(archive_handoff.archive_superseded(c)['active_lease_wait'], [entry['sha256']])
        c.call = lambda *a, **k: (_ for _ in ()).throw(KeyboardInterrupt('after remote release'))
        with self.assertRaises(KeyboardInterrupt): archive_handoff.resume(c)
        resumed = maintain.Cycle(c.c, original)
        self.assertEqual(archive_handoff.resume(resumed)['archived'], [entry['sha256']])

    def test_archive_failure_keeps_queue_and_all_working_copies(self):
        c, entries, calls = self.fixture()
        with patch.object(retention, 'immutable_bytes', side_effect=OSError('fsync failed')):
            with self.assertRaises(OSError): archive_handoff.archive_superseded(c)
        self.assertEqual(len(c.state['retained']), 1)
        self.assertEqual(calls, [])
        self.assertTrue((c.base / entries[0]['request']).exists())

    def test_partial_local_cleanup_keeps_archive_and_replays_without_release(self):
        c, entries, calls = self.fixture(); entry = entries[0]; original = Path.unlink
        def fail(p, *a, **k):
            if p == c.base / entry['result']: raise OSError('cleanup interrupted')
            return original(p, *a, **k)
        with patch.object(Path, 'unlink', fail):
            with self.assertRaises(OSError): archive_handoff.archive_superseded(c)
        resumed = maintain.Cycle(c.c, lambda *a, **k: self.fail('duplicate broker call'))
        self.assertEqual(archive_handoff.resume(resumed)['archived'], [entry['sha256']])
        self.assertTrue((c.base / 'completed-broker-archive' / entry['sha256'] / 'result.json').exists())

    def test_live_cap_stale_replacement_progress_without_growth_or_reembedding(self):
        c, entries, calls = self.fixture(16)
        (Path(c.c['handoff_state']) / 'last-failure.json').write_text('{"phase":"prepare"}')
        c.c['combined_invocation'] = ['fixture-native']
        clock = {'value': '-11'}; publications = []; events = []
        c.source_ready = lambda: True; c.retirement_scope = lambda: None; c.policy_refresh = lambda: 'policy'
        c.verify_import = lambda p, result: False
        release = c.call
        refs = {e['sha256'] for e in entries} | {c.state['pending']['sha256']}
        cache = {'ready': True, 'model_calls': 0}
        def broker(mode, raw):
            request = json.loads(raw); digest = retention.digest(raw)
            if mode == 'inspect': return {'tasks': 1, 'tokens': 1}
            if mode == 'submit':
                events.append('successor-submitted')
                if not cache['ready']:cache['model_calls'] += 1;cache['ready'] = True
                refs.add(digest)
                return {'status': 'submitted', 'request_sha256': digest}
            task = request['tasks'][0]
            return {'format': 'clawmmunity.native-embeddings.v1', 'request_sha256': digest,
                    'model': request['model'], 'dimensions': 1536, 'tasks': 1, 'ready': 1, 'pending': 0,
                    'failed': 0, 'status': 'complete', 'results': [{'task_id': task['task_id'],
                    'input_sha256': task['input_sha256'], 'vector': [1.0]+[0.0]*1535}]}
        c.broker = broker
        def call(argv, data=None, timeout=90):
            if argv[0] == 'fixture-broker':
                events.append('release')
                refs.discard(argv[-1])
                if not refs:cache['ready'] = False
                return release(argv, data, timeout)
            request = json.loads(Path(argv[argv.index('--request')+1]).read_bytes())
            if request['tasks'][0]['clock_binding']['observation_sequence'] != clock['value']:
                raise RuntimeError('native prepare failed: stale native identity/clock task fixture')
            publications.append(True)
            return b'{"phase":"committed","receipt":"fixture"}'
        c.call = call
        def fresh():
            self.assertLessEqual(len(c.state['retained']), 16)
            request = json.loads((c.base / 'successor.json').read_bytes())
            request['request_id'] = 'after-real-core-completion'
            request['tasks'][0]['clock_binding']['observation_sequence'] = clock['value']
            raw = retention.encoded(request); (c.base/'fresh.json').write_bytes(raw)
            c.state['pending'] = {'request': 'fresh.json', 'sha256': retention.digest(raw), 'export': {'tasks': 1}}
            c.checkpoint(); return True
        c.new_packet = fresh
        def verify(p, result):
            if publications: p['matched'] = 1; return True
            return False
        c.verify_import = verify
        c.finish = lambda p: (c.state.pop('pending'), c.checkpoint())
        if os.environ.get('GITCRAWL_TEST_DISABLE_ARCHIVE') == '1': c.archive_superseded = lambda: {'archived': []}
        result = c.run()
        self.assertEqual(result['status'], 'combined_published')
        self.assertEqual(publications, [True])
        self.assertEqual(c.state['retained'], [])
        self.assertNotIn('pending', c.state)
        self.assertIn('successor-submitted', events)
        self.assertEqual(cache['model_calls'], 0)
        self.assertTrue(all((c.base/'completed-broker-archive'/e['sha256']/'request.json').exists() for e in entries))

    def test_core_boundary_waits_for_an_actual_new_receipt(self):
        c, entries, calls = self.fixture(); count = []
        c.controller_idle = lambda: True
        def ready():
            count.append(1)
            at = 1000 if len(count) == 1 else 1010
            c.state['core'] = {'observed_at': datetime.datetime.fromtimestamp(at, datetime.timezone.utc).isoformat(), 'through': str(at), 'complete': 1}
            return True
        c.source_ready = ready
        with patch.object(maintain.time, 'time', return_value=1011), patch.object(maintain.time, 'sleep') as sleep:
            result = c.await_core_boundary()
        self.assertEqual(result['through'], '1010'); self.assertEqual(len(count), 2); sleep.assert_called_once_with(1)
        self.assertEqual(calls, [])

    def test_core_boundary_budget_defers_without_any_provider_or_settlement(self):
        c, entries, calls = self.fixture(); c.source_ready = lambda: False
        c.controller_idle = lambda: True
        with patch.object(maintain.time, 'monotonic', side_effect=[0, 121]):
            with self.assertRaises(maintain.CoreBoundaryPending): c.await_core_boundary()
        self.assertEqual(calls, [])

    def test_archive_pre_intent_checkpoint_crash_rebuilds_from_preserved_evidence(self):
        c, entries, calls = self.fixture(); entry = entries[0]; original = c.checkpoint
        def crash():
            if any(e.get('archive_handoff') for e in c.state['retained']):
                raise KeyboardInterrupt('archive durable before queue checkpoint')
            original()
        c.checkpoint = crash
        with self.assertRaises(KeyboardInterrupt):archive_handoff.archive_superseded(c)
        disk = json.loads(c.statepath.read_bytes())
        self.assertNotIn('archive_handoff', disk['retained'][0])
        self.assertEqual(calls, [])
        resumed = maintain.Cycle(c.c, c.call)
        self.assertEqual(archive_handoff.archive_superseded(resumed)['archived'], [entry['sha256']])

    def test_changed_archive_bytes_block_broker_release(self):
        c, entries, calls = self.fixture(); entry = entries[0]
        c.call = lambda *a, **k: (_ for _ in ()).throw(RuntimeError('native_request_inflight'))
        archive_handoff.archive_superseded(c)
        path = c.base / 'completed-broker-archive' / entry['sha256'] / 'result.json'
        path.write_bytes(b'changed')
        with self.assertRaisesRegex(RuntimeError, 'archive file changed'):archive_handoff.resume(c)
        self.assertEqual(calls, [])
        self.assertTrue((c.base / entry['result']).exists())


if __name__ == '__main__': unittest.main()
