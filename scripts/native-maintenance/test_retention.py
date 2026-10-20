from contextlib import closing
import copy
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

import maintain
import retention


class RetentionTests(unittest.TestCase):
    def fixture(self):
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        base = Path(tmp.name).resolve()
        handoff = base / 'handoff'
        handoff.mkdir()
        (handoff / 'controller.lock').touch()
        db = base / 'archive.db'
        with closing(sqlite3.connect(db)) as con, con:
            con.executescript('PRAGMA user_version=16; CREATE TABLE repositories(id INTEGER PRIMARY KEY,full_name TEXT,github_repo_id TEXT); CREATE TABLE cluster_runs(id INTEGER PRIMARY KEY,repo_id INTEGER,status TEXT,stats_json TEXT);')
            con.execute('CREATE TABLE threads(id INTEGER PRIMARY KEY,repo_id INTEGER,github_id TEXT,number INTEGER,kind TEXT,state TEXT,closed_at_local TEXT,observation_sequence INTEGER,evidence_observation_sequence INTEGER,evidence_source_updated_at TEXT,updated_at_gh TEXT,updated_at TEXT)')
            con.execute("INSERT INTO threads VALUES(1,1,'provider-1',1,'issue','open',NULL,-20,0,'','2026-01-02T00:00:00Z','2026-01-02T00:00:01Z')")
            con.execute('INSERT INTO repositories VALUES(1,?,?)', ('openclaw/openclaw', '42'))
            con.execute('INSERT INTO cluster_runs VALUES(7,1,?,?)', ('success', json.dumps({'native_producer': {'input_sha256': 'input', 'state_sha256': 'state', 'algorithm': 'algorithm'}})))
        stat = db.stat()
        header = {'format': 'gitcrawl.embedding.request.v1', 'repository': 'openclaw/openclaw', 'model': 'text-embedding-3-small', 'dimensions': 1536, 'basis': 'title_original', 'native_hash_version': 'embedding:v5', 'scope': {'max_tasks': 2000}, 'source_binding': {'source_commit': '6c9c1bea7d094397dcdfecfb50e693460592a5aa', 'archive_path': str(db), 'device': str(stat.st_dev), 'inode': str(stat.st_ino), 'schema_version': 16, 'repo_id': '1', 'provider_repo_id': '42', 'consumer_exclusions_sha256': 'policy', 'owner_exclusions_sha256': 'owner-policy'}}
        task = {'task_id': 'old-task', 'input': 'native', 'input_sha256': retention.digest(b'native'), 'native_content_hash': 'native-hash', 'thread_binding': {'thread_id': '1', 'github_id': 'provider-1', 'number': 1, 'kind': 'issue'}, 'clock_binding': {'observation_sequence': '-10', 'evidence_observation_sequence': '0', 'evidence_source_updated_at': '', 'updated_at_gh': '2026-01-01T00:00:00Z', 'updated_at': '2026-01-01T00:00:01Z'}, 'input_bytes': 6, 'input_runes': 6, 'text_truncated': False, 'original_text_runes': 6}
        raw = retention.encoded({**header, 'tasks': [task]})
        (base / 'old-request.json').write_bytes(raw)
        result = b'{"results":[]}'
        (base / 'old-result.json').write_bytes(result)
        entry = {'sha256': retention.digest(raw), 'request': 'old-request.json', 'result': 'old-result.json', 'result_sha256': retention.digest(result), 'created_at': '2026-01-01T00:00:00Z'}
        new = {k: copy.deepcopy(v) for k, v in task.items() if k != 'input'}
        new['task_id'] = 'completed-task'
        new['clock_binding']['observation_sequence'] = '-11'
        native = {'format': 'gitcrawl.combined-producer.v1', 'phase': 'committed', 'committed_at': '2026-01-01T00:00:20Z', 'run_id': 7, 'request_sha256': 'completed-request', 'response_sha256': 'response', 'consumer_exclusions_sha256': 'policy', 'imported': 1, 'unchanged': 0, 'input_sha256': 'input', 'state_sha256': 'state', 'algorithm': 'algorithm'}
        native_path = handoff / 'native.json'
        native_path.write_bytes(retention.encoded(native))
        done = {'sha256': 'completed-request', 'stage': 'released', 'native_receipt': str(native_path), 'native_receipt_sha256': retention.digest(native_path.read_bytes()), 'result_sha256': 'response', 'native_bindings': [new], 'matched': 1, 'export': {'model': header['model'], 'dimensions': 1536}, 'request_header': header}
        done_path = base / 'completed-receipt.json'
        done_path.write_bytes(retention.encoded(done))
        calls = []
        def broker(argv, data=None, timeout=90):
            calls.append(argv)
            self.assertEqual(argv[-2], 'release')
            return retention.encoded({'status': 'released', 'request_sha256': argv[-1], 'removed': 1})
        cycle = maintain.Cycle({'state_dir': str(base), 'handoff_state': str(handoff), 'db': str(db), 'broker': ['fixture-broker']}, broker)
        cycle.validate_pending = lambda pending: None
        cycle.state = {'retained': [entry], 'pending': {'sentinel': 'preserve exactly'}}
        cycle.checkpoint()
        return cycle, entry, done, done_path, calls

    def test_exact_completed_retirement_preserves_pending_and_receipts(self):
        c, entry, done, path, calls = self.fixture()
        receipt = path.read_bytes()
        db_hash = retention.digest(Path(c.c['db']).read_bytes())
        original = copy.deepcopy(entry)
        result = retention.retire_completed(c, path)
        self.assertEqual(result['retired'], [entry['sha256']])
        self.assertEqual(c.state['pending'], {'sentinel': 'preserve exactly'})
        self.assertEqual(path.read_bytes(), receipt)
        self.assertEqual(retention.digest(Path(c.c['db']).read_bytes()), db_hash)
        proof = json.loads((c.base / entry['retirement']['proof']).read_bytes())
        self.assertEqual(proof['original_retained_entry'], original)
        self.assertEqual(len(calls), 1)
        self.assertFalse((c.base / 'old-request.json').exists())

    def test_partial_match_and_later_timestamp_do_not_authorize_retirement(self):
        c, entry, done, path, calls = self.fixture()
        raw = json.loads((c.base / entry['request']).read_bytes())
        extra = copy.deepcopy(raw['tasks'][0]); extra['task_id'] = 'unmatched'; extra['thread_binding']['thread_id'] = '2'
        raw['tasks'].append(extra)
        (c.base / entry['request']).write_bytes(retention.encoded(raw)); entry['sha256'] = retention.digest(retention.encoded(raw)); c.checkpoint()
        result = retention.retire_completed(c, path)
        self.assertEqual(result['retired'], [])
        self.assertEqual(len(c.state['retained']), 1)
        self.assertEqual(calls, [])

    def test_source_binding_mismatch_preserves_entry(self):
        c, entry, done, path, calls = self.fixture()
        done['request_header']['source_binding']['inode'] = 'other'
        path.write_bytes(retention.encoded(done))
        self.assertIsNone(retention.prove(c, entry, path))
        self.assertEqual(calls, [])

    def test_stale_clock_and_different_input_do_not_match(self):
        for field, value in [('clock_binding', {'observation_sequence': '-9', 'evidence_observation_sequence': '0'}), ('input_sha256', 'different')]:
            c, entry, done, path, calls = self.fixture()
            done['native_bindings'][0][field] = value
            path.write_bytes(retention.encoded(done))
            self.assertIsNone(retention.prove(c, entry, path))
            self.assertEqual(calls, [])

    def test_archive_publication_disagreement_refuses_release(self):
        c, entry, done, path, calls = self.fixture()
        with closing(sqlite3.connect(c.c['db'])) as db, db:
            db.execute('UPDATE cluster_runs SET stats_json=?', ('{}',))
        with self.assertRaisesRegex(RuntimeError, 'publication binding differs'):
            retention.retire_completed(c, path)
        self.assertEqual(calls, [])

    def test_active_lease_is_preserved_then_retried(self):
        c, entry, done, path, calls = self.fixture()
        original = c.call
        c.call = lambda *a, **k: (_ for _ in ()).throw(RuntimeError('native_request_inflight'))
        result = retention.retire_completed(c, path)
        self.assertEqual(result['active_lease_wait'], [entry['sha256']])
        self.assertTrue((c.base / entry['request']).exists())
        c.call = original
        self.assertEqual(retention.resume(c)['retired'], [entry['sha256']])

    def test_crash_after_remote_release_before_checkpoint_replays_only_release(self):
        c, entry, done, path, calls = self.fixture()
        def crash(argv, data=None, timeout=90):
            calls.append(argv)
            raise KeyboardInterrupt('remote released, local checkpoint interrupted')
        c.call = crash
        with self.assertRaises(KeyboardInterrupt):
            retention.retire_completed(c, path)
        on_disk = json.loads(c.statepath.read_bytes())
        self.assertEqual(on_disk['retained'][0]['retirement']['stage'], 'release_intent')
        def replay(argv, data=None, timeout=90):
            calls.append(argv)
            self.assertEqual(argv[-2], 'release')
            return retention.encoded({'status': 'released', 'request_sha256': argv[-1], 'removed': 0})
        resumed = maintain.Cycle(c.c, replay)
        self.assertEqual(retention.resume(resumed)['retired'], [entry['sha256']])
        self.assertEqual(len(calls), 2)
        self.assertEqual(resumed.state['pending'], {'sentinel': 'preserve exactly'})

    def test_partial_local_cleanup_resumes_without_broker_call(self):
        c, entry, done, path, calls = self.fixture()
        original = Path.unlink
        def fail_result(p, *a, **k):
            if p.name == 'old-result.json':
                raise OSError('synthetic cleanup interruption')
            return original(p, *a, **k)
        with patch.object(Path, 'unlink', fail_result):
            with self.assertRaises(OSError):
                retention.retire_completed(c, path)
        self.assertFalse((c.base / 'old-request.json').exists())
        self.assertTrue((c.base / 'old-result.json').exists())
        resumed = maintain.Cycle(c.c, lambda *a, **k: self.fail('duplicate broker release'))
        self.assertEqual(retention.resume(resumed)['retired'], [entry['sha256']])

    def test_legacy_requires_exact_explicit_target(self):
        c, entry, done, path, calls = self.fixture()
        done.pop('request_header'); path.write_bytes(retention.encoded(done))
        self.assertIsNone(retention.prove(c, entry, path))
        with self.assertRaisesRegex(RuntimeError, 'exact retained digest'):
            retention.retire_completed(c, path, legacy=True)
        self.assertEqual(retention.retire_completed(c, path, target=entry['sha256'], legacy=True)['retired'], [entry['sha256']])

    def test_partial_envelope_cleanup_keeps_unproven_second_envelope(self):
        c, entry, done, path, calls = self.fixture()
        other = copy.deepcopy(entry); other['request'] = 'other-request.json'; other.pop('result'); other.pop('result_sha256')
        raw = json.loads((c.base / entry['request']).read_bytes()); raw['tasks'][0]['thread_binding']['thread_id'] = '99'
        (c.base / other['request']).write_bytes(retention.encoded(raw)); other['sha256'] = retention.digest(retention.encoded(raw))
        c.state['retained'].append(other); c.checkpoint()
        self.assertEqual(retention.retire_completed(c, path)['retired'], [entry['sha256']])
        self.assertEqual(c.state['retained'], [other])
        self.assertTrue((c.base / other['request']).exists())

    def test_partial_proof_write_does_not_block_replay(self):
        c, entry, done, path, calls = self.fixture()
        with patch.object(retention.os, 'link', side_effect=OSError('synthetic interrupted publication')):
            with self.assertRaises(OSError):
                retention.retire_completed(c, path)
        self.assertNotIn('retirement', entry)
        self.assertEqual(calls, [])
        self.assertEqual(retention.retire_completed(c, path)['retired'], [entry['sha256']])

    def test_successful_finish_checks_retained_completion_proof(self):
        c, entry, done, path, calls = self.fixture()
        done.update(request='already-cleaned.json', native_publication=json.loads(Path(done['native_receipt']).read_bytes()))
        c.state['pending'] = done
        c.checkpoint()
        c.finish(done)
        self.assertEqual(c.state['retained'], [])
        self.assertNotIn('pending', c.state)
        self.assertEqual(len(calls), 1)

    def test_one_retirement_frees_full_cap_for_normal_stale_replacement(self):
        c, entry, done, path, calls = self.fixture()
        raw = (c.base / entry['request']).read_bytes()
        for i in range(15):
            c.state['retained'].append({'sha256': 'unproven-' + str(i), 'request': 'untouched-' + str(i)})
        (c.base / 'pending.json').write_bytes(raw)
        c.state['pending'] = {'request': 'pending.json', 'sha256': retention.digest(raw), 'export': {'tasks': 1}, 'inspect': {}, 'submit': {}}
        c.checkpoint()
        self.assertEqual(retention.retire_completed(c, path, target=entry['sha256'])['retired'], [entry['sha256']])
        self.assertEqual(len(c.state['retained']), 15)
        c.source_ready = lambda: True
        c.retirement_scope = lambda: None
        c.archive_superseded = lambda: {'archived': []}
        c.policy_refresh = lambda: 'policy'
        c.verify_import = lambda *a: False
        c.c['combined_invocation'] = ['fixture-combined']
        (Path(c.c['handoff_state']) / 'last-failure.json').write_text('{"phase":"prepare"}')
        exported = []
        def new_packet():
            self.assertEqual(len(c.state['retained']), 16)
            exported.append(True)
            request = json.loads(raw); request['new'] = True
            value = retention.encoded(request)
            (c.base / 'replacement.json').write_bytes(value)
            c.state['pending'] = {'request': 'replacement.json', 'sha256': retention.digest(value), 'inspect': {}, 'submit': {}}
            c.checkpoint()
            return True
        c.new_packet = new_packet
        c.broker = lambda mode, value: {'status': 'pending', 'failed': 1} if json.loads(value).get('new') else {'status': 'complete', 'results': []}
        c.call = lambda *a, **kw: (_ for _ in ()).throw(RuntimeError('native prepare failed: stale native identity/clock task fixture'))
        self.assertEqual(c.run()['status'], 'provider_failure')
        self.assertEqual(exported, [True])
        self.assertEqual(len(c.state['retained']), 16)

    def zero_fixture(self):
        c, entry, done, path, calls = self.fixture()
        c.policy.write_bytes(b'{}')
        policy = retention.digest(b'{}')
        request = json.loads((c.base / entry['request']).read_bytes())
        request['source_binding']['consumer_exclusions_sha256'] = policy
        raw = retention.encoded(request)
        (c.base / entry['request']).write_bytes(raw)
        entry['sha256'] = retention.digest(raw)
        c.checkpoint()
        native = json.loads(Path(done['native_receipt']).read_bytes())
        native.update(consumer_exclusions_sha256=policy, eligible=1, members=1, cluster_scope={"mode":"full-native"})
        # A changed input does not need to reappear: the native full publication
        # plus a fresh zero-task eligibility result supersedes the old task.
        zero = {'tasks': 0, 'database_writes': False, 'provider_calls': False,
                'source_identity': retention.archive_identity(c), 'consumer_exclusions_sha256': policy}
        return c, entry, native, zero, calls

    def test_changed_input_frees_slot_with_fresh_zero_scope_and_publication(self):
        c, entry, native, zero, calls = self.zero_fixture()
        before = retention.capture_scope(c)
        result = retention.retire_zero_scope(c, before, native, zero)
        self.assertEqual(result['retired'], [entry['sha256']])
        self.assertEqual(len(calls), 1)
        self.assertEqual(c.state['pending'], {'sentinel': 'preserve exactly'})

    def test_zero_tasks_without_publication_or_stable_heads_cannot_retire(self):
        c, entry, native, zero, calls = self.zero_fixture()
        before = retention.capture_scope(c)
        native['phase'] = 'prepared'
        with self.assertRaisesRegex(RuntimeError, 'incomplete'):
            retention.retire_zero_scope(c, before, native, zero)
        native['phase'] = 'committed'
        with closing(sqlite3.connect(c.c['db'])) as db, db:
            db.execute('UPDATE threads SET observation_sequence=-21')
        self.assertEqual(retention.retire_zero_scope(c, before, native, zero)['retired'], [])
        self.assertEqual(calls, [])

    def test_new_due_task_or_policy_change_preserves_envelope(self):
        c, entry, native, zero, calls = self.zero_fixture()
        before = retention.capture_scope(c)
        zero['tasks'] = 1
        self.assertEqual(retention.retire_zero_scope(c, before, native, zero)['retired'], [])
        zero['tasks'] = 0
        zero['consumer_exclusions_sha256'] = 'changed'
        self.assertEqual(retention.retire_zero_scope(c, before, native, zero)['retired'], [])
        self.assertEqual(calls, [])

    def test_old_policy_entry_does_not_strand_scope_retirement(self):
        c, entry, native, zero, calls = self.zero_fixture()
        old = copy.deepcopy(entry)
        old.update(request='old-policy-request.json', invalidated='consumer_policy_changed')
        old.pop('result'); old.pop('result_sha256')
        request = json.loads((c.base / entry['request']).read_bytes())
        request['source_binding']['consumer_exclusions_sha256'] = 'earlier-same-set-binding'
        raw = retention.encoded(request)
        (c.base / old['request']).write_bytes(raw)
        old['sha256'] = retention.digest(raw)
        c.state['retained'].insert(0, old)
        before = retention.capture_scope(c)
        self.assertIn(old['sha256'], before['entries'])
        c.state['scope_retirement_pending'] = {'before': before, 'publication': native}
        c.checkpoint()
        resumed = maintain.Cycle(c.c, c.call)
        resumed.zero_export = lambda: zero
        resumed.finish_scope_retirement()
        self.assertNotIn('scope_retirement_pending', resumed.state)
        self.assertEqual(resumed.state['retained'], [old])
        self.assertEqual((c.base / old['request']).read_bytes(), raw)
        self.assertEqual([call[-1] for call in calls], [entry['sha256']])
        again = maintain.Cycle(c.c, c.call)
        again.finish_scope_retirement()
        self.assertEqual(again.state['retained'], [old])
        self.assertEqual(len(calls), 1)

    def test_changed_input_cycles_do_not_grow_retained_cap(self):
        c, entry, native, zero, calls = self.zero_fixture()
        original_request = json.loads((c.base / entry['request']).read_bytes())
        for cycle in range(20):
            request = copy.deepcopy(original_request)
            task = request['tasks'][0]
            task.update(input='old-'+str(cycle), input_sha256=retention.digest(('old-'+str(cycle)).encode()), input_bytes=len('old-'+str(cycle)))
            raw = retention.encoded(request)
            name = 'cycle-'+str(cycle)+'.json'
            (c.base / name).write_bytes(raw)
            current = {'sha256': retention.digest(raw), 'request': name}
            c.state['retained'] = [current]
            c.checkpoint()
            self.assertEqual(retention.retire_zero_scope(c, retention.capture_scope(c), native, zero)['retired'], [current['sha256']])
            self.assertEqual(c.state['retained'], [])
        self.assertEqual(len(calls), 20)

    def test_zero_scope_active_lease_and_release_crash_replay(self):
        c, entry, native, zero, calls = self.zero_fixture()
        before = retention.capture_scope(c)
        original = c.call
        c.call = lambda *a, **k: (_ for _ in ()).throw(RuntimeError('native_request_inflight'))
        self.assertEqual(retention.retire_zero_scope(c, before, native, zero)['active_lease_wait'], [entry['sha256']])
        c.call = lambda *a, **k: (_ for _ in ()).throw(KeyboardInterrupt('after broker release'))
        with self.assertRaises(KeyboardInterrupt):
            retention.resume(c)
        resumed = maintain.Cycle(c.c, original)
        self.assertEqual(retention.resume(resumed)['retired'], [entry['sha256']])

    def test_zero_scope_missing_source_row_is_not_provider_deletion(self):
        c, entry, native, zero, calls = self.zero_fixture()
        before = retention.capture_scope(c)
        with closing(sqlite3.connect(c.c['db'])) as db, db:
            db.execute('DELETE FROM threads')
        self.assertEqual(retention.retire_zero_scope(c, before, native, zero)['retired'], [])
        self.assertEqual(calls, [])

    def test_completion_ownership_survives_pre_intent_checkpoint_crash(self):
        c, entry, done, path, calls = self.fixture()
        done.update(request='already-cleaned.json', native_publication=json.loads(Path(done['native_receipt']).read_bytes()))
        c.state['pending'] = done
        c.checkpoint()
        original = c.checkpoint
        def crash_before_intent_checkpoint():
            if any(e.get('retirement') for e in c.state['retained']):
                raise KeyboardInterrupt('before retained retirement intent checkpoint')
            original()
        c.checkpoint = crash_before_intent_checkpoint
        with self.assertRaises(KeyboardInterrupt):
            c.finish(done)
        disk = json.loads(c.statepath.read_bytes())
        self.assertIn('pending', disk)
        self.assertEqual(disk['pending']['sha256'], done['sha256'])
        self.assertEqual(calls, [])
        resumed = maintain.Cycle(c.c, c.call)
        self.assertEqual(resumed.run()['status'], 'recovered_release')
        self.assertNotIn('pending', resumed.state)
        self.assertEqual(resumed.state['retained'], [])
        self.assertEqual(len(calls), 1)

    def test_zero_scope_completion_keeps_pending_until_intent_checkpoint(self):
        c, entry, native, zero, calls = self.zero_fixture()
        before = retention.capture_scope(c)
        done_path = c.base / 'completed-receipt.json'
        done = json.loads(done_path.read_bytes())
        done['request_header']['source_binding']['consumer_exclusions_sha256'] = zero['consumer_exclusions_sha256']
        native_path = Path(done['native_receipt'])
        native_path.write_bytes(retention.encoded(native))
        done['native_receipt_sha256'] = retention.digest(native_path.read_bytes())
        done.update(request='already-cleaned.json', native_publication=native, retirement_scope_before=before)
        c.state['pending'] = done
        c.checkpoint()
        c.zero_export = lambda: zero
        # This specifically exercises the zero-scope gate, after the exact-input
        # gate cannot prove a replacement for legitimately changed old input.
        with patch.object(retention, 'retire_completed', return_value={'retired': []}):
            original = c.checkpoint
            def crash():
                if any(e.get('retirement') for e in c.state['retained']):
                    raise KeyboardInterrupt('zero-scope pre-intent checkpoint')
                original()
            c.checkpoint = crash
            with self.assertRaises(KeyboardInterrupt):
                c.finish(done)
        self.assertIn('pending', json.loads(c.statepath.read_bytes()))
        resumed = maintain.Cycle(c.c, c.call)
        resumed.zero_export = lambda: zero
        with patch.object(retention, 'retire_completed', return_value={'retired': []}):
            self.assertEqual(resumed.run()['status'], 'recovered_release')
        self.assertEqual(resumed.state['retained'], [])
        self.assertNotIn('pending', resumed.state)


    def test_partial_zero_scope_never_retires_full_work(self):
        c, entry, native, zero, calls = self.zero_fixture()
        native['cluster_scope'] = {'mode': 'available-subset'}
        before = retention.capture_scope(c)
        result = retention.retire_zero_scope(c, before, native, zero)
        self.assertEqual(result['retired'], [])
        self.assertEqual(calls, [])
        self.assertEqual(len(c.state['retained']), 1)


if __name__ == '__main__':
    unittest.main()
