"""Proof-bound retirement of superseded native broker envelopes; no archive writes."""
from contextlib import closing
import hashlib
import json
import os
from pathlib import Path
import sqlite3
import uuid


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def encoded(value):
    return (json.dumps(value, sort_keys=True, separators=(',', ':')) + '\n').encode()


def immutable(path, value):
    return immutable_bytes(path, encoded(value))


def immutable_bytes(path, raw):
    path = Path(path)
    if path.is_symlink():
        raise RuntimeError('retirement receipt symlink refused')
    if path.exists():
        if path.read_bytes() != raw:
            raise RuntimeError('retirement receipt already exists with different bytes')
        return digest(raw)
    temporary = path.with_name(path.name + '.tmp-' + uuid.uuid4().hex)
    try:
        fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        with os.fdopen(fd, 'wb') as output:
            output.write(raw)
            output.flush()
            os.fsync(output.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.is_symlink() or path.read_bytes() != raw:
                raise RuntimeError('retirement receipt already exists with different bytes')
    finally:
        temporary.unlink(missing_ok=True)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return digest(raw)


def owned_file(base, name):
    if not isinstance(name, str) or Path(name).name != name:
        raise RuntimeError('retirement local filename invalid')
    path = Path(base) / name
    if path.is_symlink():
        raise RuntimeError('retirement local symlink refused')
    return path


def task_key(task):
    names = ('input_sha256', 'native_content_hash', 'thread_binding', 'input_bytes',
             'input_runes', 'text_truncated', 'original_text_runes')
    return encoded({name: task[name] for name in names})


def follows(new, old):
    # The native clock sequence is signed (negative for fetch observations).
    # Input and provider identity must also match; sequence/time alone is no proof.
    a, b = new['clock_binding'], old['clock_binding']
    if a == b:
        return True
    x, y = int(a['observation_sequence']), int(b['observation_sequence'])
    return x < 0 and y < 0 and abs(x) > abs(y) and a['evidence_observation_sequence'] == b['evidence_observation_sequence'] == '0'


def source_and_publication(cycle, source, repository, native):
    path = Path(cycle.c['db']).resolve()
    before = path.stat()
    if (str(path), str(before.st_dev), str(before.st_ino)) != (source['archive_path'], source['device'], source['inode']):
        raise RuntimeError('retirement source identity changed')
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
        db.execute('PRAGMA query_only=ON')
        db.execute('BEGIN')
        if db.execute('PRAGMA user_version').fetchone()[0] != source['schema_version']:
            raise RuntimeError('retirement source schema changed')
        repo = db.execute('SELECT full_name,coalesce(github_repo_id,\'\') FROM repositories WHERE id=?', (source['repo_id'],)).fetchone()
        if repo != (repository, source['provider_repo_id']):
            raise RuntimeError('retirement provider identity changed')
        run = db.execute('SELECT repo_id,status,stats_json FROM cluster_runs WHERE id=?', (native['run_id'],)).fetchone()
        if not run or str(run[0]) != source['repo_id'] or run[1] not in ('success', 'completed'):
            raise RuntimeError('retirement native publication absent')
        saved = json.loads(run[2]).get('native_producer', {})
        if any(saved.get(k) != native.get(k) for k in ('input_sha256', 'state_sha256', 'algorithm')):
            raise RuntimeError('retirement native publication binding differs')
    after = path.stat()
    if (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
        raise RuntimeError('retirement source replaced during verification')


def completed(cycle, path):
    path = Path(path)
    if path.parent.resolve() != cycle.base.resolve() or path.is_symlink():
        raise RuntimeError('completion receipt outside native state')
    raw = path.read_bytes()
    done = json.loads(raw)
    native_path = Path(done['native_receipt'])
    if native_path.parent.resolve() != Path(cycle.c['handoff_state']).resolve() or native_path.is_symlink():
        raise RuntimeError('native receipt outside handoff state')
    native_raw = native_path.read_bytes()
    if digest(native_raw) != done['native_receipt_sha256']:
        raise RuntimeError('native receipt checksum mismatch')
    native = json.loads(native_raw)
    bindings = done.get('native_bindings', [])
    if (done.get('stage') != 'released' or native.get('format') != 'gitcrawl.combined-producer.v1'
            or native.get('phase') != 'committed' or not native.get('committed_at')
            or native.get('request_sha256') != done.get('sha256')
            or native.get('response_sha256') != done.get('result_sha256')
            or native.get('imported', 0) + native.get('unchanged', 0) != done.get('matched')
            or len(bindings) != done.get('matched') or not bindings
            or len({t['task_id'] for t in bindings}) != len(bindings)):
        raise RuntimeError('completed replacement publication proof invalid')
    return done, native, {'path': str(path), 'sha256': digest(raw),
                          'native_path': str(native_path), 'native_sha256': digest(native_raw)}


def prove(cycle, retained, completion_path, legacy=False):
    done, native, publication = completed(cycle, completion_path)
    request_path = owned_file(cycle.base, retained['request'])
    if retained.get('invalidated') == 'consumer_policy_changed' and not request_path.exists():
        return None
    raw = request_path.read_bytes()
    if digest(raw) != retained['sha256']:
        raise RuntimeError('retained request checksum mismatch')
    request = json.loads(raw)
    header = {k: v for k, v in request.items() if k != 'tasks'}
    successor = done.get('request_header')
    fields = ('repository', 'model', 'dimensions', 'basis', 'native_hash_version', 'source_binding')
    if successor is not None:
        if any(successor.get(k) != header.get(k) for k in fields):
            return None
    elif not legacy:
        return None
    # Legacy recovery is explicit and single-envelope only. Its fixed producer
    # contract is additionally bound to the persisted archive run and provider ID.
    elif (header.get('repository') != 'openclaw/openclaw' or header.get('basis') != 'title_original'
          or header.get('native_hash_version') != 'embedding:v5'
          or header.get('source_binding', {}).get('source_commit') != '6c9c1bea7d094397dcdfecfb50e693460592a5aa'):
        return None
    export = done.get('export', {})
    if (export.get('model') != header['model'] or export.get('dimensions') != header['dimensions']
            or native.get('consumer_exclusions_sha256') != header['source_binding']['consumer_exclusions_sha256']):
        return None
    matches = []
    for task in request['tasks']:
        if digest(task['input'].encode()) != task['input_sha256'] or len(task['input'].encode()) != task['input_bytes']:
            raise RuntimeError('retained task input binding invalid')
        match = next((t for t in done['native_bindings'] if task_key(t) == task_key(task) and follows(t, task)), None)
        if match is None:
            return None
        matches.append({'retained_task_id': task['task_id'], 'completed_task_id': match['task_id'],
                        'input_sha256': task['input_sha256'], 'native_content_hash': task['native_content_hash'],
                        'thread_binding': task['thread_binding'], 'retained_clock': task['clock_binding'],
                        'completed_clock': match['clock_binding']})
    if not matches:
        return None
    source_and_publication(cycle, header['source_binding'], header['repository'], native)
    files = {retained['request']: retained['sha256']}
    if retained.get('result'):
        result_raw = owned_file(cycle.base, retained['result']).read_bytes()
        if digest(result_raw) != retained.get('result_sha256'):
            raise RuntimeError('retained result checksum mismatch')
        files[retained['result']] = retained['result_sha256']
    return {'format': 'gitcrawl.retirement-proof.v1', 'original_retained_entry': retained,
            'request_header': header, 'publication': publication, 'matches': matches,
            'files': files, 'legacy_explicit_recovery': legacy}


def resume(cycle):
    retired, waiting = [], []
    for entry in list(cycle.state['retained']):
        state = entry.get('retirement')
        if not state:
            continue
        proof_path = owned_file(cycle.base, state['proof'])
        raw = proof_path.read_bytes()
        if digest(raw) != state['proof_sha256']:
            raise RuntimeError('retirement intent proof changed')
        proof = json.loads(raw)
        if proof['original_retained_entry']['sha256'] != entry['sha256']:
            raise RuntimeError('retirement request binding changed')
        if proof.get('kind') == 'zero_scope_publication':
            zero_publication(cycle, proof)
        else:
            done, native, publication = completed(cycle, proof['publication']['path'])
            if publication != proof['publication']:
                raise RuntimeError('retirement publication changed')
            source_and_publication(cycle, proof['request_header']['source_binding'], proof['request_header']['repository'], native)
        if state['stage'] == 'release_intent':
            try:
                response = json.loads(cycle.call(cycle.c['broker'] + ['release', entry['sha256']], b''))
            except RuntimeError as error:
                if 'native_request_inflight' in str(error):
                    waiting.append(entry['sha256'])
                    continue
                raise
            if response.get('status') != 'released' or response.get('request_sha256') != entry['sha256']:
                raise RuntimeError('retirement broker response binding invalid')
            state.update(stage='released', response=response)
            cycle.checkpoint()
        if state['stage'] != 'released':
            raise RuntimeError('unknown retirement stage')
        immutable(cycle.base / (entry['sha256'] + '-retirement-receipt.json'),
                  {'proof': state['proof'], 'proof_sha256': state['proof_sha256'], 'release': state['response']})
        for name, expected in proof['files'].items():
            path = owned_file(cycle.base, name)
            if path.exists():
                if digest(path.read_bytes()) != expected:
                    raise RuntimeError('retirement local file changed; preserved')
                path.unlink()
        cycle.state['retained'].remove(entry)
        cycle.checkpoint()
        retired.append(entry['sha256'])
    return {'retired': retired, 'active_lease_wait': waiting}


def retire_completed(cycle, completion_path, target=None, legacy=False):
    if legacy and not target:
        raise RuntimeError('legacy retirement requires exact retained digest')
    for entry in list(cycle.state['retained']):
        if entry.get('retirement') or target and entry['sha256'] != target:
            continue
        proof = prove(cycle, entry, completion_path, legacy)
        if proof is None:
            continue
        name = entry['sha256'] + '-' + digest(encoded(proof))[:16] + '-retirement-proof.json'
        checksum = immutable(cycle.base / name, proof)
        entry['retirement'] = {'stage': 'release_intent', 'proof': name, 'proof_sha256': checksum}
        cycle.checkpoint()
    return resume(cycle)


def archive_identity(cycle):
    path = Path(cycle.c['db']).resolve()
    before = path.stat()
    with closing(sqlite3.connect(path.as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
        version = db.execute('PRAGMA user_version').fetchone()[0]
        repo = db.execute("SELECT id,coalesce(github_repo_id,'') FROM repositories WHERE full_name='openclaw/openclaw'").fetchone()
    after = path.stat()
    if not repo or (before.st_dev, before.st_ino) != (after.st_dev, after.st_ino):
        raise RuntimeError('zero-scope archive identity unavailable')
    return {'archive_path': str(path), 'device': str(before.st_dev), 'inode': str(before.st_ino),
            'schema_version': version, 'repo_id': str(repo[0]), 'provider_repo_id': repo[1]}


def capture_scope(cycle):
    """Bounded point metadata for retained tasks; never reads native bodies/vectors."""
    identity = archive_identity(cycle)
    result = {'identity': identity, 'policy_sha256': digest(cycle.policy.read_bytes()), 'entries': {}}
    with closing(sqlite3.connect(Path(cycle.c['db']).resolve().as_uri() + '?mode=ro', uri=True, timeout=5)) as db:
        db.execute('PRAGMA query_only=ON;')
        db.execute('BEGIN')
        for entry in cycle.state['retained']:
            if entry.get('retirement'):
                continue
            path = owned_file(cycle.base, entry['request'])
            if not path.exists() and entry.get('invalidated') == 'consumer_policy_changed':
                continue
            raw = path.read_bytes()
            if digest(raw) != entry['sha256']:
                raise RuntimeError('retained scope request checksum mismatch')
            request = json.loads(raw)
            if any(request['source_binding'].get(k) != v for k, v in identity.items()):
                continue
            heads = []
            for task in request['tasks']:
                thread = task['thread_binding']
                row = db.execute('SELECT id,repo_id,github_id,number,kind,state,closed_at_local,observation_sequence,evidence_observation_sequence,evidence_source_updated_at,updated_at_gh,updated_at FROM threads WHERE id=?', (thread['thread_id'],)).fetchone()
                if not row or str(row[1]) != request['source_binding']['repo_id'] or (str(row[0]), row[2], row[3], row[4]) != (thread['thread_id'], thread['github_id'], thread['number'], thread['kind']):
                    heads = []
                    break
                heads.append(list(row))
            if heads:
                result['entries'][entry['sha256']] = {'heads': heads, 'header': {k: v for k, v in request.items() if k != 'tasks'}, 'task_ids': [t['task_id'] for t in request['tasks']]}
    if archive_identity(cycle) != identity:
        raise RuntimeError('zero-scope archive replaced')
    return result


def zero_publication(cycle, proof):
    zero, native = proof['zero_export'], proof['native_publication']
    if native.get('cluster_scope',{}).get('mode')=='available-subset':
        raise RuntimeError('partial eligibility cannot prove full-scope retirement')
    if (zero.get('tasks') != 0 or zero.get('database_writes') is not False or zero.get('provider_calls') is not False
            or native.get('format') not in ('gitcrawl.cluster-producer.v1', 'gitcrawl.combined-producer.v1')
            or native.get('phase') not in ('committed', 'unchanged')
            or type(native.get('eligible')) is not int or native['eligible'] < 0
            or native.get('phase') == 'committed' and native.get('members') != native['eligible']):
        raise RuntimeError('zero-scope publication proof incomplete')
    if proof.get('native_receipt'):
        path = Path(proof['native_receipt']['path'])
        if path.parent.resolve() != Path(cycle.c['handoff_state']).resolve() or digest(path.read_bytes()) != proof['native_receipt']['sha256']:
            raise RuntimeError('zero-scope native receipt changed')
        saved = json.loads(path.read_bytes())
        for key in ('run_id', 'input_sha256', 'state_sha256', 'eligible', 'phase'):
            if saved.get(key) != native.get(key):
                raise RuntimeError('zero-scope native receipt binding differs')
    header = proof['request_header']
    if native.get('consumer_exclusions_sha256', zero['consumer_exclusions_sha256']) != zero['consumer_exclusions_sha256']:
        raise RuntimeError('zero-scope native policy differs')
    if any(header['source_binding'].get(k) != v for k, v in zero['source_identity'].items()):
        raise RuntimeError('zero-scope source binding differs')
    if zero['consumer_exclusions_sha256'] != header['source_binding']['consumer_exclusions_sha256']:
        raise RuntimeError('zero-scope policy binding differs')
    source_and_publication(cycle, header['source_binding'], header['repository'], native)


def retire_zero_scope(cycle, before, native, zero):
    """Full fresh native eligibility closes changed-input requests after publication."""
    if native.get("cluster_scope",{}).get("mode")=="available-subset":
        return {"retired": [], "active_lease_wait": []}
    if zero.get('tasks') != 0 or zero.get('source_identity') != before['identity'] or zero.get('consumer_exclusions_sha256') != before['policy_sha256']:
        return {'retired': [], 'active_lease_wait': []}
    after = capture_scope(cycle)
    if after['identity'] != before['identity'] or after['policy_sha256'] != before['policy_sha256']:
        raise RuntimeError('zero-scope source changed across publication')
    for entry in list(cycle.state['retained']):
        if entry.get('retirement'):
            continue
        left, right = before['entries'].get(entry['sha256']), after['entries'].get(entry['sha256'])
        if not left or left != right:
            continue
        header = right['header']
        # Old policy requests remain owned, but cannot obstruct compatible
        # entries or strand replay of the completed scope-retirement operation.
        if header['source_binding'].get('consumer_exclusions_sha256') != before['policy_sha256']:
            continue
        if header['source_binding'].get('source_commit') != '6c9c1bea7d094397dcdfecfb50e693460592a5aa':
            continue
        if (header.get('repository'), header.get('model'), header.get('dimensions'), header.get('basis'), header.get('native_hash_version')) != ('openclaw/openclaw', 'text-embedding-3-small', 1536, 'title_original', 'embedding:v5'):
            continue
        files = {entry['request']: entry['sha256']}
        if entry.get('result'):
            path = owned_file(cycle.base, entry['result'])
            if digest(path.read_bytes()) != entry['result_sha256']:
                raise RuntimeError('retained zero-scope result changed')
            files[entry['result']] = entry['result_sha256']
        proof = {'format': 'gitcrawl.retirement-proof.v1', 'kind': 'zero_scope_publication',
                 'original_retained_entry': entry, 'request_header': header, 'files': files,
                 'task_source_bindings_before_and_after': right, 'zero_export': zero,
                 'native_publication': native, 'native_receipt': None}
        if native.get('receipt'):
            path = Path(native['receipt'])
            proof['native_receipt'] = {'path': str(path), 'sha256': digest(path.read_bytes())}
        zero_publication(cycle, proof)
        name = entry['sha256'] + '-' + digest(encoded(proof))[:16] + '-retirement-proof.json'
        checksum = immutable(cycle.base / name, proof)
        entry['retirement'] = {'stage': 'release_intent', 'proof': name, 'proof_sha256': checksum}
        cycle.checkpoint()
    return resume(cycle)
