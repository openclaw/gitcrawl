"""Private durable handoff for completed superseded broker work, not source rows."""
from contextlib import closing
import sqlite3
import json
import math
import os
from pathlib import Path
import re

import retention


def private_directory(path):
    path = Path(path)
    if path.is_symlink():
        raise RuntimeError('broker archive symlink refused')
    path.mkdir(mode=0o700, exist_ok=True)
    stat = path.stat()
    if stat.st_uid != os.getuid() or stat.st_mode & 0o077:
        raise RuntimeError('broker archive must be owner-private')
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
    return path


def read_bounded(path, limit):
    if path.stat().st_size > limit:
        raise RuntimeError('broker archive input exceeds bound')
    raw = path.read_bytes()
    if len(raw) > limit:
        raise RuntimeError('broker archive input grew beyond bound')
    return raw


def complete_evidence(cycle, entry):
    if not re.fullmatch('[a-f0-9]{64}', entry.get('sha256', '')):
        raise RuntimeError('superseded request digest invalid')
    # Incomplete or privacy-invalidated requests remain owned by their old path.
    if not entry.get('result') or entry.get('invalidated'):
        return None
    request_raw = read_bounded(retention.owned_file(cycle.base, entry['request']), 16 << 20)
    result_raw = read_bounded(retention.owned_file(cycle.base, entry['result']), 128 << 20)
    if retention.digest(request_raw) != entry['sha256'] or retention.digest(result_raw) != entry.get('result_sha256'):
        raise RuntimeError('superseded request/result checksum mismatch')
    request, result = json.loads(request_raw), json.loads(result_raw)
    tasks = request.get('tasks', [])
    if (request.get('format') != 'gitcrawl.embedding.request.v1'
            or request.get('repository') != 'openclaw/openclaw' or request.get('basis') != 'title_original'
            or request.get('native_hash_version') != 'embedding:v5'
            or request.get('model') != 'text-embedding-3-small' or request.get('dimensions') != 1536
            or not 1 <= len(tasks) <= 2000):
        raise RuntimeError('superseded request contract invalid')
    if (result.get('format') != 'clawmmunity.native-embeddings.v1'
            or result.get('request_sha256') != entry['sha256']
            or result.get('model') != request['model'] or result.get('dimensions') != 1536):
        raise RuntimeError('superseded result binding invalid')
    if (result.get('status') != 'complete' or result.get('pending') != 0 or result.get('failed') != 0
            or result.get('ready') != len(tasks) or result.get('tasks') != len(tasks)):
        return None
    outputs = {row['task_id']: row for row in result.get('results', [])}
    if len(outputs) != len(tasks) or len(outputs) != len(result['results']) or set(outputs) != {t['task_id'] for t in tasks}:
        raise RuntimeError('superseded result task set differs')
    policy_raw = cycle.policy.read_bytes()
    blocked = set(json.loads(policy_raw)['excluded_thread_ids'])
    with closing(sqlite3.connect(Path(cycle.c['db']).resolve().as_uri()+'?mode=ro', uri=True, timeout=5)) as db:
        exclusions = db.execute('SELECT number,kind,original_thread_id FROM thread_exclusions WHERE repository=?', (request['repository'],)).fetchall()
    native_blocked = {(number, kind) for number, kind, _ in exclusions}
    blocked.update(str(original) for _, _, original in exclusions)
    for task in tasks:
        if task['thread_binding']['thread_id'] in blocked or (task['thread_binding']['number'], task['thread_binding']['kind']) in native_blocked:
            return None
        if retention.digest(task['input'].encode()) != task['input_sha256']:
            raise RuntimeError('superseded task input binding invalid')
        row = outputs[task['task_id']]
        vector = row.get('vector')
        if (row.get('input_sha256') != task['input_sha256'] or not isinstance(vector, list)
                or len(vector) != 1536 or not all(type(x) in (int, float) and math.isfinite(x) for x in vector)
                or not any(x != 0 for x in vector)):
            raise RuntimeError('superseded model result invalid')
    identity = retention.archive_identity(cycle)
    if any(request.get('source_binding', {}).get(k) != value for k, value in identity.items()):
        raise RuntimeError('superseded archive identity changed')
    bindings = {'request_header': {k: v for k, v in request.items() if k != 'tasks'},
                'tasks': [{k: v for k, v in task.items() if k != 'input'} for task in tasks]}
    return request_raw, result_raw, retention.encoded(bindings), identity


def manifest(cycle, entry, evidence):
    request, result, bindings, identity = evidence
    root = private_directory(cycle.base / 'completed-broker-archive')
    directory = private_directory(root / entry['sha256'])
    files = {}
    for name, raw in [('request.json', request), ('result.json', result), ('bindings.json', bindings)]:
        retention.immutable_bytes(directory / name, raw)
        files[name] = {'sha256': retention.digest(raw), 'bytes': len(raw)}
        if retention.digest((directory / name).read_bytes()) != files[name]['sha256']:
            raise RuntimeError('broker archive readback mismatch')
    entry_name = 'entry-' + retention.digest(retention.encoded(entry)) + '.json'
    retention.immutable(directory / entry_name, entry)
    value = {'format': 'gitcrawl.completed-broker-archive.v1', 'request_sha256': entry['sha256'],
             'result_sha256': entry['result_sha256'], 'files': files, 'source_identity': identity,
             'original_entry': entry_name, 'original_entry_sha256': retention.digest((directory / entry_name).read_bytes()),
             'current_consumer_policy_sha256': retention.digest(cycle.policy.read_bytes()),
             'disposition': 'completed superseded derived work; exact evidence preserved; not a native publication'}
    name = 'manifest-' + retention.digest(retention.encoded(value)) + '.json'
    checksum = retention.immutable(directory / name, value)
    return {'manifest': str(directory / name), 'manifest_sha256': checksum, 'stage': 'release_intent'}


def verify_archive(cycle, entry):
    intent = entry['archive_handoff']
    path = Path(intent['manifest'])
    root = (cycle.base / 'completed-broker-archive').resolve()
    if path.parent.parent.resolve() != root or path.is_symlink() or path.parent.is_symlink():
        raise RuntimeError('broker archive outside private root')
    raw = path.read_bytes()
    if retention.digest(raw) != intent['manifest_sha256']:
        raise RuntimeError('broker archive manifest changed')
    proof = json.loads(raw)
    if proof['request_sha256'] != entry['sha256'] or proof['source_identity'] != retention.archive_identity(cycle):
        raise RuntimeError('broker archive request/source binding changed')
    for name, value in proof['files'].items():
        file = retention.owned_file(path.parent, name)
        if file.stat().st_size != value['bytes'] or retention.digest(file.read_bytes()) != value['sha256']:
            raise RuntimeError('broker archive file changed')
    original = retention.owned_file(path.parent, proof['original_entry'])
    if retention.digest(original.read_bytes()) != proof['original_entry_sha256']:
        raise RuntimeError('broker archive original entry changed')
    return path, proof


def resume(cycle):
    archived, waiting = [], []
    for entry in list(cycle.state['retained']):
        intent = entry.get('archive_handoff')
        if not intent:
            continue
        path, proof = verify_archive(cycle, entry)
        if intent['stage'] == 'release_intent':
            try:
                result = json.loads(cycle.call(cycle.c['broker'] + ['release', entry['sha256']], b''))
            except RuntimeError as error:
                if 'native_request_inflight' in str(error):
                    waiting.append(entry['sha256'])
                    continue
                raise
            if result.get('status') != 'released' or result.get('request_sha256') != entry['sha256']:
                raise RuntimeError('archived broker release binding invalid')
            intent.update(stage='released', release=result)
            cycle.checkpoint()
        if intent['stage'] != 'released':
            raise RuntimeError('unknown broker archive handoff stage')
        retention.immutable(path.parent / 'retirement-receipt.json', {'manifest': path.name,
                            'manifest_sha256': intent['manifest_sha256'], 'release': intent['release']})
        for key, expected in [('request', entry['sha256']), ('result', entry['result_sha256'])]:
            file = retention.owned_file(cycle.base, entry[key])
            if file.exists():
                if retention.digest(file.read_bytes()) != expected:
                    raise RuntimeError('exclusive broker working copy changed; preserved')
                file.unlink()
        cycle.state['retained'].remove(entry)
        cycle.checkpoint()
        archived.append(entry['sha256'])
    return {'archived': archived, 'active_lease_wait': waiting}


def archive_superseded(cycle):
    """A submitted successor holds reusable jobs before old refs can be released."""
    summary = resume(cycle)
    if not cycle.state['retained']:
        return summary
    pending = cycle.state.get('pending', {})
    submitted = pending.get('submit', {})
    if submitted.get('status') != 'submitted' or submitted.get('request_sha256') != pending.get('sha256'):
        return summary
    raw = retention.owned_file(cycle.base, pending['request']).read_bytes()
    if retention.digest(raw) != pending['sha256']:
        raise RuntimeError('successor request binding changed')
    for entry in list(cycle.state['retained']):
        if entry.get('archive_handoff') or entry.get('retirement') or entry['sha256'] == pending['sha256']:
            continue
        evidence = complete_evidence(cycle, entry)
        if evidence is None:
            continue
        entry['archive_handoff'] = manifest(cycle, entry, evidence)
        entry['archive_handoff']['successor_request_sha256'] = pending['sha256']
        cycle.checkpoint()
    result = resume(cycle)
    return {key: summary[key] + result[key] for key in summary}
