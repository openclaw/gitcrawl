"""Owned source objects for official-main code indexing. Never a serving checkout."""
import datetime
import json
import os
from pathlib import Path
import re
import shutil
import signal
import stat
import subprocess
import time
import uuid

REMOTE = 'https://github.com/openclaw/openclaw.git'
BRANCH = 'main'
CACHE_BUDGET_BYTES = 4 << 30
CACHE_COMPACT_BYTES = 2 << 30
MIN_FREE_BYTES = 2 << 30


def now():
    return datetime.datetime.now(datetime.timezone.utc).isoformat()


def atomic(path, value):
    raw = (json.dumps(value, sort_keys=True) + '\n').encode()
    temporary = path.with_name(path.name + '.tmp-' + uuid.uuid4().hex)
    with open(temporary, 'xb') as f:
        os.chmod(temporary, 0o600)
        f.write(raw); f.flush(); os.fsync(f.fileno())
    os.replace(temporary, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _stop_git(process):
    def send(signum):
        try:
            os.killpg(process.pid, signum)
            return True
        except ProcessLookupError:
            return False
    if send(signal.SIGTERM):
        deadline = time.monotonic()+1
        try:
            process.communicate(timeout=1)
        except subprocess.TimeoutExpired:
            pass
        # The Git parent can exit before a helper that closed its pipes.
        while send(0):
            remaining = deadline-time.monotonic()
            if remaining <= 0:
                send(signal.SIGKILL)
                break
            time.sleep(min(.05, remaining))
    process.communicate()


def git_command(repo, args, timeout=120):
    env = {k: v for k, v in os.environ.items() if not k.upper().startswith('GIT_')}
    env.update(GIT_CONFIG_GLOBAL=os.devnull, GIT_CONFIG_NOSYSTEM='1',
               GIT_TERMINAL_PROMPT='0', GIT_ASKPASS='/usr/bin/false',
               GIT_OPTIONAL_LOCKS='0', GIT_NO_REPLACE_OBJECTS='1')
    command = ['/usr/bin/git', '-c', 'core.hooksPath='+os.devnull,
               '-c', 'maintenance.auto=false', '-c', 'gc.auto=0',
               '-c', 'protocol.ext.allow=never', '-C', str(repo), *args]
    # Git can spawn repack, prune, fetch, and transport helpers. Keep their
    # lifetime inside the controller lock even if Git times out or is stopped.
    process = None
    interrupted = False
    handlers = {}
    def defer(_signum, _frame):
        nonlocal interrupted
        interrupted = True
    try:
        # Latch every stop request until the whole Git group has been handled.
        # The normal controller handler raises immediately and could interrupt
        # either process startup or cleanup. A short wait bounds stop latency.
        for signum in (signal.SIGINT, signal.SIGTERM):
            handlers[signum] = signal.getsignal(signum)
            signal.signal(signum, defer)
        process = subprocess.Popen(command, env=env, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, start_new_session=True)
        deadline = time.monotonic()+timeout
        while True:
            if interrupted:
                raise KeyboardInterrupt('upstream git interrupted')
            remaining = deadline-time.monotonic()
            if remaining <= 0:
                raise subprocess.TimeoutExpired(command, timeout)
            try:
                stdout, stderr = process.communicate(timeout=min(.2, remaining))
                if interrupted:
                    raise KeyboardInterrupt('upstream git interrupted')
                break
            except subprocess.TimeoutExpired:
                continue
    except BaseException:
        if process is not None:
            _stop_git(process)
        raise
    finally:
        for signum, handler in handlers.items():
            signal.signal(signum, handler)
    if interrupted:
        raise KeyboardInterrupt('upstream git interrupted')
    if process.returncode:
        # Do not copy transport/helper messages or potential proxy credentials.
        raise RuntimeError('upstream git '+args[0]+' failed, exit='+str(process.returncode))
    if args[0] == 'gc' and stderr:
        raise RuntimeError('upstream git gc reported a diagnostic despite exiting successfully')
    if len(stdout) > 1 << 20:
        raise RuntimeError('upstream metadata output exceeds1MiB')
    return stdout.decode().strip()


class SourceCache:
    def __init__(self, state, source, git=git_command):
        self.state = Path(state).resolve()
        self.root = self.state/'upstream'
        self.repo = self.root/'source.git'
        # The CLI cannot turn this into a checkout/reset of any external tree.
        if Path(source).absolute() != self.repo and Path(source).resolve() != self.repo:
            raise RuntimeError('source must be the dedicated state/upstream/source.git cache')
        self.git = git

    def _allocated(self):
        allocated = 0
        for parent, directories, files in os.walk(self.repo, followlinks=False):
            for name in directories+files:
                path = Path(parent)/name
                if path.is_symlink():
                    raise RuntimeError('owned source metadata symlink refused')
                if path.is_file():
                    allocated += path.stat().st_size
        return allocated

    @staticmethod
    def _verify_stage(stage):
        directories = [stage]
        while directories:
            for path in directories.pop().iterdir():
                info = path.lstat()
                if stat.S_ISDIR(info.st_mode):
                    directories.append(path)
                elif not (stat.S_ISREG(info.st_mode) and info.st_nlink == 1):
                    raise RuntimeError('source initialization unsafe metadata refused')

    def owned(self):
        if self.root.is_symlink() or self.repo.is_symlink():
            raise RuntimeError('upstream cache symlink refused')
        created = not self.root.exists()
        self.root.mkdir(mode=0o700, exist_ok=True)
        if self.root.stat().st_uid != os.getuid() or self.root.stat().st_mode & 0o077:
            raise RuntimeError('upstream cache must be owner-private')
        identity = {'format': 'gitcrawl.code-cache.v1', 'repository_url': REMOTE,
                    'branch': BRANCH, 'git_dir': str(self.repo)}
        marker = self.root/'owner.json'
        if created:
            atomic(marker, identity)
        if marker.is_symlink() or not marker.exists() or json.loads(marker.read_bytes()) != identity:
            raise RuntimeError('upstream cache ownership marker differs')
        if not self.repo.exists():
            # A killed initial fetch/init never promotes a half-configured cache.
            # This fixed staging name belongs only to the checked owner marker.
            stage = self.root/'initializing.git'
            if stage.is_symlink():
                raise RuntimeError('source initialization symlink refused')
            if any(os.path.lexists(stage/name) for name in ('commondir', '.git')):
                raise RuntimeError('source initialization Git directory redirection refused')
            stage.mkdir(mode=0o700, exist_ok=True)
            self._verify_stage(stage)
            self.git(stage, ['init', '--bare', '--template=', '.'])
            self.git(stage, ['config', 'remote.origin.url', REMOTE])
            self.git(stage, ['config', 'remote.origin.fetch', '+refs/heads/main:refs/remotes/origin/main'])
            os.rename(stage, self.repo)
            fd = os.open(self.root, os.O_RDONLY)
            try:
                os.fsync(fd)
            finally:
                os.close(fd)
        if os.path.lexists(self.repo/'commondir'):
            raise RuntimeError('owned source Git common directory redirection refused')
        git_dirs = self.git(self.repo, ['rev-parse', '--path-format=absolute',
                                       '--absolute-git-dir', '--git-common-dir']).splitlines()
        if len(git_dirs) != 2 or any(Path(path).resolve() != self.repo for path in git_dirs):
            raise RuntimeError('owned source Git directory identity differs')
        allowed = {'core.repositoryformatversion': '0', 'core.filemode': 'true', 'core.bare': 'true',
                   'remote.origin.url': REMOTE,
                   'remote.origin.fetch': '+refs/heads/main:refs/remotes/origin/main'}
        # Reject local transport redirection, helpers, includes, filters and aliases.
        raw = self.git(self.repo, ['config', '--local', '--null', '--list'])
        config = {}
        for entry in raw.rstrip('\0').split('\0'):
            key, sep, value = entry.partition('\n')
            if key in ('core.ignorecase', 'core.precomposeunicode') and value in ('true', 'false'):
                continue  # Git's filesystem probes; no worktree is created.
            if not sep or key in config or key not in allowed or value != allowed[key]:
                raise RuntimeError('unsupported owned source Git configuration')
            config[key] = value
        if config != allowed or self.git(self.repo, ['rev-parse', '--is-bare-repository']) != 'true':
            raise RuntimeError('owned source repository identity differs')
        for path in ['objects/info/alternates', 'objects/info/http-alternates', 'info/grafts', 'refs/replace']:
            if (self.repo/path).exists():
                raise RuntimeError('owned source object redirection refused')
        if shutil.disk_usage(self.root).free < MIN_FREE_BYTES:
            raise RuntimeError('upstream cache disk budget unavailable')
        allocated = self._allocated()
        if allocated > CACHE_COMPACT_BYTES:
            # Compact unreachable objects from prior shallow fetches inside the
            # owned bare cache only; never touch archives or receipts.
            self.git(self.repo, ['gc', '--quiet', '--prune=now'], timeout=300)
            allocated = self._allocated()
        if allocated > CACHE_BUDGET_BYTES or shutil.disk_usage(self.root).free < MIN_FREE_BYTES:
            raise RuntimeError('upstream cache disk budget unavailable')

    def capture(self, run_id):
        if not re.fullmatch('[a-f0-9]{32}', run_id):
            raise RuntimeError('invalid source run ID')
        self.owned()
        started = now()
        # Fetch occurs while ordinary collection is running. No worktree is used.
        self.git(self.repo, ['fetch', '--quiet', '--depth=1', '--no-tags',
                            '--no-auto-maintenance', '--no-recurse-submodules',
                            'origin', '+refs/heads/main:refs/remotes/origin/main'])
        observed = now()
        commit = self.git(self.repo, ['rev-parse', '--verify', 'refs/remotes/origin/main^{commit}'])
        if not re.fullmatch('[a-f0-9]{40}', commit):
            raise RuntimeError('invalid official upstream commit ID')
        committed = self.git(self.repo, ['show', '-s', '--format=%cI', commit])
        datetime.datetime.fromisoformat(committed.replace('Z', '+00:00'))
        tree = self.git(self.repo, ['rev-parse', commit+'^{tree}'])
        self.owned()
        receipt = {'format': 'gitcrawl.code-upstream.v1', 'repository_url': REMOTE,
                   'branch': BRANCH, 'git_dir': str(self.repo), 'commit': commit,
                   'commit_time': committed, 'tree': tree,
                   'fetch_started_at': started, 'observed_at': observed}
        path = self.root/(run_id+'-upstream.json')
        if path.exists():
            raise RuntimeError('upstream receipt collision')
        atomic(path, receipt)
        return receipt, path
