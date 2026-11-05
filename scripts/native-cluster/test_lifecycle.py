import json
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest.mock import patch

import run_code_update as m


class LifecycleTests(unittest.TestCase):
    def test_rejected_prepare_reaps_producer_and_closes_pipes(self):
        popen = m.subprocess.Popen
        for exited in (True, False):
            with self.subTest(exited=exited):
                temp = tempfile.TemporaryDirectory()
                self.addCleanup(temp.cleanup)
                root = Path(temp.name).resolve()
                producer = root / 'producer'
                producer.write_text(
                    '#!' + sys.executable + '\nimport time\n'
                    'print(\'{"phase":"invalid"}\', flush=True)\n' +
                    ('' if exited else 'time.sleep(60)\n'))
                producer.chmod(0o700)
                args = types.SimpleNamespace(
                    state_dir=str(root), label='fixture', binary=str(producer),
                    mode='cluster', db=str(root / 'archive.sqlite'),
                    consumer_exclusions=str(root / 'exclusions.json'), write_seconds=1)
                calls = []
                def run(argv, timeout=20):
                    calls.append(argv)
                    self.fail('unexpected lifecycle call')
                control = m.Lifecycle(args, run)
                control.verify = lambda: None
                children = []
                def spawn(*args, **kwargs):
                    child = popen(*args, **kwargs)
                    children.append(child)
                    def cleanup():
                        if child.poll() is None:
                            child.kill()
                        try:
                            child.wait(timeout=5)
                        finally:
                            child.stdout.close()
                            child.stderr.close()
                    self.addCleanup(cleanup)
                    if exited:
                        child.wait(timeout=5)
                    return child
                with patch.object(m.subprocess, 'Popen', side_effect=spawn):
                    with self.assertRaisesRegex(RuntimeError, 'unexpected native producer phase'):
                        control.update()
                self.assertEqual(calls, [])
                self.assertEqual(len(children), 1)
                self.assertIsNotNone(children[0].poll())
                self.assertEqual((children[0].stdout.closed, children[0].stderr.closed),
                                 (True, True))
                failure = json.loads((root / 'last-failure.json').read_text())
                self.assertEqual(failure['error'], 'unexpected native producer phase')


if __name__ == '__main__':
    unittest.main()
