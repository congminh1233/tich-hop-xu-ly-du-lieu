import importlib.util
from pathlib import Path
import sys
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch
import json
import zipfile

MODULE = Path(__file__).resolve().parents[1] / 'scripts/diagnose_hdfs.py'
spec = importlib.util.spec_from_file_location('diagnostic', MODULE)
d = importlib.util.module_from_spec(spec)
spec.loader.exec_module(d)


class DiagnosticTests(unittest.TestCase):
    def test_supervisor_success(self):
        result = d.supervise([sys.executable, '-c', 'pass'], seconds=2)
        self.assertEqual(result['exit_code'], 0)
        self.assertFalse(result['timed_out'])

    def test_supervisor_preserves_failure(self):
        result = d.supervise([sys.executable, '-c', 'raise SystemExit(7)'], seconds=2)
        self.assertEqual(result['exit_code'], 7)

    def test_timeout_dumps_and_reaps(self):
        import os
        dumped = []
        result = d.supervise([sys.executable, '-c', 'import time;time.sleep(30)'],
                             seconds=0.5, dump_after=(0.1, 0.3), dump=dumped.append)
        self.assertEqual(result['exit_code'], 124)
        self.assertEqual(len(dumped), 2)
        self.assertLess(result['elapsed_seconds'], 3)
        with self.assertRaises(ProcessLookupError):
            os.kill(dumped[0], 0)

    def host_case(self, fail):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root/'infra/hdfs').mkdir(parents=True)
            (root/'infra/hdfs/compose.yaml').write_text('')
            (root/'scripts').mkdir()
            calls = []
            def fake_run(command, log, **kwargs):
                Path(log).write_text('fake transport log\n')
                if '--mode' not in command:
                    return 0
                mode = command[command.index('--mode')+1]
                calls.append(mode)
                if mode == fail:
                    return 124
                d.save(Path(log).parent/(mode+'-result.json'), {'status':'passed'})
                return 0
            with patch.object(d, '__file__', str(root/'scripts/diagnose_hdfs.py')), \
                 patch.object(d, 'stream_command', side_effect=fake_run), \
                 patch.object(d.os, 'getuid', return_value=1000):
                code = d.host(SimpleNamespace(snapshot='2f14f1cbb8e6c322fa283c26'))
            archives = list((root/'reports/hdfs').glob('*.zip'))
            self.assertEqual(len(archives), 1)
            with zipfile.ZipFile(archives[0]) as z:
                self.assertIsNone(z.testzip())
                result = json.loads(z.read(next(n for n in z.namelist() if n.endswith('/diagnosis.json'))))
            return code, calls, result

    def test_stops_at_failure_and_packages(self):
        code, calls, result = self.host_case('hdfs')
        self.assertEqual(code, 1)
        self.assertEqual(calls, ['local', 'hdfs'])
        self.assertEqual(result['first_failed_probe'], 'hdfs')

    def test_all_pass_is_diagnostic_not_verify(self):
        code, calls, result = self.host_case(None)
        self.assertEqual(code, 0)
        self.assertEqual(calls, list(d.MODES))
        self.assertEqual(result['status'], 'all_probes_passed')


if __name__ == '__main__':
    unittest.main()
