import hashlib
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import zipfile

ROOT = Path(__file__).resolve().parents[1]


def module(name):
    spec = importlib.util.spec_from_file_location(name, ROOT / 'tools' / (name + '.py'))
    obj = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(obj)
    return obj


class ReleaseTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.pack = module('package_release')
        (self.root / 'code.py').write_text('print(1)\n')
        self.manifest(['code.py'])

    def manifest(self, paths):
        entries = [{'path': p, 'bytes': (self.root/p).stat().st_size,
                    'sha256': self.pack.sha(self.root/p)} for p in paths]
        (self.root/'RELEASE_MANIFEST.json').write_text(json.dumps({'files': entries}))

    def test_extra_secret_is_not_packaged_and_source_unchanged(self):
        (self.root/'.env').write_text('SECRET=test')
        before = (self.root/'code.py').read_bytes()
        out = self.root/'out.zip'
        self.pack.package(self.root, out)
        with zipfile.ZipFile(out) as z:
            self.assertEqual(set(z.namelist()), {'IT5427_TON_IoT_release/code.py',
                                               'IT5427_TON_IoT_release/RELEASE_MANIFEST.json'})
        self.assertEqual(before, (self.root/'code.py').read_bytes())
        with self.assertRaises(ValueError): self.pack.package(self.root, out)

    def test_secret_in_manifest_rejected(self):
        (self.root/'.env').write_text('SECRET=test')
        self.manifest(['.env'])
        with self.assertRaises(ValueError): self.pack.payload(self.root)

    def test_changed_file_rejected(self):
        (self.root/'code.py').write_text('changed')
        with self.assertRaises(ValueError): self.pack.payload(self.root)

    def test_symlink_rejected(self):
        (self.root/'link.py').symlink_to(self.root/'code.py')
        self.manifest(['link.py'])
        with self.assertRaises(ValueError): self.pack.payload(self.root)

    def test_dataset_exact_duplicate_extra_and_tamper(self):
        checker = module('check_dataset')
        raw = self.root/'raw'; raw.mkdir()
        p = raw/'one.csv'; p.write_bytes(b'a\n1\n')
        manifest = {'files': [{'file_name':'one.csv','size_bytes':4,
                              'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}]}
        self.assertEqual(checker.check(raw,manifest), [])
        (raw/'sub').mkdir(); (raw/'sub/one.csv').write_bytes(p.read_bytes())
        self.assertTrue(checker.check(raw,manifest))
        (raw/'sub/one.csv').unlink(); p.write_bytes(b'a\n2\n')
        self.assertTrue(checker.check(raw,manifest))


if __name__ == '__main__': unittest.main()
