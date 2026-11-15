"""Check the exact 28 approved CSVs before ingestion; never changes data."""
import argparse
import hashlib
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def check(folder, manifest):
    expected = {x['file_name']: x for x in manifest['files']}
    found = {}
    errors = []
    for path in sorted(folder.rglob('*.csv')):
        if path.is_symlink():
            errors.append(f'Symlink rejected: {path.name}')
            continue
        found.setdefault(path.name, []).append(path)
    for name, entry in expected.items():
        paths = found.get(name, [])
        if len(paths) != 1:
            errors.append(f'{name}: expected one file, found {len(paths)}')
            continue
        path = paths[0]
        if path.stat().st_size != entry['size_bytes']:
            errors.append(f'{name}: size differs from approved dataset')
            continue
        h = hashlib.sha256()
        with path.open('rb') as stream:
            for block in iter(lambda: stream.read(4 * 1024**2), b''):
                h.update(block)
        if h.hexdigest() != entry['sha256']:
            errors.append(f'{name}: SHA256 differs from approved dataset')
    for name in sorted(set(found) - set(expected)):
        errors.append(f'Unexpected CSV in selected scope: {name}')
    return errors


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('folder', nargs='?', default='data/raw', type=Path)
    a = p.parse_args()
    manifest = json.loads((ROOT / 'config/dataset_manifest.json').read_text())
    errors = check(a.folder, manifest)
    print(json.dumps({'status': 'failed' if errors else 'passed',
                      'expected_files': len(manifest['files']), 'errors': errors}, indent=2))
    return 1 if errors else 0


if __name__ == '__main__':
    raise SystemExit(main())
