"""Repackage only the reviewed payload; reject changes, secrets and symlinks."""
import argparse
import hashlib
import json
from pathlib import Path, PurePosixPath
import zipfile

ROOT = Path(__file__).resolve().parents[1]


def sha(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1024**2), b''):
            h.update(block)
    return h.hexdigest()


def payload(root):
    entries = json.loads((root / 'RELEASE_MANIFEST.json').read_text())['files']
    paths = []
    seen = set()
    for item in entries:
        name = item['path']
        rel = PurePosixPath(name)
        if rel.is_absolute() or '..' in rel.parts or '\\' in name or name in seen:
            raise ValueError('Invalid/duplicate manifest path')
        seen.add(name)
        if any(p in ('.env', 'passwords.json', '.venv', '__pycache__', '.git', 'dist') for p in rel.parts):
            raise ValueError('Forbidden payload: ' + name)
        path = root.joinpath(*rel.parts)
        if any(p.is_symlink() for p in (path, *path.parents)) or not path.resolve().is_relative_to(root.resolve()):
            raise ValueError('Symlink/outside path rejected: ' + name)
        if not path.is_file() or path.stat().st_size != item['bytes'] or sha(path) != item['sha256']:
            raise ValueError('Reviewed payload changed or missing: ' + name)
        paths.append(path)
    manifest = root / 'RELEASE_MANIFEST.json'
    if manifest.is_symlink():
        raise ValueError('Symlink manifest rejected')
    return sorted(paths) + [manifest]


def package(root, output, dry_run=False):
    paths = payload(root)
    if output.exists() or output.is_symlink():
        raise ValueError('Output already exists')
    if dry_run:
        return {'status': 'dry_run_passed', 'files': len(paths),
                'bytes': sum(p.stat().st_size for p in paths),
                'paths': [p.relative_to(root).as_posix() for p in paths]}
    output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(output, 'x', zipfile.ZIP_DEFLATED) as z:
        for path in paths:
            z.write(path, 'IT5427_TON_IoT_release/' + path.relative_to(root).as_posix())
    with zipfile.ZipFile(output) as z:
        bad = z.testzip()
        if bad:
            raise ValueError('Invalid ZIP entry: ' + bad)
    checksum = output.with_suffix(output.suffix + '.sha256')
    with checksum.open('x') as f:
        f.write(sha(output) + '  ' + output.name + '\n')
    return {'status': 'packaged', 'file': str(output), 'bytes': output.stat().st_size,
            'sha256': sha(output), 'files': len(paths)}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--output', type=Path, default=ROOT.parent / 'IT5427_TON_IoT_release.zip')
    p.add_argument('--dry-run', action='store_true')
    a = p.parse_args()
    print(json.dumps(package(ROOT, a.output.resolve(), a.dry_run), indent=2))


if __name__ == '__main__':
    main()
