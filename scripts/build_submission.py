"""Package only project source, documentation and the supplied brief; verify every archive member."""
import hashlib
from pathlib import Path
import zipfile

ROOT = Path(__file__).resolve().parents[1]
TOP_LEVEL = (
    'AGENT.md', 'README.md', 'Dockerfile', 'compose.yaml', 'alembic.ini',
    'pyproject.toml', 'requirements.txt', 'requirements-dev.txt', '.gitignore',
    '.dockerignore', 'Mini_Distributed_Cloud_Object_Store.pdf',
)


def main():
    sources = [ROOT / name for name in TOP_LEVEL]
    for directory, extensions in (
        ('object_store', {'.py'}), ('migrations', {'.py'}), ('scripts', {'.py'}),
        ('tests', {'.py'}), ('docs', {'.md', '.json'}),
    ):
        sources.extend(path for path in (ROOT / directory).rglob('*')
                       if path.is_file() and path.suffix in extensions and '__pycache__' not in path.parts)
    files = {}
    for path in sorted(sources):
        if path.is_symlink() or not path.is_file():
            raise RuntimeError(f'Expected a regular project file: {path}')
        files[path.relative_to(ROOT).as_posix()] = path.read_bytes()
    manifest = ''.join(f'{hashlib.sha256(data).hexdigest()}  {name}\n' for name, data in files.items())
    files['MANIFEST.sha256'] = manifest.encode()
    output = ROOT / 'dist'
    output.mkdir(exist_ok=True)
    archive = output / 'mini-distributed-object-store.zip'
    with zipfile.ZipFile(archive, 'w', compression=zipfile.ZIP_DEFLATED) as bundle:
        for name, data in files.items():
            info = zipfile.ZipInfo('mini-distributed-object-store/' + name, (2026, 1, 1, 0, 0, 0))
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            bundle.writestr(info, data)
    with zipfile.ZipFile(archive) as bundle:
        if bundle.testzip() is not None or len(bundle.namelist()) != len(files):
            raise RuntimeError('Submission archive integrity check failed')
        for name, data in files.items():
            if bundle.read('mini-distributed-object-store/' + name) != data:
                raise RuntimeError('Archive member mismatch: ' + name)
    digest = hashlib.sha256(archive.read_bytes()).hexdigest()
    (output / 'SHA256SUMS').write_text(f'{digest}  {archive.name}\n')
    print(f'Verified {len(files)} archive members: {archive}\nSHA-256: {digest}')


if __name__ == '__main__':
    main()
