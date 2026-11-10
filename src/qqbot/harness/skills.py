"""Immutable skill resources shared by native pi discovery and host execution."""

import argparse
import hashlib
import json
import os
import re
import shutil
import stat
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath
from uuid import uuid4

MAX_SKILL_BYTES = 20 * 1024 * 1024
SKILLSETS = Path('/opt/qqbot/skillsets')
DEFAULT_SKILLS = Path(__file__).resolve().parents[3] / 'skills'


class SkillVersionUnavailable(ValueError):
    pass


def skill_files(directory):
    directory = Path(directory)
    if directory.is_symlink() or not directory.is_dir():
        raise SkillVersionUnavailable('skill directory must be a real directory')
    files = {}
    total = 0
    for parent, directories, names in os.walk(directory, followlinks=False):
        directories[:] = sorted(name for name in directories if name != '.git')
        for name in directories:
            if (Path(parent) / name).is_symlink():
                raise SkillVersionUnavailable('skill directories cannot contain symbolic links')
        for name in sorted(names):
            path = Path(parent) / name
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            try:
                info = os.fstat(fd)
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
                    raise SkillVersionUnavailable('skill resources must be regular files without hardlinks')
                with os.fdopen(fd, 'rb', closefd=False) as source:
                    data = source.read(MAX_SKILL_BYTES + 1)
            finally:
                os.close(fd)
            total += len(data)
            if total > MAX_SKILL_BYTES or len(files) >= 512:
                raise SkillVersionUnavailable('skill resources exceed the bounded bundle size')
            files[path.relative_to(directory).as_posix()] = data
    return files


def skill_version(files):
    manifest = [(path, hashlib.sha256(data).hexdigest()) for path, data in sorted(files.items())]
    return hashlib.sha256(json.dumps(manifest, separators=(',', ':')).encode()).hexdigest()


def trusted_skill_directory(directory):
    directory = Path(directory)
    for path in (directory, *directory.parents):
        info = path.lstat()
        if not stat.S_ISDIR(info.st_mode) or info.st_uid != 0 or info.st_mode & 0o022:
            raise SkillVersionUnavailable('production skill paths require root-owned non-writable directories')
    for parent, directories, names in os.walk(directory, followlinks=False):
        for name in directories + names:
            info = (Path(parent) / name).lstat()
            if info.st_uid != 0 or info.st_mode & 0o022 or stat.S_ISLNK(info.st_mode):
                raise SkillVersionUnavailable('production skill resources require immutable root ownership')


def publish_skills(source, destination):
    if Path(destination).resolve().is_relative_to(Path(source).resolve()):
        raise ValueError("skill snapshots must be outside the maintained source directory")
    files = skill_files(source)
    version = skill_version(files)
    destination = Path(destination)
    destination.mkdir(parents=True, exist_ok=True)
    target = destination / version
    if not target.exists():
        temporary = Path(tempfile.mkdtemp(prefix='.skills-', dir=destination))
        try:
            for relative, data in files.items():
                output = temporary / relative
                output.parent.mkdir(parents=True, exist_ok=True)
                output.write_bytes(data)
                output.chmod(0o444)
            for parent, _directories, _names in os.walk(temporary, topdown=False):
                Path(parent).chmod(0o555)
            try:
                temporary.rename(target)
            except OSError:
                if not target.is_dir():
                    raise
        finally:
            if temporary.exists():
                for parent, _directories, _names in os.walk(temporary):
                    Path(parent).chmod(0o700)
                shutil.rmtree(temporary)
    if skill_version(skill_files(target)) != version:
        raise SkillVersionUnavailable('existing skill snapshot differs from its version')
    return version


@dataclass(frozen=True)
class SkillSnapshot:
    version: str
    directory: Path
    hashes: dict

    def read(self, path, offset=0, limit=65536):
        relative = PurePosixPath(path).relative_to('/skills')
        if not relative.parts or any(part in {'.', '..'} for part in relative.parts):
            raise ValueError('invalid skill resource path')
        if type(offset) is not int or type(limit) is not int or offset < 0 or not 1 <= limit <= 65536:
            raise ValueError('invalid skill resource page')
        key = relative.as_posix()
        if key not in self.hashes:
            raise ValueError('skill resource is absent from the pinned version')
        from types import SimpleNamespace

        from qqbot.capabilities.gateway import workspace_file
        fd = workspace_file(SimpleNamespace(workspace=str(self.directory)), key, os.O_RDONLY)
        with os.fdopen(fd, 'rb') as source:
            data = source.read(MAX_SKILL_BYTES + 1)
        if hashlib.sha256(data).hexdigest() != self.hashes[key]:
            raise SkillVersionUnavailable('pinned skill resource changed')
        return {'path': '/skills/' + key, 'text': data[offset:offset + limit].decode(errors='replace'),
                'next_offset': offset + limit if len(data) > offset + limit else None,
                'skill_version': self.version, 'sha256': self.hashes[key]}


class SkillStore:
    def __init__(self, source, directory, *, production=False):
        self.source = Path(source).resolve()
        self.directory = Path(directory).absolute() if production else Path(directory).resolve()
        self.production = production
        self.current_version = None

    def select(self, version=None):
        try:
            if version is None:
                if self.production:
                    current = self.directory / 'current'
                    if current.lstat().st_uid != 0:
                        raise SkillVersionUnavailable('production skill selection requires root ownership')
                    selected = current.resolve(strict=True)
                    if selected.parent != self.directory or not re.fullmatch(r'[0-9a-f]{64}', selected.name):
                        raise SkillVersionUnavailable('invalid production skill selection')
                    version = selected.name
                else:
                    version = publish_skills(self.source, self.directory)
                self.current_version = version
            if not isinstance(version, str) or not re.fullmatch(r'[0-9a-f]{64}', version):
                raise SkillVersionUnavailable('invalid skill version')
            path = self.directory / version
            if self.production:
                trusted_skill_directory(path)
            files = skill_files(path)
            if skill_version(files) != version:
                raise SkillVersionUnavailable('pinned skill version has changed')
            return SkillSnapshot(version, path, {key: hashlib.sha256(data).hexdigest() for key, data in files.items()})
        except (OSError, ValueError) as error:
            raise SkillVersionUnavailable(f'Fixed skill version unavailable ({version or "current"}). Restore that version before retrying.') from error


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('source')
    parser.add_argument('destination')
    args = parser.parse_args()
    root = Path(args.destination)
    version = publish_skills(args.source, root)
    temporary = root / ('.current-' + uuid4().hex)
    try:
        temporary.symlink_to(version, target_is_directory=True)
        os.replace(temporary, root / 'current')
    finally:
        temporary.unlink(missing_ok=True)
    print(version)


if __name__ == '__main__':
    main()
