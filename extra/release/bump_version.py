#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import with_statement

import errno
import os
import re
import shlex
import subprocess
import sys

from contextlib import contextmanager
from tempfile import NamedTemporaryFile

rq = lambda s: s.strip("\"'")


def cmd(*args):
    return subprocess.Popen(args, stdout=subprocess.PIPE).communicate()[0]


@contextmanager
def no_enoent():
    try:
        yield
    except OSError, exc:
        if exc.errno != errno.ENOENT:
            raise


class StringVersion(object):

    def decode(self, s):
        s = rq(s)
        text = ""
        major, minor, release = s.split(".")
        if not release.isdigit():
            pos = release.index(re.split("\d+", release)[1][0])
            release, text = release[:pos], release[pos:]
        return int(major), int(minor), int(release), text

    def encode(self, v):
        return ".".join(map(str, v[:3])) + v[3]
to_str = StringVersion().encode
from_str = StringVersion().decode


class TupleVersion(object):

    def decode(self, s):
        v = list(map(rq, s.split(", ")))
        return (tuple(map(int, v[0:3])) +
                tuple(["".join(v[3:])]))

    def encode(self, v):
        v = list(v)

        def quote(lit):
            if isinstance(lit, basestring):
                return '"%s"' % (lit, )
            return str(lit)

        if not v[-1]:
            v.pop()
        return ", ".join(map(quote, v))


class VersionFile(object):

    def __init__(self, filename):
        self.filename = filename
        self._kept = None

    def _as_orig(self, version):
        return self.wb % {"version": self.type.encode(version),
                          "kept": self._kept}

    def write(self, version):
        pattern = self.regex
        with no_enoent():
            with NamedTemporaryFile() as dest:
                with open(self.filename) as orig:
                    for line in orig:
                        if pattern.match(line):
                            dest.write(self._as_orig(version))
                        else:
                            dest.write(line)
                os.rename(dest.name, self.filename)

    def parse(self):
        pattern = self.regex
        gpos = 0
        with open(self.filename) as fh:
            for line in fh:
                m = pattern.match(line)
                if m:
                    if "?P<keep>" in pattern.pattern:
                        self._kept, gpos = m.groupdict()["keep"], 1
                    return self.type.decode(m.groups()[gpos])


class PyVersion(VersionFile):
    regex = re.compile(r'^VERSION\s*=\s*\((.+?)\)')
    wb = "VERSION = (%(version)s)\n"
    type = TupleVersion()


class SphinxVersion(VersionFile):
    regex = re.compile(r'^:[Vv]ersion:\s*(.+?)$')
    wb = ':Version: %(version)s\n'
    type = StringVersion()


class CPPVersion(VersionFile):
    regex = re.compile(r'^\#\s*define\s*(?P<keep>\w*)VERSION\s+(.+)')
    wb = '#define %(kept)sVERSION "%(version)s"\n'
    type = StringVersion()


_filetype_to_type = {"py": PyVersion,
                     "rst": SphinxVersion,
                     "txt": SphinxVersion,
                     "c": CPPVersion,
                     "h": CPPVersion}

def filetype_to_type(filename):
    _, _, suffix = filename.rpartition(".")
    return _filetype_to_type[suffix](filename)


def bump(*files, **kwargs):
    version = kwargs.get("version")
    before_commit = kwargs.get("before_commit")
    files = [filetype_to_type(f) for f in files]
    versions = [v.parse() for v in files]
    current = list(reversed(sorted(versions)))[0]  # find highest

    if version:
        next = from_str(version)
    else:
        major, minor, release, text = current
        if text:
            raise Exception("Can't bump alpha releases")
        next = (major, minor, release + 1, text)

    print("Bump version from %s -> %s" % (to_str(current), to_str(next)))

    for v in files:
        print("  writing %r..." % (v.filename, ))
        v.write(next)

    if before_commit:
        cmd(*shlex.split(before_commit))

    print(cmd("git", "commit", "-m", "Bumps version to %s" % (to_str(next), ),
        *[f.filename for f in files]))
    print(cmd("git", "tag", "v%s" % (to_str(next), )))


def main(argv=sys.argv, version=None, before_commit=None):
    if not len(argv) > 1:
        print("Usage: distdir [docfile] -- <custom version>")
        sys.exit(0)

    args = []
    for arg in argv:
        if arg.startswith("--before-commit="):
            _, before_commit = arg.split('=')
        else:
            args.append(arg)

    if "--" in args:
        c = args.index('--')
        version = args[c + 1]
        argv = args[:c]
    bump(*args[1:], version=version, before_commit=before_commit)

if __name__ == "__main__":
    main()
