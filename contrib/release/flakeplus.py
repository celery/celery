#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import with_statement

import os
import pprint
import re
import sys

from collections import defaultdict
from itertools import starmap
from unipath import Path

RE_COMMENT = r'^\s*\#'
RE_NOQA = r'.+?\#\s+noqa+'
RE_MULTILINE_COMMENT_O = r'^\s*(?:\'\'\'|""").+?(?:\'\'\'|""")'
RE_MULTILINE_COMMENT_S = r'^\s*(?:\'\'\'|""")'
RE_MULTILINE_COMMENT_E = r'(?:^|.+?)(?:\'\'\'|""")'
RE_WITH = r'(?:^|\s+)with\s+'
RE_WITH_IMPORT = r'''from\s+ __future__\s+ import\s+ with_statement'''
RE_PRINT = r'''(?:^|\s+)print\((?:"|')(?:\W+?)?[A-Z0-9:]{2,}'''
RE_ABS_IMPORT = r'''from\s+ __future__\s+ import\s+ absolute_import'''

acc = defaultdict(lambda: {"abs": False, "print": False})


def compile(regex):
    return re.compile(regex, re.VERBOSE)


class FlakePP(object):
    re_comment = compile(RE_COMMENT)
    re_ml_comment_o = compile(RE_MULTILINE_COMMENT_O)
    re_ml_comment_s = compile(RE_MULTILINE_COMMENT_S)
    re_ml_comment_e = compile(RE_MULTILINE_COMMENT_E)
    re_abs_import = compile(RE_ABS_IMPORT)
    re_print = compile(RE_PRINT)
    re_with_import = compile(RE_WITH_IMPORT)
    re_with = compile(RE_WITH)
    re_noqa = compile(RE_NOQA)
    map = {"abs": False, "print": False,
            "with": False, "with-used": False}

    def __init__(self, verbose=False):
        self.verbose = verbose
        self.steps = (("abs", self.re_abs_import),
                      ("with", self.re_with_import),
                      ("with-used", self.re_with),
                      ("print", self.re_print))

    def analyze_fh(self, fh):
        steps = self.steps
        filename = fh.name
        acc = dict(self.map)
        index = 0
        errors = [0]

        def error(fmt, **kwargs):
            errors[0] += 1
            self.announce(fmt, **dict(kwargs, filename=filename))

        for index, line in enumerate(self.strip_comments(fh)):
            for key, pattern in self.steps:
                if pattern.match(line):
                    acc[key] = True
        if index:
            if not acc["abs"]:
                error("%(filename)s: missing abs import")
            if acc["with-used"] and not acc["with"]:
                error("%(filename)s: missing with import")
            if acc["print"]:
                error("%(filename)s: left over print statement")

        return filename, errors[0], acc

    def analyze_file(self, filename):
        with open(filename) as fh:
            return self.analyze_fh(fh)

    def analyze_tree(self, dir):
        for dirpath, _, filenames in os.walk(dir):
            for path in (Path(dirpath, f) for f in filenames):
                if path.endswith(".py"):
                    yield self.analyze_file(path)

    def analyze(self, *paths):
        for path in map(Path, paths):
            if path.isdir():
                for res in self.analyze_tree(path):
                    yield res
            else:
                yield self.analyze_file(path)

    def strip_comments(self, fh):
        re_comment = self.re_comment
        re_ml_comment_o = self.re_ml_comment_o
        re_ml_comment_s = self.re_ml_comment_s
        re_ml_comment_e = self.re_ml_comment_e
        re_noqa = self.re_noqa
        in_ml = False

        for line in fh.readlines():
            if in_ml:
                if re_ml_comment_e.match(line):
                    in_ml = False
            else:
                if re_noqa.match(line) or re_ml_comment_o.match(line):
                    pass
                elif re_ml_comment_s.match(line):
                    in_ml = True
                elif re_comment.match(line):
                    pass
                else:
                    yield line

    def announce(self, fmt, **kwargs):
        sys.stderr.write((fmt + "\n") % kwargs)


def main(argv=sys.argv, exitcode=0):
    for _, errors, _ in FlakePP(verbose=True).analyze(*argv[1:]):
        if errors:
            exitcode = 1
    return exitcode


if __name__ == "__main__":
    sys.exit(main())
