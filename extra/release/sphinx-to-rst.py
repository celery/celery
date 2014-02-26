#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function, unicode_literals

import codecs
import os
import re
import sys

from collections import Callable

dirname = ''

RE_CODE_BLOCK = re.compile(r'.. code-block:: (.+?)\s*$')
RE_INCLUDE = re.compile(r'.. include:: (.+?)\s*$')
RE_REFERENCE = re.compile(r':(.+?):`(.+?)`')
UNITABLE = {'…': '...'}
X = re.compile(re.escape('…'))
HEADER = re.compile('^[\=\~\-]+$')
UNIRE = re.compile('|'.join(re.escape(p) for p in UNITABLE),
                   re.UNICODE)


def _replace_handler(match, key=UNITABLE.__getitem__):
    return key(match.group(0))


def include_file(lines, pos, match):
    global dirname
    orig_filename = match.groups()[0]
    filename = os.path.join(dirname, orig_filename)
    fh = codecs.open(filename, encoding='utf-8')
    try:
        old_dirname = dirname
        dirname = os.path.dirname(orig_filename)
        try:
            lines[pos] = sphinx_to_rst(fh)
        finally:
            dirname = old_dirname
    finally:
        fh.close()


def asciify(lines):
    prev_diff = None
    for line in lines:
        new_line = UNIRE.sub(_replace_handler, line)
        if prev_diff and HEADER.match(new_line):
            new_line = ''.join([
                new_line.rstrip(), new_line[0] * prev_diff, '\n'])
        prev_diff = len(new_line) - len(line)
        yield new_line.encode('ascii')


def replace_code_block(lines, pos, match):
    lines[pos] = ''
    curpos = pos - 1
    # Find the first previous line with text to append "::" to it.
    while True:
        prev_line = lines[curpos]
        if not prev_line.isspace():
            prev_line_with_text = curpos
            break
        curpos -= 1

    if lines[prev_line_with_text].endswith(':'):
        lines[prev_line_with_text] += ':'
    else:
        lines[prev_line_with_text] += '::'

TO_RST_MAP = {RE_CODE_BLOCK: replace_code_block,
              RE_REFERENCE: r'``\2``',
              RE_INCLUDE: include_file}


def _process(lines):
    lines = list(lines)                                 # non-destructive
    for i, line in enumerate(lines):
        for regex, alt in TO_RST_MAP.items():
            if isinstance(alt, Callable):
                match = regex.match(line)
                if match:
                    alt(lines, i, match)
                    line = lines[i]
            else:
                lines[i] = regex.sub(alt, line)
    return asciify(lines)


def sphinx_to_rst(fh):
    return ''.join(_process(fh))


if __name__ == '__main__':
    global dirname
    dirname = os.path.dirname(sys.argv[1])
    fh = codecs.open(sys.argv[1], encoding='utf-8')
    try:
        print(sphinx_to_rst(fh))
    finally:
        fh.close()
