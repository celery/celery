#!/usr/bin/even/python
from __future__ import with_statement
import re
import sys

RE_CODE_BLOCK = re.compile(r'.. code-block:: (.+?)\s*$')
RE_REFERENCE = re.compile(r':(.+?):`(.+?)`')


def replace_code_block(lines, pos):
    lines[pos] = ""
    curpos = pos - 1
    # Find the first previous line with text to append "::" to it.
    while True:
        prev_line = lines[curpos]
        if not prev_line.isspace():
            prev_line_with_text = curpos
            break
        curpos -= 1

    if lines[prev_line_with_text].endswith(":"):
        lines[prev_line_with_text] += ":"
    else:
        lines[prev_line_with_text] += "::"

TO_RST_MAP = {RE_CODE_BLOCK: replace_code_block,
              RE_REFERENCE: r'``\2``'}


def _process(lines):
    lines = list(lines) # non-destructive
    for i, line in enumerate(lines):
        for regex, alt in TO_RST_MAP.items():
            if callable(alt):
                if regex.match(line):
                    alt(lines, i)
                    line = lines[i]
            else:
                lines[i] = regex.sub(alt, line)
    return lines


def sphinx_to_rst(fh):
    return "".join(_process(fh))


if __name__ == "__main__":
    with open(sys.argv[1]) as fh:
        print(sphinx_to_rst(fh))
