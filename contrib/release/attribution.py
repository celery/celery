#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import with_statement

import fileinput

from pprint import pprint


def author(line):
    try:
        A, E = line.strip().rsplit(None, 1)
        E.replace(">", "").replace("<", "")
    except ValueError:
        A, E = line.strip(), None
    return A.lower() if A else A, E.lower() if E else E


def proper_name(name):
    return name and " " in name


def find_missing_authors(seen):
    with open("AUTHORS") as authors:
        known = map(author, authors.readlines())

    seen_authors = set(filter(proper_name, (t[0] for t in seen)))
    seen_emails = set(t[1] for t in seen)
    known_authors = set(t[0] for t in known)
    known_emails = set(t[1] for t in known)

    pprint(seen_authors - known_authors)


if __name__ == "__main__":
    find_missing_authors(map(author, fileinput.input()))

