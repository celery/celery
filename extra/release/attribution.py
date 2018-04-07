#!/usr/bin/env python
from __future__ import absolute_import, unicode_literals

import fileinput
from pprint import pprint


def author(line):
    try:
        A, E = line.strip().rsplit(None, 1)
        E.replace('>', '').replace('<', '')
    except ValueError:
        A, E = line.strip(), None
    return A.lower() if A else A, E.lower() if E else E


def proper_name(name):
    return name and ' ' in name


def find_missing_authors(seen):
    with open('AUTHORS') as authors:
        known = [author(line) for line in authors.readlines()]

    seen_authors = {t[0] for t in seen if proper_name(t[0])}
    known_authors = {t[0] for t in known}
    # maybe later?:
    #   seen_emails = {t[1] for t in seen}
    #   known_emails = {t[1] for t in known}

    pprint(seen_authors - known_authors)


if __name__ == '__main__':
    find_missing_authors([author(line) for line in fileinput.input()])
