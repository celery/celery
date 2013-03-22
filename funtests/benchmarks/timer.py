# -*- coding: utf-8 -*-
from __future__ import absolute_import

import sys

from time import sleep
from celery.utils import timer2 as timer

def noop(*args, **kwargs):
    return


def insert(s, n=100000):
    for i in xrange(n):
        s.apply_after(1 + (i and i / 10.0), noop, (i, ))


def slurp(s, n=100000):
    i = 0
    it = iter(s)
    while i < n:
        delay, entry = next(it)
        if entry:
            i += 1
            s.apply_entry(entry)
        #else:
            #if delay:
            #    sleep(delay)

if __name__ == '__main__':
    s = timer.Schedule()
    insert(s)
    if '--insert-only' not in sys.argv:
        slurp(s)

