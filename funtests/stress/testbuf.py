from __future__ import absolute_import

import sys
import time

from celery.result import ResultSet

from stress.app import app, sleeping



def testbuf(padbytes=0, megabytes=0):
    padding = float(padbytes) + 2 ** 20 * float(megabytes)
    results = []
    print('> padding: %r' % (padding, ))

    for i in range(8 * 4):
        results.append(sleeping.delay(1, kw='x' * int(padding)))
        time.sleep(0.01)

    res = ResultSet(results)
    print(res.join())

testbuf(*sys.argv[1:])
