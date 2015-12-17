from celery import group
import socket
from stress.app import add, raising

def on_ready(result):
    print('RESULT: %r' % (result,))

def test():
    group(add.s(i, i) for i in range(10)).delay().then(on_ready)

    p = group(add.s(i, i) for i in range(10)).delay()
    print(p.get(timeout=5))

    p = add.delay(2, 2)
    print(p.get(timeout=5))
    p = add.delay(2, 2)
    print(p.get(timeout=5))
    p = add.delay(2, 2)
    print(p.get(timeout=5))
    p = add.delay(2, 2)
    print(p.get(timeout=5))
    p = raising.delay()
    try:
        print(p.get(timeout=5))
    except Exception as exc:
        print('raised: %r' % (exc),)


for i in range(100):
    test()
