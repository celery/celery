#import eventlet
#eventlet.monkey_patch()

from celery import group
import socket
from stress.app import add, raising

def on_ready(result):
    print('RESULT: %r' % (result.get(),))

finished = [0]

def test():
    #group(add.s(i, i) for i in range(1000)).delay().then(on_ready)

    p = group(add.s(i, i) for i in range(1000)).delay()
    x = p.get(timeout=5)
    y = p.get(timeout=5)
    try:
        assert x == y
    except AssertionError:
        print('-' * 64)
        print('X: %r' % (x,))
        print('Y: %r' % (y,))
        raise
    assert not any(m is None for m in x)
    assert not any(m is None for m in y)

    #p = add.delay(2, 2)
    #print(p.get(timeout=5))
    #p = add.delay(2, 2)
    #print(p.get(timeout=5))
    #p = add.delay(2, 2)
    #print(p.get(timeout=5))
    #p = add.delay(2, 2)
    #print(p.get(timeout=5))
    #p = raising.delay()
    #try:
    #    print(p.get(timeout=5))
    #except Exception as exc:
    #    print('raised: %r' % (exc),)
    finished[0] += 1


for i in range(10):
    test()


#for i in range(2):
#    eventlet.spawn(test)

#while finished[0] < 100:
#    import time
#    time.sleep(0)
