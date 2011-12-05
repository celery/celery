from functools import wraps
from Queue import Queue

from celery.utils import cached_property


def coroutine(fun):
    """Decorator that turns a generator into a coroutine that is
    started automatically, and that can send values back to the caller.

    **Example coroutine that returns values to caller**::

        @coroutine
        def adder(self):
            while 1:
            x, y = (yield)
            self.give(x + y)

        >>> c = adder()

        # call sends value and returns the result.
        >>> c.call(4, 4)
        8

        # or you can send the value and get the result later.
        >>> c.send(4, 4)
        >>> c.get()
        8


    **Example sink (input-only coroutine)**::

        @coroutine
        def uniq():
            seen = set()
            while 1:
                line = (yield)
                if line not in seen:
                    seen.add(line)
                    print(line)

        >>> u = uniq()
        >>> [u.send(l) for l in [1, 2, 2, 3]]
        [1, 2, 3]

    **Example chaining coroutines**::

        @coroutine
        def uniq(callback):
            seen = set()
            while 1:
                line = (yield)
                if line not in seen:
                    callback.send(line)
                    seen.add(line)

        @coroutine
        def uppercaser(callback):
            while 1:
                line = (yield)
                callback.send(str(line).upper())

        @coroutine
        def printer():
            while 1:
                line = (yield)
                print(line)

        >>> pipe = uniq(uppercaser(printer()))
        >>> for line in file("AUTHORS").readlines():
                pipe.send(line)

    """
    @wraps(fun)
    def start(*args, **kwargs):
        return Coroutine.start_from(fun, *args, **kwargs)
    return start


class Coroutine(object):
    _gen = None
    started = False

    def bind(self, generator):
        self._gen = generator

    def _next(self):
        return self._gen.next()
    next = __next__ = _next

    def start(self):
        if self.started:
            raise ValueError("coroutine already started")
        self.next()
        self.started = True
        return self

    def send1(self, value):
        return self._gen.send(value)

    def call1(self, value, timeout=None):
        self.send1(value)
        return self.get(timeout=timeout)

    def send(self, *args):
        return self._gen.send(args)

    def call(self, *args, **opts):
        self.send(*args)
        return self.get(**opts)

    @classmethod
    def start_from(cls, fun, *args, **kwargs):
        coro = cls()
        coro.bind(fun(coro, *args, **kwargs))
        return coro.start()

    @cached_property
    def __output__(self):
        return Queue()

    @property
    def give(self):
        return self.__output__.put_nowait

    @property
    def get(self):
        return self.__output__.get

if __name__ == "__main__":

    @coroutine
    def adder(self):
        while 1:
            x, y = (yield)
            self.give(x + y)

    x = adder()
    for i in xrange(10):
        print(x.call(i, i))
