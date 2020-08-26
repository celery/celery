import itertools
import time

import pytest
from billiard.einfo import ExceptionInfo

pytest.importorskip('multiprocessing')


def do_something(i):
    return i * i


def long_something():
    time.sleep(1)


def raise_something(i):
    try:
        raise KeyError('FOO EXCEPTION')
    except KeyError:
        return ExceptionInfo()


class test_TaskPool:

    def setup(self):
        from celery.concurrency.prefork import TaskPool
        self.TaskPool = TaskPool

    def test_attrs(self):
        p = self.TaskPool(2)
        assert p.limit == 2
        assert p._pool is None

    def x_apply(self):
        p = self.TaskPool(2)
        p.start()
        scratchpad = {}
        proc_counter = itertools.count()

        def mycallback(ret_value):
            process = next(proc_counter)
            scratchpad[process] = {}
            scratchpad[process]['ret_value'] = ret_value

        myerrback = mycallback

        res = p.apply_async(do_something, args=[10], callback=mycallback)
        res2 = p.apply_async(raise_something, args=[10], errback=myerrback)
        res3 = p.apply_async(do_something, args=[20], callback=mycallback)

        assert res.get() == 100
        time.sleep(0.5)
        assert scratchpad.get(0)['ret_value'] == 100

        assert isinstance(res2.get(), ExceptionInfo)
        assert scratchpad.get(1)
        time.sleep(1)
        assert isinstance(scratchpad[1]['ret_value'], ExceptionInfo)
        assert scratchpad[1]['ret_value'].exception.args == ('FOO EXCEPTION',)

        assert res3.get() == 400
        time.sleep(0.5)
        assert scratchpad.get(2)['ret_value'] == 400

        res3 = p.apply_async(do_something, args=[30], callback=mycallback)

        assert res3.get() == 900
        time.sleep(0.5)
        assert scratchpad.get(3)['ret_value'] == 900
        p.stop()
