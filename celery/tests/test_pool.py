import unittest
import logging
import itertools
import time
from celery.worker.pool import TaskPool
from celery.datastructures import ExceptionInfo
import sys


def do_something(i):
    return i * i


def long_something():
    time.sleep(1)


def raise_something(i):
    try:
        raise KeyError("FOO EXCEPTION")
    except KeyError:
        return ExceptionInfo(sys.exc_info())


class TestTaskPool(unittest.TestCase):

    def test_attrs(self):
        p = TaskPool(limit=2)
        self.assertEqual(p.limit, 2)
        self.assertTrue(isinstance(p.logger, logging.Logger))
        self.assertTrue(p._pool is None)

    def x_start_stop(self):
        p = TaskPool(limit=2)
        p.start()
        self.assertTrue(p._pool)
        p.stop()
        self.assertTrue(p._pool is None)

    def x_apply(self):
        p = TaskPool(limit=2)
        p.start()
        scratchpad = {}
        proc_counter = itertools.count().next

        def mycallback(ret_value, meta):
            process = proc_counter()
            scratchpad[process] = {}
            scratchpad[process]["ret_value"] = ret_value
            scratchpad[process]["meta"] = meta

        myerrback = mycallback

        res = p.apply_async(do_something, args=[10], callbacks=[mycallback],
                            meta={"foo": "bar"})
        res2 = p.apply_async(raise_something, args=[10], errbacks=[myerrback],
                            meta={"foo2": "bar2"})
        res3 = p.apply_async(do_something, args=[20], callbacks=[mycallback],
                            meta={"foo3": "bar3"})

        self.assertEqual(res.get(), 100)
        time.sleep(0.5)
        self.assertTrue(scratchpad.get(0))
        self.assertEqual(scratchpad[0]["ret_value"], 100)
        self.assertEqual(scratchpad[0]["meta"], {"foo": "bar"})

        self.assertTrue(isinstance(res2.get(), ExceptionInfo))
        self.assertTrue(scratchpad.get(1))
        time.sleep(1)
        #self.assertEqual(scratchpad[1]["ret_value"], "FOO")
        self.assertTrue(isinstance(scratchpad[1]["ret_value"],
                          ExceptionInfo))
        self.assertEqual(scratchpad[1]["ret_value"].exception.args,
                          ("FOO EXCEPTION", ))
        self.assertEqual(scratchpad[1]["meta"], {"foo2": "bar2"})

        self.assertEqual(res3.get(), 400)
        time.sleep(0.5)
        self.assertTrue(scratchpad.get(2))
        self.assertEqual(scratchpad[2]["ret_value"], 400)
        self.assertEqual(scratchpad[2]["meta"], {"foo3": "bar3"})

        res3 = p.apply_async(do_something, args=[30], callbacks=[mycallback],
                            meta={"foo4": "bar4"})

        self.assertEqual(res3.get(), 900)
        time.sleep(0.5)
        self.assertTrue(scratchpad.get(3))
        self.assertEqual(scratchpad[3]["ret_value"], 900)
        self.assertEqual(scratchpad[3]["meta"], {"foo4": "bar4"})

        p.stop()
