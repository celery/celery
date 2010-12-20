from itertools import count

from celery.concurrency.base import apply_target, BasePool
from celery.tests.utils import unittest


class test_BasePool(unittest.TestCase):

    def test_apply_target(self):

        scratch = {}
        counter = count(0).next

        def gen_callback(name, retval=None):

            def callback(*args):
                scratch[name] = (counter(), args)
                return retval

            return callback

        res = apply_target(gen_callback("target", 42),
                           args=(8, 16),
                           callback=gen_callback("callback"),
                           accept_callback=gen_callback("accept_callback"))

        self.assertDictEqual(scratch,
                             {"accept_callback": (0, ()),
                              "target": (1, (8, 16)),
                              "callback": (2, (42, ))})

        # No accept callback
        scratch.clear()
        res2 = apply_target(gen_callback("target", 42),
                            args=(8, 16),
                            callback=gen_callback("callback"),
                            accept_callback=None)
        self.assertDictEqual(scratch,
                              {"target": (3, (8, 16)),
                               "callback": (4, (42, ))})

    def test_interface_on_start(self):
        BasePool(10).on_start()

    def test_interface_on_stop(self):
        BasePool(10).on_stop()

    def test_interface_on_apply(self):
        BasePool(10).on_apply()

    def test_interface_info(self):
        self.assertDictEqual(BasePool(10).info, {})

    def test_active(self):
        p = BasePool(10)
        self.assertFalse(p.active)
        p._state = p.RUN
        self.assertTrue(p.active)

