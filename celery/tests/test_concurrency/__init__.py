from __future__ import absolute_import
from __future__ import with_statement

import os

from itertools import count

from celery.concurrency.base import apply_target, BasePool
from celery.tests.utils import Case


class test_BasePool(Case):

    def test_apply_target(self):

        scratch = {}
        counter = count(0).next

        def gen_callback(name, retval=None):

            def callback(*args):
                scratch[name] = (counter(), args)
                return retval

            return callback

        apply_target(gen_callback("target", 42),
                     args=(8, 16),
                     callback=gen_callback("callback"),
                     accept_callback=gen_callback("accept_callback"))

        self.assertDictContainsSubset({
                              "target": (1, (8, 16)),
                              "callback": (2, (42, ))}, scratch)
        pa1 = scratch["accept_callback"]
        self.assertEqual(0, pa1[0])
        self.assertEqual(pa1[1][0], os.getpid())
        self.assertTrue(pa1[1][1])

        # No accept callback
        scratch.clear()
        apply_target(gen_callback("target", 42),
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

    def test_restart(self):
        p = BasePool(10)
        with self.assertRaises(NotImplementedError):
            p.restart()
