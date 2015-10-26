from __future__ import absolute_import

# some of these are tested in test_worker, so I've only written tests
# here to complete coverage.  Should move everyting to this module at some
# point [-ask]

from celery.platforms import IS_WINDOWS
from celery.worker.components import Pool

from celery.tests.case import AppCase, Mock, SkipTest


class test_Pool(AppCase):

    def test_close_terminate(self):
        w = Mock()
        comp = Pool(w)
        pool = w.pool = Mock()
        comp.close(w)
        pool.close.assert_called_with()
        comp.terminate(w)
        pool.terminate.assert_called_with()

        w.pool = None
        comp.close(w)
        comp.terminate(w)

    def test_create_when_eventloop(self):
        if IS_WINDOWS:
            raise SkipTest('Win32')
        w = Mock()
        w.use_eventloop = w.pool_putlocks = w.pool_cls.uses_semaphore = True
        comp = Pool(w)
        w.pool = Mock()
        comp.create(w)
        self.assertIs(w.process_task, w._process_task_sem)

    def test_create_calls_instantiate_with_max_memory(self):
        w = Mock()
        w.use_eventloop = w.pool_putlocks = w.pool_cls.uses_semaphore = True
        comp = Pool(w)
        comp.instantiate = Mock()
        w.max_memory_per_child = 32

        comp.create(w)

        self.assertEqual(
            comp.instantiate.call_args[1]['max_memory_per_child'], 32)
