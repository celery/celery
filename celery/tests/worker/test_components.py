from __future__ import absolute_import, unicode_literals

# some of these are tested in test_worker, so I've only written tests
# here to complete coverage.  Should move everyting to this module at some
# point [-ask]

from celery.exceptions import ImproperlyConfigured
from celery.worker.components import Beat, Hub, Pool, Timer

from celery.tests.case import AppCase, Mock, patch, skip


class test_Timer(AppCase):

    def test_create__eventloop(self):
        w = Mock(name='w')
        w.use_eventloop = True
        Timer(w).create(w)
        self.assertFalse(w.timer.queue)


class test_Hub(AppCase):

    def setup(self):
        self.w = Mock(name='w')
        self.hub = Hub(self.w)
        self.w.hub = Mock(name='w.hub')

    @patch('celery.worker.components.set_event_loop')
    @patch('celery.worker.components.get_event_loop')
    def test_create(self, get_event_loop, set_event_loop):
        self.hub._patch_thread_primitives = Mock(name='ptp')
        self.assertIs(self.hub.create(self.w), self.hub)
        self.hub._patch_thread_primitives.assert_called_with(self.w)

    def test_start(self):
        self.hub.start(self.w)

    def test_stop(self):
        self.hub.stop(self.w)
        self.w.hub.close.assert_called_with()

    def test_terminate(self):
        self.hub.terminate(self.w)
        self.w.hub.close.assert_called_with()


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

    @skip.if_win32()
    def test_create_when_eventloop(self):
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


class test_Beat(AppCase):

    def test_create__green(self):
        w = Mock(name='w')
        w.pool_cls.__module__ = 'foo_gevent'
        with self.assertRaises(ImproperlyConfigured):
            Beat(w).create(w)
