from __future__ import absolute_import
from __future__ import with_statement

import os
import sys

from nose import SkipTest
from mock import Mock

from celery.concurrency.gevent import (
    Schedule,
    Timer,
    TaskPool,
)

from celery.tests.utils import Case, mock_module, patch_many, skip_if_pypy
gevent_modules = (
    'gevent',
    'gevent.monkey',
    'gevent.greenlet',
    'gevent.pool',
    'greenlet',
)


class GeventCase(Case):

    @skip_if_pypy
    def setUp(self):
        try:
            self.gevent = __import__('gevent')
        except ImportError:
            raise SkipTest(
                'gevent not installed, skipping related tests.')


class test_gevent_patch(GeventCase):

    def test_is_patched(self):
        with mock_module(*gevent_modules):
            monkey_patched = []
            import gevent
            from gevent import monkey
            gevent.version_info = (1, 0, 0)
            prev_monkey_patch = monkey.patch_all
            monkey.patch_all = lambda: monkey_patched.append(True)
            prev_gevent = sys.modules.pop('celery.concurrency.gevent', None)
            os.environ.pop('GEVENT_NOPATCH')
            try:
                import celery.concurrency.gevent  # noqa
                self.assertTrue(monkey_patched)
            finally:
                sys.modules['celery.concurrency.gevent'] = prev_gevent
                os.environ['GEVENT_NOPATCH'] = 'yes'
                monkey.patch_all = prev_monkey_patch


class test_Schedule(Case):

    def test_sched(self):
        with mock_module(*gevent_modules):
            with patch_many('gevent.greenlet',
                            'gevent.greenlet.GreenletExit') as (greenlet,
                                                                GreenletExit):
                greenlet.Greenlet = object
                x = Schedule()
                greenlet.Greenlet = Mock()
                x._Greenlet.spawn_later = Mock()
                x._GreenletExit = KeyError
                entry = Mock()
                g = x._enter(1, 0, entry)
                self.assertTrue(x.queue)

                x._entry_exit(g)
                g.kill.assert_called_with()
                self.assertFalse(x._queue)

                x._queue.add(g)
                x.clear()
                x._queue.add(g)
                g.kill.side_effect = KeyError()
                x.clear()


class test_TasKPool(Case):

    def test_pool(self):
        with mock_module(*gevent_modules):
            with patch_many('gevent.spawn_raw', 'gevent.pool.Pool') as (
                    spawn_raw, Pool):
                x = TaskPool()
                x.on_start()
                x.on_stop()
                x.on_apply(Mock())
                x._pool = None
                x.on_stop()

                x._pool = Mock()
                x._pool._semaphore.counter = 1
                x._pool.size = 1
                x.grow()
                self.assertEqual(x._pool.size, 2)
                self.assertEqual(x._pool._semaphore.counter, 2)
                x.shrink()
                self.assertEqual(x._pool.size, 1)
                self.assertEqual(x._pool._semaphore.counter, 1)

                x._pool = [4, 5, 6]
                self.assertEqual(x.num_processes, 3)


class test_Timer(Case):

    def test_timer(self):
        with mock_module(*gevent_modules):
            x = Timer()
            x.ensure_started()
            x.schedule = Mock()
            x.start()
            x.stop()
            x.schedule.clear.assert_called_with()
