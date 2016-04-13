from __future__ import absolute_import, unicode_literals

from celery.concurrency.gevent import (
    Timer,
    TaskPool,
    apply_timeout,
)

from celery.tests.case import AppCase, Mock, patch, skip

gevent_modules = (
    'gevent',
    'gevent.monkey',
    'gevent.greenlet',
    'gevent.pool',
    'greenlet',
)


@skip.if_pypy()
class GeventCase(AppCase):

    def setup(self):
        self.mock_modules(*gevent_modules)


class test_gevent_patch(GeventCase):

    def test_is_patched(self):
        with patch('gevent.monkey.patch_all', create=True) as patch_all:
            import gevent
            gevent.version_info = (1, 0, 0)
            from celery import maybe_patch_concurrency
            maybe_patch_concurrency(['x', '-P', 'gevent'])
            patch_all.assert_called()


class test_Timer(GeventCase):

    def setup(self):
        GeventCase.setup(self)
        self.greenlet = self.patch('gevent.greenlet')
        self.GreenletExit = self.patch('gevent.greenlet.GreenletExit')

    def test_sched(self):
        self.greenlet.Greenlet = object
        x = Timer()
        self.greenlet.Greenlet = Mock()
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

        g = x._Greenlet()
        g.cancel()


class test_TaskPool(GeventCase):

    def setup(self):
        GeventCase.setup(self)
        self.spawn_raw = self.patch('gevent.spawn_raw')
        self.Pool = self.patch('gevent.pool.Pool')

    def test_pool(self):
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


class test_apply_timeout(AppCase):

    def test_apply_timeout(self):

            class Timeout(Exception):
                value = None

                def __init__(self, value):
                    self.__class__.value = value

                def __enter__(self):
                    return self

                def __exit__(self, *exc_info):
                    pass
            timeout_callback = Mock(name='timeout_callback')
            apply_target = Mock(name='apply_target')
            apply_timeout(
                Mock(), timeout=10, callback=Mock(name='callback'),
                timeout_callback=timeout_callback,
                apply_target=apply_target, Timeout=Timeout,
            )
            self.assertEqual(Timeout.value, 10)
            apply_target.assert_called()

            apply_target.side_effect = Timeout(10)
            apply_timeout(
                Mock(), timeout=10, callback=Mock(),
                timeout_callback=timeout_callback,
                apply_target=apply_target, Timeout=Timeout,
            )
            timeout_callback.assert_called_with(False, 10)
