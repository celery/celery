from __future__ import absolute_import, unicode_literals

from celery.concurrency.threads import NullDict, TaskPool, apply_target

from celery.tests.case import AppCase, Case, Mock, mock


class test_NullDict(Case):

    def test_setitem(self):
        x = NullDict()
        x['foo'] = 1
        with self.assertRaises(KeyError):
            x['foo']


class test_TaskPool(AppCase):

    def test_without_threadpool(self):

        with mock.mask_modules('threadpool'):
            with self.assertRaises(ImportError):
                TaskPool(app=self.app)

    def test_with_threadpool(self):
        with mock.module('threadpool'):
            x = TaskPool(app=self.app)
            self.assertTrue(x.ThreadPool)
            self.assertTrue(x.WorkRequest)

    def test_on_start(self):
        with mock.module('threadpool'):
            x = TaskPool(app=self.app)
            x.on_start()
            self.assertTrue(x._pool)
            self.assertIsInstance(x._pool.workRequests, NullDict)

    def test_on_stop(self):
        with mock.module('threadpool'):
            x = TaskPool(app=self.app)
            x.on_start()
            x.on_stop()
            x._pool.dismissWorkers.assert_called_with(x.limit, do_join=True)

    def test_on_apply(self):
        with mock.module('threadpool'):
            x = TaskPool(app=self.app)
            x.on_start()
            callback = Mock()
            accept_callback = Mock()
            target = Mock()
            req = x.on_apply(target, args=(1, 2), kwargs={'a': 10},
                             callback=callback,
                             accept_callback=accept_callback)
            x.WorkRequest.assert_called_with(
                apply_target,
                (target, (1, 2), {'a': 10}, callback, accept_callback),
            )
            x._pool.putRequest.assert_called_with(req)
            x._pool._results_queue.queue.clear.assert_called_with()
