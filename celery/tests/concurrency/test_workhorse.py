from __future__ import absolute_import

import errno
import socket
import time
import os

from itertools import cycle

from celery.five import items, range
from celery.utils.functional import noop
from celery.tests.case import AppCase, Mock, SkipTest, call, patch
from celery.concurrency.workhorse import TaskPool

from kombu.async.semaphore import LaxBoundedSemaphore

class test_Workhorse(AppCase):

    def setup(self):
        if not hasattr(os, 'fork'):
            raise SkipTest('missing os.fork')

    def test_run_stop_pool(self):
        pool = TaskPool(semaphore=LaxBoundedSemaphore(10))
        pool.start()
        pids = []
        accept_callback = lambda pid, ts: pids.append(pid)
        success_callback = Mock()
        error_callback = Mock()
        pool.apply_async(
            lambda x: x,
            (2, ),
            {},
            accept_callback=accept_callback,
            correlation_id='asdf-1234',
            error_callback=error_callback,
            callback=success_callback,
        )
        self.assertTrue(pool.workers)
        self.assertEqual(pids, list(pool.workers))
        pool.stop()
        success_callback.assert_called_with(None)
        self.assertFalse(error_callback.called)

    def test_terminate_pool(self):
        pool = TaskPool(semaphore=LaxBoundedSemaphore(10))
        pool.start()
        pids = []
        accept_callback = lambda pid, ts: pids.append(pid)
        success_callback = Mock()
        error_callback = Mock()
        pool.apply_async(
            lambda x: time.sleep(x),
            (2, ),
            {},
            accept_callback=accept_callback,
            correlation_id='asdf-1234',
            error_callback=error_callback,
            callback=success_callback,
        )
        self.assertTrue(pool.workers)
        self.assertEqual(pids, list(pool.workers))
        pool.terminate()
        self.assertFalse(success_callback.called)
        self.assertTrue(error_callback.called)


    def test_grow_pool(self):
        semaphore = LaxBoundedSemaphore(1)
        pool = TaskPool(semaphore=semaphore)
        pool.start()
        pids = []
        accept_callback = lambda pid, ts: pids.append(pid)
        success_callback = Mock()
        error_callback = Mock()
        for i in range(3):
            pool.apply_async(
                lambda x: time.sleep(x),
                (2, ),
                {},
                accept_callback=accept_callback,
                correlation_id='asdf-1234-%s' % i,
                error_callback=error_callback,
                callback=success_callback,
            )
        self.assertTrue(pool.workers)
        self.assertEqual(pids, list(pool.workers))

        #self.assertEqual(semaphore.value, 0)
        pool.terminate()
        # TODO TODO TODO TODO TODO TODO
        #pool.grow()
        #self.assertFalse(success_callback.called)
        #self.assertTrue(error_callback.called)

    #def test_restart
    #
    #def test_timeout
    #
    #def test_
