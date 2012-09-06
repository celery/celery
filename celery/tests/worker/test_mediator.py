from __future__ import absolute_import

import sys

from Queue import Queue

from mock import Mock, patch

from celery.worker.mediator import Mediator
from celery.worker.state import revoked as revoked_tasks
from celery.tests.utils import Case


class MockTask(object):
    hostname = 'harness.com'
    id = 1234
    name = 'mocktask'

    def __init__(self, value, **kwargs):
        self.value = value

    on_ack = Mock()

    def revoked(self):
        if self.id in revoked_tasks:
            self.on_ack()
            return True
        return False


class test_Mediator(Case):

    def test_mediator_start__stop(self):
        ready_queue = Queue()
        m = Mediator(ready_queue, lambda t: t)
        m.start()
        self.assertFalse(m._is_shutdown.isSet())
        self.assertFalse(m._is_stopped.isSet())
        m.stop()
        m.join()
        self.assertTrue(m._is_shutdown.isSet())
        self.assertTrue(m._is_stopped.isSet())

    def test_mediator_body(self):
        ready_queue = Queue()
        got = {}

        def mycallback(value):
            got['value'] = value.value

        m = Mediator(ready_queue, mycallback)
        ready_queue.put(MockTask('George Costanza'))

        m.body()

        self.assertEqual(got['value'], 'George Costanza')

        ready_queue.put(MockTask('Jerry Seinfeld'))
        m._does_debug = False
        m.body()
        self.assertEqual(got['value'], 'Jerry Seinfeld')

    @patch('os._exit')
    def test_mediator_crash(self, _exit):
        ms = [None]

        class _Mediator(Mediator):

            def body(self):
                try:
                    raise KeyError('foo')
                finally:
                    ms[0]._is_shutdown.set()

        ready_queue = Queue()
        ms[0] = m = _Mediator(ready_queue, None)
        ready_queue.put(MockTask('George Constanza'))

        stderr = Mock()
        p, sys.stderr = sys.stderr, stderr
        try:
            m.run()
        finally:
            sys.stderr = p
        self.assertTrue(_exit.call_count)
        self.assertTrue(stderr.write.call_count)

    def test_mediator_body_exception(self):
        ready_queue = Queue()

        def mycallback(value):
            raise KeyError('foo')

        m = Mediator(ready_queue, mycallback)
        ready_queue.put(MockTask('Elaine M. Benes'))

        m.body()

    def test_run(self):
        ready_queue = Queue()

        condition = [None]

        def mycallback(value):
            condition[0].set()

        m = Mediator(ready_queue, mycallback)
        condition[0] = m._is_shutdown
        ready_queue.put(MockTask('Elaine M. Benes'))

        m.run()
        self.assertTrue(m._is_shutdown.isSet())
        self.assertTrue(m._is_stopped.isSet())
