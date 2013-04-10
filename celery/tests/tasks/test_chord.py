from __future__ import absolute_import
from __future__ import with_statement

from mock import patch
from contextlib import contextmanager

from celery import canvas
from celery import current_app
from celery import result
from celery.result import AsyncResult, GroupResult
from celery.task import task, TaskSet
from celery.tests.utils import AppCase, Mock

passthru = lambda x: x


@current_app.task
def add(x, y):
    return x + y


@current_app.task
def callback(r):
    return r


class TSR(GroupResult):
    is_ready = True
    value = None

    def ready(self):
        return self.is_ready

    def join(self, **kwargs):
        return self.value

    def join_native(self, **kwargs):
        return self.value


@contextmanager
def patch_unlock_retry():
    unlock = current_app.tasks['celery.chord_unlock']
    retry = Mock()
    prev, unlock.retry = unlock.retry, retry
    try:
        yield unlock, retry
    finally:
        unlock.retry = prev


class test_unlock_chord_task(AppCase):

    @patch('celery.result.GroupResult')
    def test_unlock_ready(self, GroupResult):

        class AlwaysReady(TSR):
            is_ready = True
            value = [2, 4, 8, 6]

        @task()
        def callback(*args, **kwargs):
            pass

        pts, result.GroupResult = result.GroupResult, AlwaysReady
        callback.apply_async = Mock()
        callback_s = callback.s()
        try:
            with patch_unlock_retry() as (unlock, retry):
                subtask, canvas.maybe_subtask = canvas.maybe_subtask, passthru
                try:
                    unlock('group_id', callback_s,
                           result=map(AsyncResult, ['1', '2', '3']),
                           GroupResult=AlwaysReady)
                finally:
                    canvas.maybe_subtask = subtask
                callback.apply_async.assert_called_with(([2, 4, 8, 6], ), {})
                # did not retry
                self.assertFalse(retry.call_count)
        finally:
            result.GroupResult = pts

    @patch('celery.result.GroupResult')
    def test_when_not_ready(self, GroupResult):
        with patch_unlock_retry() as (unlock, retry):

            class NeverReady(TSR):
                is_ready = False

            pts, result.GroupResult = result.GroupResult, NeverReady
            try:
                callback = Mock()
                unlock('group_id', callback, interval=10, max_retries=30,
                       result=map(AsyncResult, [1, 2, 3]),
                       GroupResult=NeverReady)
                self.assertFalse(callback.delay.call_count)
                # did retry
                unlock.retry.assert_called_with(countdown=10, max_retries=30)
            finally:
                result.GroupResult = pts

    def test_is_in_registry(self):
        self.assertIn('celery.chord_unlock', current_app.tasks)


class test_chord(AppCase):

    def test_eager(self):
        from celery import chord

        @task()
        def addX(x, y):
            return x + y

        @task()
        def sumX(n):
            return sum(n)

        self.app.conf.CELERY_ALWAYS_EAGER = True
        try:
            x = chord(addX.s(i, i) for i in xrange(10))
            body = sumX.s()
            result = x(body)
            self.assertEqual(result.get(), sum(i + i for i in xrange(10)))
        finally:
            self.app.conf.CELERY_ALWAYS_EAGER = False

    def test_apply(self):
        self.app.conf.CELERY_ALWAYS_EAGER = False
        from celery import chord

        m = Mock()
        m.app.conf.CELERY_ALWAYS_EAGER = False
        m.AsyncResult = AsyncResult
        prev, chord._type = chord._type, m
        try:
            x = chord(add.s(i, i) for i in xrange(10))
            body = add.s(2)
            result = x(body)
            self.assertTrue(result.id)
            # does not modify original subtask
            with self.assertRaises(KeyError):
                body.options['task_id']
            self.assertTrue(chord._type.called)
        finally:
            chord._type = prev


class test_Chord_task(AppCase):

    def test_run(self):
        prev, current_app.backend = current_app.backend, Mock()
        current_app.backend.cleanup = Mock()
        current_app.backend.cleanup.__name__ = 'cleanup'
        try:
            Chord = current_app.tasks['celery.chord']

            body = dict()
            Chord(TaskSet(add.subtask((i, i)) for i in xrange(5)), body)
            Chord([add.subtask((j, j)) for j in xrange(5)], body)
            self.assertEqual(current_app.backend.on_chord_apply.call_count, 2)
        finally:
            current_app.backend = prev
