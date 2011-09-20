from __future__ import absolute_import

from mock import patch

from celery import current_app
from celery.task import chords
from celery.task import TaskSet
from celery.tests.utils import AppCase, Mock

passthru = lambda x: x


@current_app.task
def add(x, y):
    return x + y


class test_unlock_chord_task(AppCase):

    @patch("celery.task.chords.TaskSetResult")
    @patch("celery.task.chords._unlock_chord.retry")
    def test_unlock_ready(self, retry, TaskSetResult):
        callback = Mock()
        result = Mock(attrs=dict(ready=lambda: True,
                                 join=lambda **kw: [2, 4, 8, 6]))
        TaskSetResult.restore = lambda setid: result
        subtask, chords.subtask = chords.subtask, passthru
        try:
            chords._unlock_chord("setid", callback)
        finally:
            chords.subtask = subtask
        callback.delay.assert_called_with([2, 4, 8, 6])
        result.delete.assert_called_with()
        # did not retry
        self.assertFalse(retry.call_count)

    @patch("celery.task.chords.TaskSetResult")
    @patch("celery.task.chords._unlock_chord.retry")
    def test_when_not_ready(self, retry, TaskSetResult):
        callback = Mock()
        result = Mock(attrs=dict(ready=lambda: False))
        TaskSetResult.restore = lambda setid: result
        chords._unlock_chord("setid", callback, interval=10, max_retries=30)
        self.assertFalse(callback.delay.call_count)
        # did retry
        chords._unlock_chord.retry.assert_called_with(countdown=10,
                                                     max_retries=30)

    def test_is_in_registry(self):
        from celery.registry import tasks
        self.assertIn("celery.chord_unlock", tasks)


class test_chord(AppCase):

    def test_apply(self):

        class chord(chords.chord):
            Chord = Mock()

        x = chord(add.subtask((i, i)) for i in xrange(10))
        body = add.subtask((2, ))
        result = x(body)
        self.assertEqual(result.task_id, body.options["task_id"])
        self.assertTrue(chord.Chord.apply_async.call_count)


class test_Chord_task(AppCase):

    def test_run(self):

        class Chord(chords.Chord):
            backend = Mock()

        body = dict()
        Chord()(TaskSet(add.subtask((i, i)) for i in xrange(5)), body)
        Chord()([add.subtask((i, i)) for i in xrange(5)], body)
        self.assertEqual(Chord.backend.on_chord_apply.call_count, 2)
