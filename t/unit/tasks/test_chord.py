from __future__ import absolute_import, unicode_literals

from contextlib import contextmanager

import pytest
from case import Mock

from celery import canvas, group, result, uuid
from celery.exceptions import ChordError, Retry
from celery.five import range
from celery.result import AsyncResult, EagerResult, GroupResult


def passthru(x):
    return x


class ChordCase:

    def setup(self):

        @self.app.task(shared=False)
        def add(x, y):
            return x + y
        self.add = add


class TSR(GroupResult):
    is_ready = True
    value = None

    def ready(self):
        return self.is_ready

    def join(self, propagate=True, **kwargs):
        if propagate:
            for value in self.value:
                if isinstance(value, Exception):
                    raise value
        return self.value
    join_native = join

    def _failed_join_report(self):
        for value in self.value:
            if isinstance(value, Exception):
                yield EagerResult('some_id', value, 'FAILURE')


class TSRNoReport(TSR):

    def _failed_join_report(self):
        return iter([])


@contextmanager
def patch_unlock_retry(app):
    unlock = app.tasks['celery.chord_unlock']
    retry = Mock()
    retry.return_value = Retry()
    prev, unlock.retry = unlock.retry, retry
    try:
        yield unlock, retry
    finally:
        unlock.retry = prev


class test_unlock_chord_task(ChordCase):

    def test_unlock_ready(self):

        class AlwaysReady(TSR):
            is_ready = True
            value = [2, 4, 8, 6]

        with self._chord_context(AlwaysReady) as (cb, retry, _):
            cb.type.apply_async.assert_called_with(
                ([2, 4, 8, 6],), {}, task_id=cb.id,
            )
            # didn't retry
            assert not retry.call_count

    def test_deps_ready_fails(self):
        GroupResult = Mock(name='GroupResult')
        GroupResult.return_value.ready.side_effect = KeyError('foo')
        unlock_chord = self.app.tasks['celery.chord_unlock']

        with pytest.raises(KeyError):
            unlock_chord('groupid', Mock(), result=[Mock()],
                         GroupResult=GroupResult, result_from_tuple=Mock())

    def test_callback_fails(self):

        class AlwaysReady(TSR):
            is_ready = True
            value = [2, 4, 8, 6]

        def setup(callback):
            callback.apply_async.side_effect = IOError()

        with self._chord_context(AlwaysReady, setup) as (cb, retry, fail):
            fail.assert_called()
            assert fail.call_args[0][0] == cb.id
            assert isinstance(fail.call_args[1]['exc'], ChordError)

    def test_unlock_ready_failed(self):

        class Failed(TSR):
            is_ready = True
            value = [2, KeyError('foo'), 8, 6]

        with self._chord_context(Failed) as (cb, retry, fail_current):
            cb.type.apply_async.assert_not_called()
            # didn't retry
            assert not retry.call_count
            fail_current.assert_called()
            assert fail_current.call_args[0][0] == cb.id
            assert isinstance(fail_current.call_args[1]['exc'], ChordError)
            assert 'some_id' in str(fail_current.call_args[1]['exc'])

    def test_unlock_ready_failed_no_culprit(self):
        class Failed(TSRNoReport):
            is_ready = True
            value = [2, KeyError('foo'), 8, 6]

        with self._chord_context(Failed) as (cb, retry, fail_current):
            fail_current.assert_called()
            assert fail_current.call_args[0][0] == cb.id
            assert isinstance(fail_current.call_args[1]['exc'], ChordError)

    @contextmanager
    def _chord_context(self, ResultCls, setup=None, **kwargs):
        @self.app.task(shared=False)
        def callback(*args, **kwargs):
            pass
        self.app.finalize()

        pts, result.GroupResult = result.GroupResult, ResultCls
        callback.apply_async = Mock()
        callback_s = callback.s()
        callback_s.id = 'callback_id'
        fail_current = self.app.backend.fail_from_current_stack = Mock()
        try:
            with patch_unlock_retry(self.app) as (unlock, retry):
                signature, canvas.maybe_signature = (
                    canvas.maybe_signature, passthru,
                )
                if setup:
                    setup(callback)
                try:
                    assert self.app.tasks['celery.chord_unlock'] is unlock
                    try:
                        unlock(
                            'group_id', callback_s,
                            result=[
                                self.app.AsyncResult(r) for r in ['1', 2, 3]
                            ],
                            GroupResult=ResultCls, **kwargs
                        )
                    except Retry:
                        pass
                finally:
                    canvas.maybe_signature = signature
                yield callback_s, retry, fail_current
        finally:
            result.GroupResult = pts

    def test_when_not_ready(self):
        class NeverReady(TSR):
            is_ready = False

        with self._chord_context(NeverReady, interval=10, max_retries=30) \
                as (cb, retry, _):
            cb.type.apply_async.assert_not_called()
            # did retry
            retry.assert_called_with(countdown=10, max_retries=30)

    def test_is_in_registry(self):
        assert 'celery.chord_unlock' in self.app.tasks


class test_chord(ChordCase):

    def test_eager(self):
        from celery import chord

        @self.app.task(shared=False)
        def addX(x, y):
            return x + y

        @self.app.task(shared=False)
        def sumX(n):
            return sum(n)

        self.app.conf.task_always_eager = True
        x = chord(addX.s(i, i) for i in range(10))
        body = sumX.s()
        result = x(body)
        assert result.get() == sum(i + i for i in range(10))

    def test_apply(self):
        self.app.conf.task_always_eager = False
        from celery import chord

        m = Mock()
        m.app.conf.task_always_eager = False
        m.AsyncResult = AsyncResult
        prev, chord.run = chord.run, m
        try:
            x = chord(self.add.s(i, i) for i in range(10))
            body = self.add.s(2)
            result = x(body)
            assert result.id
            # does not modify original signature
            with pytest.raises(KeyError):
                body.options['task_id']
            chord.run.assert_called()
        finally:
            chord.run = prev


class test_add_to_chord:

    def setup(self):

        @self.app.task(shared=False)
        def add(x, y):
            return x + y
        self.add = add

        @self.app.task(shared=False, bind=True)
        def adds(self, sig, lazy=False):
            return self.add_to_chord(sig, lazy)
        self.adds = adds

    def test_add_to_chord(self):
        self.app.backend = Mock(name='backend')

        sig = self.add.s(2, 2)
        sig.delay = Mock(name='sig.delay')
        self.adds.request.group = uuid()
        self.adds.request.id = uuid()

        with pytest.raises(ValueError):
            # task not part of chord
            self.adds.run(sig)
        self.adds.request.chord = self.add.s()

        res1 = self.adds.run(sig, True)
        assert res1 == sig
        assert sig.options['task_id']
        assert sig.options['group_id'] == self.adds.request.group
        assert sig.options['chord'] == self.adds.request.chord
        sig.delay.assert_not_called()
        self.app.backend.add_to_chord.assert_called_with(
            self.adds.request.group, sig.freeze(),
        )

        self.app.backend.reset_mock()
        sig2 = self.add.s(4, 4)
        sig2.delay = Mock(name='sig2.delay')
        res2 = self.adds.run(sig2)
        assert res2 == sig2.delay.return_value
        assert sig2.options['task_id']
        assert sig2.options['group_id'] == self.adds.request.group
        assert sig2.options['chord'] == self.adds.request.chord
        sig2.delay.assert_called_with()
        self.app.backend.add_to_chord.assert_called_with(
            self.adds.request.group, sig2.freeze(),
        )


class test_Chord_task(ChordCase):

    def test_run(self):
        self.app.backend = Mock()
        self.app.backend.cleanup = Mock()
        self.app.backend.cleanup.__name__ = 'cleanup'
        Chord = self.app.tasks['celery.chord']

        body = self.add.signature()
        Chord(group(self.add.signature((i, i)) for i in range(5)), body)
        Chord([self.add.signature((j, j)) for j in range(5)], body)
        assert self.app.backend.apply_chord.call_count == 2
