from __future__ import absolute_import, unicode_literals

from collections import defaultdict
from contextlib import contextmanager

from kombu.utils.limits import TokenBucket

from celery.exceptions import InvalidTaskError
from celery.worker import state
from celery.worker.strategy import proto1_to_proto2
from celery.utils.timeutils import rate

from celery.tests.case import (
    AppCase, Mock, TaskMessage, TaskMessage1, patch, task_message_from_sig,
)


class test_proto1_to_proto2(AppCase):

    def setup(self):
        self.message = Mock(name='message')
        self.body = {
            'args': (1,),
            'kwargs': {'foo': 'baz'},
            'utc': False,
            'taskset': '123',
        }

    def test_message_without_args(self):
        self.body.pop('args')
        with self.assertRaises(InvalidTaskError):
            proto1_to_proto2(self.message, self.body)

    def test_message_without_kwargs(self):
        self.body.pop('kwargs')
        with self.assertRaises(InvalidTaskError):
            proto1_to_proto2(self.message, self.body)

    def test_message_kwargs_not_mapping(self):
        self.body['kwargs'] = (2,)
        with self.assertRaises(InvalidTaskError):
            proto1_to_proto2(self.message, self.body)

    def test_message_no_taskset_id(self):
        self.body.pop('taskset')
        self.assertTrue(proto1_to_proto2(self.message, self.body))

    def test_message(self):
        body, headers, decoded, utc = proto1_to_proto2(self.message, self.body)
        self.assertTupleEqual(body, ((1,), {'foo': 'baz'}, {
            'callbacks': None, 'errbacks': None, 'chord': None, 'chain': None,
        }))
        self.assertDictEqual(headers, dict(self.body, group='123'))
        self.assertTrue(decoded)
        self.assertFalse(utc)


class test_default_strategy_proto2(AppCase):

    def setup(self):
        @self.app.task(shared=False)
        def add(x, y):
            return x + y

        self.add = add

    def get_message_class(self):
        return TaskMessage

    def prepare_message(self, message):
        return message

    class Context(object):

        def __init__(self, sig, s, reserved, consumer, message):
            self.sig = sig
            self.s = s
            self.reserved = reserved
            self.consumer = consumer
            self.message = message

        def __call__(self, callbacks=[], **kwargs):
            return self.s(
                self.message,
                (self.message.payload
                    if not self.message.headers.get('id') else None),
                self.message.ack, self.message.reject, callbacks, **kwargs
            )

        def was_reserved(self):
            return self.reserved.called

        def was_rate_limited(self):
            assert not self.was_reserved()
            return self.consumer._limit_task.called

        def was_scheduled(self):
            assert not self.was_reserved()
            assert not self.was_rate_limited()
            return self.consumer.timer.call_at.called

        def event_sent(self):
            return self.consumer.event_dispatcher.send.call_args

        def get_request(self):
            if self.was_reserved():
                return self.reserved.call_args[0][0]
            if self.was_rate_limited():
                return self.consumer._limit_task.call_args[0][0]
            if self.was_scheduled():
                return self.consumer.timer.call_at.call_args[0][0]
            raise ValueError('request not handled')

    @contextmanager
    def _context(self, sig,
                 rate_limits=True, events=True, utc=True, limit=None):
        self.assertTrue(sig.type.Strategy)

        reserved = Mock()
        consumer = Mock()
        consumer.task_buckets = defaultdict(lambda: None)
        if limit:
            bucket = TokenBucket(rate(limit), capacity=1)
            consumer.task_buckets[sig.task] = bucket
        consumer.controller.state.revoked = set()
        consumer.disable_rate_limits = not rate_limits
        consumer.event_dispatcher.enabled = events
        s = sig.type.start_strategy(self.app, consumer, task_reserved=reserved)
        self.assertTrue(s)

        message = task_message_from_sig(
            self.app, sig, utc=utc, TaskMessage=self.get_message_class(),
        )
        message = self.prepare_message(message)
        yield self.Context(sig, s, reserved, consumer, message)

    def test_when_logging_disabled(self):
        with patch('celery.worker.strategy.logger') as logger:
            logger.isEnabledFor.return_value = False
            with self._context(self.add.s(2, 2)) as C:
                C()
                logger.info.assert_not_called()

    def test_task_strategy(self):
        with self._context(self.add.s(2, 2)) as C:
            C()
            self.assertTrue(C.was_reserved())
            req = C.get_request()
            C.consumer.on_task_request.assert_called_with(req)
            self.assertTrue(C.event_sent())

    def test_callbacks(self):
        with self._context(self.add.s(2, 2)) as C:
            callbacks = [Mock(name='cb1'), Mock(name='cb2')]
            C(callbacks=callbacks)
            req = C.get_request()
            for callback in callbacks:
                callback.assert_called_with(req)

    def test_when_events_disabled(self):
        with self._context(self.add.s(2, 2), events=False) as C:
            C()
            self.assertTrue(C.was_reserved())
            self.assertFalse(C.event_sent())

    def test_eta_task(self):
        with self._context(self.add.s(2, 2).set(countdown=10)) as C:
            C()
            self.assertTrue(C.was_scheduled())
            C.consumer.qos.increment_eventually.assert_called_with()

    def test_eta_task_utc_disabled(self):
        with self._context(self.add.s(2, 2).set(countdown=10), utc=False) as C:
            C()
            self.assertTrue(C.was_scheduled())
            C.consumer.qos.increment_eventually.assert_called_with()

    def test_when_rate_limited(self):
        task = self.add.s(2, 2)
        with self._context(task, rate_limits=True, limit='1/m') as C:
            C()
            self.assertTrue(C.was_rate_limited())

    def test_when_rate_limited__limits_disabled(self):
        task = self.add.s(2, 2)
        with self._context(task, rate_limits=False, limit='1/m') as C:
            C()
            self.assertTrue(C.was_reserved())

    def test_when_revoked(self):
        task = self.add.s(2, 2)
        task.freeze()
        try:
            with self._context(task) as C:
                C.consumer.controller.state.revoked.add(task.id)
                state.revoked.add(task.id)
                C()
                with self.assertRaises(ValueError):
                    C.get_request()
        finally:
            state.revoked.discard(task.id)


class test_default_strategy_proto1(test_default_strategy_proto2):

    def get_message_class(self):
        return TaskMessage1


class test_default_strategy_proto1__no_utc(test_default_strategy_proto2):

    def get_message_class(self):
        return TaskMessage1

    def prepare_message(self, message):
        message.payload['utc'] = False
        return message
