from collections import defaultdict
from contextlib import contextmanager

import pytest
from case import ANY, Mock, patch
from kombu.utils.limits import TokenBucket

from celery import Task, signals
from celery.exceptions import InvalidTaskError
from celery.utils.time import rate
from celery.worker import state
from celery.worker.request import Request
from celery.worker.strategy import default as default_strategy
from celery.worker.strategy import hybrid_to_proto2, proto1_to_proto2


class test_proto1_to_proto2:

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
        body, _, _, _ = proto1_to_proto2(self.message, self.body)
        assert body[:2] == ((), {'foo': 'baz'})

    def test_message_without_kwargs(self):
        self.body.pop('kwargs')
        body, _, _, _ = proto1_to_proto2(self.message, self.body)
        assert body[:2] == ((1,), {})

    def test_message_kwargs_not_mapping(self):
        self.body['kwargs'] = (2,)
        with pytest.raises(InvalidTaskError):
            proto1_to_proto2(self.message, self.body)

    def test_message_no_taskset_id(self):
        self.body.pop('taskset')
        assert proto1_to_proto2(self.message, self.body)

    def test_message(self):
        body, headers, decoded, utc = proto1_to_proto2(self.message, self.body)
        assert body == ((1,), {'foo': 'baz'}, {
            'callbacks': None, 'errbacks': None, 'chord': None, 'chain': None,
        })
        assert headers == dict(self.body, group='123')
        assert decoded
        assert not utc


class test_default_strategy_proto2:

    def setup(self):
        @self.app.task(shared=False)
        def add(x, y):
            return x + y

        self.add = add

    def get_message_class(self):
        return self.TaskMessage

    def prepare_message(self, message):
        return message

    class Context:

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

        def was_limited_with_eta(self):
            assert not self.was_reserved()
            called = self.consumer.timer.call_at.called
            if called:
                assert self.consumer.timer.call_at.call_args[0][1] == \
                    self.consumer._limit_post_eta
            return called

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
        assert sig.type.Strategy
        assert sig.type.Request

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
        assert s

        message = self.task_message_from_sig(
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
            assert C.was_reserved()
            req = C.get_request()
            C.consumer.on_task_request.assert_called_with(req)
            assert C.event_sent()

    def test_callbacks(self):
        with self._context(self.add.s(2, 2)) as C:
            callbacks = [Mock(name='cb1'), Mock(name='cb2')]
            C(callbacks=callbacks)
            req = C.get_request()
            for callback in callbacks:
                callback.assert_called_with(req)

    def test_signal_task_received(self):
        callback = Mock()
        with self._context(self.add.s(2, 2)) as C:
            signals.task_received.connect(callback)
            C()
            callback.assert_called_once_with(sender=C.consumer,
                                             request=ANY,
                                             signal=signals.task_received)

    def test_when_events_disabled(self):
        with self._context(self.add.s(2, 2), events=False) as C:
            C()
            assert C.was_reserved()
            assert not C.event_sent()

    def test_eta_task(self):
        with self._context(self.add.s(2, 2).set(countdown=10)) as C:
            C()
            assert C.was_scheduled()
            C.consumer.qos.increment_eventually.assert_called_with()

    def test_eta_task_utc_disabled(self):
        with self._context(self.add.s(2, 2).set(countdown=10), utc=False) as C:
            C()
            assert C.was_scheduled()
            C.consumer.qos.increment_eventually.assert_called_with()

    def test_when_rate_limited(self):
        task = self.add.s(2, 2)
        with self._context(task, rate_limits=True, limit='1/m') as C:
            C()
            assert C.was_rate_limited()

    def test_when_rate_limited_with_eta(self):
        task = self.add.s(2, 2).set(countdown=10)
        with self._context(task, rate_limits=True, limit='1/m') as C:
            C()
            assert C.was_limited_with_eta()
            C.consumer.qos.increment_eventually.assert_called_with()

    def test_when_rate_limited__limits_disabled(self):
        task = self.add.s(2, 2)
        with self._context(task, rate_limits=False, limit='1/m') as C:
            C()
            assert C.was_reserved()

    def test_when_revoked(self):
        task = self.add.s(2, 2)
        task.freeze()
        try:
            with self._context(task) as C:
                C.consumer.controller.state.revoked.add(task.id)
                state.revoked.add(task.id)
                C()
                with pytest.raises(ValueError):
                    C.get_request()
        finally:
            state.revoked.discard(task.id)


class test_default_strategy_proto1(test_default_strategy_proto2):

    def get_message_class(self):
        return self.TaskMessage1


class test_default_strategy_proto1__no_utc(test_default_strategy_proto2):

    def get_message_class(self):
        return self.TaskMessage1

    def prepare_message(self, message):
        message.payload['utc'] = False
        return message


class test_custom_request_for_default_strategy(test_default_strategy_proto2):
    def test_custom_request_gets_instantiated(self):
        _MyRequest = Mock(name='MyRequest')

        class MyRequest(Request):
            def __init__(self, *args, **kwargs):
                Request.__init__(self, *args, **kwargs)
                _MyRequest()

        class MyTask(Task):
            Request = MyRequest

        @self.app.task(base=MyTask)
        def failed():
            raise AssertionError

        sig = failed.s()
        with self._context(sig) as C:
            task_message_handler = default_strategy(
                failed,
                self.app,
                C.consumer
            )
            task_message_handler(C.message, None, None, None, None)
            _MyRequest.assert_called()


class test_hybrid_to_proto2:

    def setup(self):
        self.message = Mock(name='message')
        self.body = {
            'args': (1,),
            'kwargs': {'foo': 'baz'},
            'utc': False,
            'taskset': '123',
        }

    def test_retries_default_value(self):
        _, headers, _, _ = hybrid_to_proto2(self.message, self.body)
        assert headers.get('retries') == 0

    def test_retries_custom_value(self):
        _custom_value = 3
        self.body['retries'] = _custom_value
        _, headers, _, _ = hybrid_to_proto2(self.message, self.body)
        assert headers.get('retries') == _custom_value
