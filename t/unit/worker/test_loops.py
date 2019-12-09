from __future__ import absolute_import, unicode_literals

import errno
import socket

import pytest
from case import Mock
from kombu.asynchronous import ERR, READ, WRITE, Hub
from kombu.exceptions import DecodeError

from celery.bootsteps import CLOSE, RUN
from celery.exceptions import (InvalidTaskError, WorkerLostError,
                               WorkerShutdown, WorkerTerminate)
from celery.five import Empty, python_2_unicode_compatible
from celery.platforms import EX_FAILURE, EX_OK
from celery.worker import state
from celery.worker.consumer import Consumer
from celery.worker.loops import _quick_drain, asynloop, synloop


@python_2_unicode_compatible
class PromiseEqual(object):

    def __init__(self, fun, *args, **kwargs):
        self.fun = fun
        self.args = args
        self.kwargs = kwargs

    def __eq__(self, other):
        return (other.fun == self.fun and
                other.args == self.args and
                other.kwargs == self.kwargs)

    def __repr__(self):
        return '<promise: {0.fun!r} {0.args!r} {0.kwargs!r}>'.format(self)


class X(object):

    def __init__(self, app, heartbeat=None, on_task_message=None,
                 transport_driver_type=None):
        hub = Hub()
        (
            self.obj,
            self.connection,
            self.consumer,
            self.blueprint,
            self.hub,
            self.qos,
            self.heartbeat,
            self.clock,
        ) = self.args = [Mock(name='obj'),
                         Mock(name='connection'),
                         Mock(name='consumer'),
                         Mock(name='blueprint'),
                         hub,
                         Mock(name='qos'),
                         heartbeat,
                         Mock(name='clock')]
        self.connection.supports_heartbeats = True
        self.connection.get_heartbeat_interval.side_effect = (
            lambda: self.heartbeat
        )
        self.consumer.callbacks = []
        self.obj.strategies = {}
        self.connection.connection_errors = (socket.error,)
        if transport_driver_type:
            self.connection.transport.driver_type = transport_driver_type
        self.hub.readers = {}
        self.hub.timer = Mock(name='hub.timer')
        self.hub.timer._queue = [Mock()]
        self.hub.fire_timers = Mock(name='hub.fire_timers')
        self.hub.fire_timers.return_value = 1.7
        self.hub.poller = Mock(name='hub.poller')
        self.hub.close = Mock(name='hub.close()')  # asynloop calls hub.close
        self.Hub = self.hub
        self.blueprint.state = RUN
        # need this for create_task_handler
        self._consumer = _consumer = Consumer(
            Mock(), timer=Mock(), controller=Mock(), app=app)
        _consumer.on_task_message = on_task_message or []
        self.obj.create_task_handler = _consumer.create_task_handler
        self.on_unknown_message = self.obj.on_unknown_message = Mock(
            name='on_unknown_message',
        )
        _consumer.on_unknown_message = self.on_unknown_message
        self.on_unknown_task = self.obj.on_unknown_task = Mock(
            name='on_unknown_task',
        )
        _consumer.on_unknown_task = self.on_unknown_task
        self.on_invalid_task = self.obj.on_invalid_task = Mock(
            name='on_invalid_task',
        )
        _consumer.on_invalid_task = self.on_invalid_task
        self.on_decode_error = self.obj.on_decode_error = Mock(
            name='on_decode_error',
        )
        _consumer.on_decode_error = self.on_decode_error
        _consumer.strategies = self.obj.strategies

    def timeout_then_error(self, mock):

        def first(*args, **kwargs):
            mock.side_effect = socket.error()
            raise socket.timeout()
        mock.side_effect = first

    def close_then_error(self, mock=None, mod=0, exc=None):
        mock = Mock() if mock is None else mock

        def first(*args, **kwargs):
            if not mod or mock.call_count > mod:
                self.close()
                raise (socket.error() if exc is None else exc)
        mock.side_effect = first
        return mock

    def close(self, *args, **kwargs):
        self.blueprint.state = CLOSE

    def closer(self, mock=None, mod=0):
        mock = Mock() if mock is None else mock

        def closing(*args, **kwargs):
            if not mod or mock.call_count >= mod:
                self.close()
        mock.side_effect = closing
        return mock


def get_task_callback(*args, **kwargs):
    x = X(*args, **kwargs)
    x.blueprint.state = CLOSE
    asynloop(*x.args)
    return x, x.consumer.on_message


class test_asynloop:

    def setup(self):
        @self.app.task(shared=False)
        def add(x, y):
            return x + y
        self.add = add

    def test_drain_after_consume(self):
        x, _ = get_task_callback(self.app, transport_driver_type='amqp')
        assert _quick_drain in [p.fun for p in x.hub._ready]

    def test_pool_did_not_start_at_startup(self):
        x = X(self.app)
        x.obj.restart_count = 0
        x.obj.pool.did_start_ok.return_value = False
        with pytest.raises(WorkerLostError):
            asynloop(*x.args)

    def test_setup_heartbeat(self):
        x = X(self.app, heartbeat=10)
        x.hub.timer.call_repeatedly = Mock(name='x.hub.call_repeatedly()')
        x.blueprint.state = CLOSE
        asynloop(*x.args)
        x.consumer.consume.assert_called_with()
        x.obj.on_ready.assert_called_with()
        x.hub.timer.call_repeatedly.assert_called_with(
            10 / 2.0, x.connection.heartbeat_check, (2.0,),
        )

    def task_context(self, sig, **kwargs):
        x, on_task = get_task_callback(self.app, **kwargs)
        message = self.task_message_from_sig(self.app, sig)
        strategy = x.obj.strategies[sig.task] = Mock(name='strategy')
        return x, on_task, message, strategy

    def test_on_task_received(self):
        x, on_task, msg, strategy = self.task_context(self.add.s(2, 2))
        on_task(msg)
        strategy.assert_called_with(
            msg, None,
            PromiseEqual(x._consumer.call_soon, msg.ack_log_error),
            PromiseEqual(x._consumer.call_soon, msg.reject_log_error), [],
        )

    def test_on_task_received_executes_on_task_message(self):
        cbs = [Mock(), Mock(), Mock()]
        x, on_task, msg, strategy = self.task_context(
            self.add.s(2, 2), on_task_message=cbs,
        )
        on_task(msg)
        strategy.assert_called_with(
            msg, None,
            PromiseEqual(x._consumer.call_soon, msg.ack_log_error),
            PromiseEqual(x._consumer.call_soon, msg.reject_log_error),
            cbs,
        )

    def test_on_task_message_missing_name(self):
        x, on_task, msg, strategy = self.task_context(self.add.s(2, 2))
        msg.headers.pop('task')
        on_task(msg)
        x.on_unknown_message.assert_called_with(msg.decode(), msg)

    def test_on_task_pool_raises(self):
        x, on_task, msg, strategy = self.task_context(self.add.s(2, 2))
        strategy.side_effect = ValueError()
        with pytest.raises(ValueError):
            on_task(msg)

    def test_on_task_InvalidTaskError(self):
        x, on_task, msg, strategy = self.task_context(self.add.s(2, 2))
        exc = strategy.side_effect = InvalidTaskError()
        on_task(msg)
        x.on_invalid_task.assert_called_with(None, msg, exc)

    def test_on_task_DecodeError(self):
        x, on_task, msg, strategy = self.task_context(self.add.s(2, 2))
        exc = strategy.side_effect = DecodeError()
        on_task(msg)
        x.on_decode_error.assert_called_with(msg, exc)

    @pytest.mark.parametrize('should_stop', (None, False, True, EX_OK))
    def test_should_terminate(self, should_stop):
        x = X(self.app)
        state.should_stop = should_stop
        state.should_terminate = True
        try:
            with pytest.raises(WorkerTerminate):
                asynloop(*x.args)
        finally:
            state.should_stop = None
            state.should_terminate = None

    def test_should_terminate_hub_close_raises(self):
        x = X(self.app)
        # XXX why aren't the errors propagated?!?
        state.should_terminate = EX_FAILURE
        x.hub.close.side_effect = MemoryError()
        try:
            with pytest.raises(WorkerTerminate):
                asynloop(*x.args)
        finally:
            state.should_terminate = None

    def test_should_stop(self):
        x = X(self.app)
        state.should_stop = 303
        try:
            with pytest.raises(WorkerShutdown):
                asynloop(*x.args)
        finally:
            state.should_stop = None

    def test_updates_qos(self):
        x = X(self.app)
        x.qos.prev = 3
        x.qos.value = 3
        x.hub.on_tick.add(x.closer(mod=2))
        x.hub.timer._queue = [1]
        asynloop(*x.args)
        x.qos.update.assert_not_called()

        x = X(self.app)
        x.qos.prev = 1
        x.qos.value = 6
        x.hub.on_tick.add(x.closer(mod=2))
        asynloop(*x.args)
        x.qos.update.assert_called_with()
        x.hub.fire_timers.assert_called_with(propagate=(socket.error,))

    def test_poll_empty(self):
        x = X(self.app)
        x.hub.readers = {6: Mock()}
        x.hub.timer._queue = [1]
        x.close_then_error(x.hub.poller.poll)
        x.hub.fire_timers.return_value = 33.37
        poller = x.hub.poller
        poller.poll.return_value = []
        with pytest.raises(socket.error):
            asynloop(*x.args)
        poller.poll.assert_called_with(33.37)

    def test_poll_readable(self):
        x = X(self.app)
        reader = Mock(name='reader')
        x.hub.add_reader(6, reader, 6)
        x.hub.on_tick.add(x.close_then_error(Mock(name='tick'), mod=4))
        poller = x.hub.poller
        poller.poll.return_value = [(6, READ)]
        with pytest.raises(socket.error):
            asynloop(*x.args)
        reader.assert_called_with(6)
        poller.poll.assert_called()

    def test_poll_readable_raises_Empty(self):
        x = X(self.app)
        reader = Mock(name='reader')
        x.hub.add_reader(6, reader, 6)
        x.hub.on_tick.add(x.close_then_error(Mock(name='tick'), 2))
        poller = x.hub.poller
        poller.poll.return_value = [(6, READ)]
        reader.side_effect = Empty()
        with pytest.raises(socket.error):
            asynloop(*x.args)
        reader.assert_called_with(6)
        poller.poll.assert_called()

    def test_poll_writable(self):
        x = X(self.app)
        writer = Mock(name='writer')
        x.hub.add_writer(6, writer, 6)
        x.hub.on_tick.add(x.close_then_error(Mock(name='tick'), 2))
        poller = x.hub.poller
        poller.poll.return_value = [(6, WRITE)]
        with pytest.raises(socket.error):
            asynloop(*x.args)
        writer.assert_called_with(6)
        poller.poll.assert_called()

    def test_poll_writable_none_registered(self):
        x = X(self.app)
        writer = Mock(name='writer')
        x.hub.add_writer(6, writer, 6)
        x.hub.on_tick.add(x.close_then_error(Mock(name='tick'), 2))
        poller = x.hub.poller
        poller.poll.return_value = [(7, WRITE)]
        with pytest.raises(socket.error):
            asynloop(*x.args)
        poller.poll.assert_called()

    def test_poll_unknown_event(self):
        x = X(self.app)
        writer = Mock(name='reader')
        x.hub.add_writer(6, writer, 6)
        x.hub.on_tick.add(x.close_then_error(Mock(name='tick'), 2))
        poller = x.hub.poller
        poller.poll.return_value = [(6, 0)]
        with pytest.raises(socket.error):
            asynloop(*x.args)
        poller.poll.assert_called()

    def test_poll_keep_draining_disabled(self):
        x = X(self.app)
        x.hub.writers = {6: Mock()}
        poll = x.hub.poller.poll

        def se(*args, **kwargs):
            poll.side_effect = socket.error()
        poll.side_effect = se

        poller = x.hub.poller
        poll.return_value = [(6, 0)]
        with pytest.raises(socket.error):
            asynloop(*x.args)
        poller.poll.assert_called()

    def test_poll_err_writable(self):
        x = X(self.app)
        writer = Mock(name='writer')
        x.hub.add_writer(6, writer, 6, 48)
        x.hub.on_tick.add(x.close_then_error(Mock(), 2))
        poller = x.hub.poller
        poller.poll.return_value = [(6, ERR)]
        with pytest.raises(socket.error):
            asynloop(*x.args)
        writer.assert_called_with(6, 48)
        poller.poll.assert_called()

    def test_poll_write_generator(self):
        x = X(self.app)
        x.hub.remove = Mock(name='hub.remove()')

        def Gen():
            yield 1
            yield 2
        gen = Gen()

        x.hub.add_writer(6, gen)
        x.hub.on_tick.add(x.close_then_error(Mock(name='tick'), 2))
        x.hub.poller.poll.return_value = [(6, WRITE)]
        with pytest.raises(socket.error):
            asynloop(*x.args)
        assert gen.gi_frame.f_lasti != -1
        x.hub.remove.assert_not_called()

    def test_poll_write_generator_stopped(self):
        x = X(self.app)

        def Gen():
            if 0:
                yield
        gen = Gen()
        x.hub.add_writer(6, gen)
        x.hub.on_tick.add(x.close_then_error(Mock(name='tick'), 2))
        x.hub.poller.poll.return_value = [(6, WRITE)]
        x.hub.remove = Mock(name='hub.remove()')
        with pytest.raises(socket.error):
            asynloop(*x.args)
        assert gen.gi_frame is None

    def test_poll_write_generator_raises(self):
        x = X(self.app)

        def Gen():
            raise ValueError('foo')
            yield
        gen = Gen()
        x.hub.add_writer(6, gen)
        x.hub.remove = Mock(name='hub.remove()')
        x.hub.on_tick.add(x.close_then_error(Mock(name='tick'), 2))
        x.hub.poller.poll.return_value = [(6, WRITE)]
        with pytest.raises(ValueError):
            asynloop(*x.args)
        assert gen.gi_frame is None
        x.hub.remove.assert_called_with(6)

    def test_poll_err_readable(self):
        x = X(self.app)
        reader = Mock(name='reader')
        x.hub.add_reader(6, reader, 6, 24)
        x.hub.on_tick.add(x.close_then_error(Mock(), 2))
        poller = x.hub.poller
        poller.poll.return_value = [(6, ERR)]
        with pytest.raises(socket.error):
            asynloop(*x.args)
        reader.assert_called_with(6, 24)
        poller.poll.assert_called()

    def test_poll_raises_ValueError(self):
        x = X(self.app)
        x.hub.readers = {6: Mock()}
        poller = x.hub.poller
        x.close_then_error(poller.poll, exc=ValueError)
        asynloop(*x.args)
        poller.poll.assert_called()


class test_synloop:

    def test_timeout_ignored(self):
        x = X(self.app)
        x.timeout_then_error(x.connection.drain_events)
        with pytest.raises(socket.error):
            synloop(*x.args)
        assert x.connection.drain_events.call_count == 2

    def test_updates_qos_when_changed(self):
        x = X(self.app)
        x.qos.prev = 2
        x.qos.value = 2
        x.timeout_then_error(x.connection.drain_events)
        with pytest.raises(socket.error):
            synloop(*x.args)
        x.qos.update.assert_not_called()

        x.qos.value = 4
        x.timeout_then_error(x.connection.drain_events)
        with pytest.raises(socket.error):
            synloop(*x.args)
        x.qos.update.assert_called_with()

    def test_ignores_socket_errors_when_closed(self):
        x = X(self.app)
        x.close_then_error(x.connection.drain_events)
        assert synloop(*x.args) is None


class test_quick_drain:

    def setup(self):
        self.connection = Mock(name='connection')

    def test_drain(self):
        _quick_drain(self.connection, timeout=33.3)
        self.connection.drain_events.assert_called_with(timeout=33.3)

    def test_drain_error(self):
        exc = KeyError()
        exc.errno = 313
        self.connection.drain_events.side_effect = exc
        with pytest.raises(KeyError):
            _quick_drain(self.connection, timeout=33.3)

    def test_drain_error_EAGAIN(self):
        exc = KeyError()
        exc.errno = errno.EAGAIN
        self.connection.drain_events.side_effect = exc
        _quick_drain(self.connection, timeout=33.3)
