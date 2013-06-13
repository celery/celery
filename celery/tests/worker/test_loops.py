from __future__ import absolute_import

import socket

from mock import Mock

from celery.exceptions import InvalidTaskError, SystemTerminate
from celery.five import Empty
from celery.worker import state
from celery.worker.consumer import Consumer
from celery.worker.loops import asynloop, synloop, CLOSE, READ, WRITE, ERR

from celery.tests.case import AppCase, body_from_sig


class X(object):

    def __init__(self, heartbeat=None, on_task=None):
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
                         Mock(name='Hub'),
                         Mock(name='qos'),
                         heartbeat,
                         Mock(name='clock')]
        self.connection.supports_heartbeats = True
        self.consumer.callbacks = []
        self.obj.strategies = {}
        self.connection.connection_errors = (socket.error, )
        #hent = self.Hub.__enter__ = Mock(name='Hub.__enter__')
        #self.Hub.__exit__ = Mock(name='Hub.__exit__')
        #self.hub = hent.return_value = Mock(name='hub_context')
        self.hub.on_task = on_task or []
        self.hub.readers = {}
        self.hub.writers = {}
        self.hub.fire_timers.return_value = 1.7
        self.Hub = self.hub
        # need this for create_task_handler
        _consumer = Consumer(Mock(), timer=Mock())
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
        _consumer.strategies = self.obj.strategies

    def timeout_then_error(self, mock):

        def first(*args, **kwargs):
            mock.side_effect = socket.error()
            self.connection.more_to_read = False
            raise socket.timeout()
        mock.side_effect = first

    def close_then_error(self, mock, mod=0):

        def first(*args, **kwargs):
            if not mod or mock.call_count > mod:
                self.close()
                self.connection.more_to_read = False
                raise socket.error()
        mock.side_effect = first

    def close(self, *args, **kwargs):
        self.blueprint.state = CLOSE

    def closer(self, mock=None):
        mock = Mock() if mock is None else mock
        mock.side_effect = self.close
        return mock


def get_task_callback(*args, **kwargs):
    x = X(*args, **kwargs)
    x.blueprint.state = CLOSE
    asynloop(*x.args)
    return x, x.consumer.callbacks[0]


class test_asynloop(AppCase):

    def setup(self):

        @self.app.task()
        def add(x, y):
            return x + y
        self.add = add

    def test_setup_heartbeat(self):
        x = X(heartbeat=10)
        x.blueprint.state = CLOSE
        asynloop(*x.args)
        x.consumer.consume.assert_called_with()
        x.obj.on_ready.assert_called_with()
        x.hub.timer.apply_interval.assert_called_with(
            10 * 1000.0 / 2.0, x.connection.heartbeat_check, (2.0, ),
        )

    def task_context(self, sig, **kwargs):
        x, on_task = get_task_callback(**kwargs)
        body = body_from_sig(self.app, sig)
        message = Mock()
        strategy = x.obj.strategies[sig.task] = Mock()
        return x, on_task, body, message, strategy

    def test_on_task_received(self):
        _, on_task, body, msg, strategy = self.task_context(self.add.s(2, 2))
        on_task(body, msg)
        strategy.assert_called_with(msg, body, msg.ack_log_error)

    def test_on_task_received_executes_hub_on_task(self):
        cbs = [Mock(), Mock(), Mock()]
        _, on_task, body, msg, _ = self.task_context(
            self.add.s(2, 2), on_task=cbs,
        )
        on_task(body, msg)
        [cb.assert_called_with() for cb in cbs]

    def test_on_task_message_missing_name(self):
        x, on_task, body, msg, strategy = self.task_context(self.add.s(2, 2))
        body.pop('task')
        on_task(body, msg)
        x.on_unknown_message.assert_called_with(body, msg)

    def test_on_task_not_registered(self):
        x, on_task, body, msg, strategy = self.task_context(self.add.s(2, 2))
        exc = strategy.side_effect = KeyError(self.add.name)
        on_task(body, msg)
        x.on_unknown_task.assert_called_with(body, msg, exc)

    def test_on_task_InvalidTaskError(self):
        x, on_task, body, msg, strategy = self.task_context(self.add.s(2, 2))
        exc = strategy.side_effect = InvalidTaskError()
        on_task(body, msg)
        x.on_invalid_task.assert_called_with(body, msg, exc)

    def test_should_terminate(self):
        x = X()
        # XXX why aren't the errors propagated?!?
        state.should_terminate = True
        try:
            with self.assertRaises(SystemTerminate):
                asynloop(*x.args)
        finally:
            state.should_terminate = False

    def test_should_terminate_hub_close_raises(self):
        x = X()
        # XXX why aren't the errors propagated?!?
        state.should_terminate = True
        x.hub.close.side_effect = MemoryError()
        try:
            with self.assertRaises(SystemTerminate):
                asynloop(*x.args)
        finally:
            state.should_terminate = False

    def test_should_stop(self):
        x = X()
        state.should_stop = True
        try:
            with self.assertRaises(SystemExit):
                asynloop(*x.args)
        finally:
            state.should_stop = False

    def test_updates_qos(self):
        x = X()
        x.qos.prev = 3
        x.qos.value = 3
        asynloop(*x.args, sleep=x.closer())
        self.assertFalse(x.qos.update.called)

        x = X()
        x.qos.prev = 1
        x.qos.value = 6
        asynloop(*x.args, sleep=x.closer())
        x.qos.update.assert_called_with()
        x.hub.fire_timers.assert_called_with(propagate=(socket.error, ))
        x.connection.transport.on_poll_start.assert_called_with()

    def test_poll_empty(self):
        x = X()
        x.hub.readers = {6: Mock()}
        x.close_then_error(x.connection.drain_nowait)
        x.hub.fire_timers.return_value = 33.37
        x.hub.poller.poll.return_value = []
        with self.assertRaises(socket.error):
            asynloop(*x.args)
        x.hub.poller.poll.assert_called_with(33.37)
        x.connection.transport.on_poll_empty.assert_called_with()

    def test_poll_readable(self):
        x = X()
        x.hub.readers = {6: Mock()}
        x.close_then_error(x.connection.drain_nowait, mod=4)
        x.hub.poller.poll.return_value = [(6, READ)]
        with self.assertRaises(socket.error):
            asynloop(*x.args)
        x.hub.readers[6].assert_called_with(6, READ)
        self.assertTrue(x.hub.poller.poll.called)

    def test_poll_readable_raises_Empty(self):
        x = X()
        x.hub.readers = {6: Mock()}
        x.close_then_error(x.connection.drain_nowait)
        x.hub.poller.poll.return_value = [(6, READ)]
        x.hub.readers[6].side_effect = Empty()
        with self.assertRaises(socket.error):
            asynloop(*x.args)
        x.hub.readers[6].assert_called_with(6, READ)
        self.assertTrue(x.hub.poller.poll.called)

    def test_poll_writable(self):
        x = X()
        x.hub.writers = {6: Mock()}
        x.close_then_error(x.connection.drain_nowait)
        x.hub.poller.poll.return_value = [(6, WRITE)]
        with self.assertRaises(socket.error):
            asynloop(*x.args)
        x.hub.writers[6].assert_called_with(6, WRITE)
        self.assertTrue(x.hub.poller.poll.called)

    def test_poll_writable_none_registered(self):
        x = X()
        x.hub.writers = {6: Mock()}
        x.close_then_error(x.connection.drain_nowait)
        x.hub.poller.poll.return_value = [(7, WRITE)]
        with self.assertRaises(socket.error):
            asynloop(*x.args)
        self.assertTrue(x.hub.poller.poll.called)

    def test_poll_unknown_event(self):
        x = X()
        x.hub.writers = {6: Mock()}
        x.close_then_error(x.connection.drain_nowait)
        x.hub.poller.poll.return_value = [(6, 0)]
        with self.assertRaises(socket.error):
            asynloop(*x.args)
        self.assertTrue(x.hub.poller.poll.called)

    def test_poll_keep_draining_disabled(self):
        x = X()
        x.hub.writers = {6: Mock()}
        poll = x.hub.poller.poll

        def se(*args, **kwargs):
            poll.side_effect = socket.error()
        poll.side_effect = se

        x.connection.transport.nb_keep_draining = False
        x.close_then_error(x.connection.drain_nowait)
        x.hub.poller.poll.return_value = [(6, 0)]
        with self.assertRaises(socket.error):
            asynloop(*x.args)
        self.assertTrue(x.hub.poller.poll.called)
        self.assertFalse(x.connection.drain_nowait.called)

    def test_poll_err_writable(self):
        x = X()
        x.hub.writers = {6: Mock()}
        x.close_then_error(x.connection.drain_nowait)
        x.hub.poller.poll.return_value = [(6, ERR)]
        with self.assertRaises(socket.error):
            asynloop(*x.args)
        x.hub.writers[6].assert_called_with(6, ERR)
        self.assertTrue(x.hub.poller.poll.called)

    def test_poll_write_generator(self):
        x = X()

        def Gen():
            yield 1
            yield 2
        gen = Gen()

        x.hub.writers = {6: gen}
        x.close_then_error(x.connection.drain_nowait)
        x.hub.poller.poll.return_value = [(6, WRITE)]
        with self.assertRaises(socket.error):
            asynloop(*x.args)
        self.assertTrue(gen.gi_frame.f_lasti != -1)
        self.assertFalse(x.hub.remove.called)

    def test_poll_write_generator_stopped(self):
        x = X()

        def Gen():
            raise StopIteration()
            yield
        gen = Gen()
        x.hub.writers = {6: gen}
        x.close_then_error(x.connection.drain_nowait)
        x.hub.poller.poll.return_value = [(6, WRITE)]
        with self.assertRaises(socket.error):
            asynloop(*x.args)
        self.assertIsNone(gen.gi_frame)
        x.hub.remove.assert_called_with(6)

    def test_poll_write_generator_raises(self):
        x = X()

        def Gen():
            raise ValueError('foo')
            yield
        gen = Gen()
        x.hub.writers = {6: gen}
        x.close_then_error(x.connection.drain_nowait)
        x.hub.poller.poll.return_value = [(6, WRITE)]
        with self.assertRaises(ValueError):
            asynloop(*x.args)
        self.assertIsNone(gen.gi_frame)
        x.hub.remove.assert_called_with(6)

    def test_poll_err_readable(self):
        x = X()
        x.hub.readers = {6: Mock()}
        x.close_then_error(x.connection.drain_nowait)
        x.hub.poller.poll.return_value = [(6, ERR)]
        with self.assertRaises(socket.error):
            asynloop(*x.args)
        x.hub.readers[6].assert_called_with(6, ERR)
        self.assertTrue(x.hub.poller.poll.called)

    def test_poll_raises_ValueError(self):
        x = X()
        x.hub.readers = {6: Mock()}
        x.close_then_error(x.connection.drain_nowait)
        x.hub.poller.poll.side_effect = ValueError()
        asynloop(*x.args)
        self.assertTrue(x.hub.poller.poll.called)


class test_synloop(AppCase):

    def test_timeout_ignored(self):
        x = X()
        x.timeout_then_error(x.connection.drain_events)
        with self.assertRaises(socket.error):
            synloop(*x.args)
        self.assertEqual(x.connection.drain_events.call_count, 2)

    def test_updates_qos_when_changed(self):
        x = X()
        x.qos.prev = 2
        x.qos.value = 2
        x.timeout_then_error(x.connection.drain_events)
        with self.assertRaises(socket.error):
            synloop(*x.args)
        self.assertFalse(x.qos.update.called)

        x.qos.value = 4
        x.timeout_then_error(x.connection.drain_events)
        with self.assertRaises(socket.error):
            synloop(*x.args)
        x.qos.update.assert_called_with()

    def test_ignores_socket_errors_when_closed(self):
        x = X()
        x.close_then_error(x.connection.drain_events)
        self.assertIsNone(synloop(*x.args))
