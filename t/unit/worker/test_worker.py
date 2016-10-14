from __future__ import absolute_import, print_function, unicode_literals

import os
import pytest
import socket
import sys

from collections import deque
from datetime import datetime, timedelta
from functools import partial
from threading import Event

from amqp import ChannelError
from case import Mock, patch, skip
from kombu import Connection
from kombu.common import QoS, ignore_errors
from kombu.transport.base import Message
from kombu.transport.memory import Transport
from kombu.utils.uuid import uuid

from celery.bootsteps import RUN, CLOSE, TERMINATE, StartStopStep
from celery.concurrency.base import BasePool
from celery.exceptions import (
    WorkerShutdown, WorkerTerminate, TaskRevokedError,
    InvalidTaskError, ImproperlyConfigured,
)
from celery.five import Empty, range, Queue as FastQueue
from celery.platforms import EX_FAILURE
from celery import worker as worker_module
from celery.worker import components
from celery.worker import consumer
from celery.worker import state
from celery.worker.consumer import Consumer
from celery.worker.pidbox import gPidbox
from celery.worker.request import Request
from celery.utils.nodenames import worker_direct
from celery.utils.serialization import pickle
from celery.utils.timer2 import Timer


def MockStep(step=None):
    if step is None:
        step = Mock(name='step')
    else:
        step.blueprint = Mock(name='step.blueprint')
    step.blueprint.name = 'MockNS'
    step.name = 'MockStep(%s)' % (id(step),)
    return step


def mock_event_dispatcher():
    evd = Mock(name='event_dispatcher')
    evd.groups = ['worker']
    evd._outbound_buffer = deque()
    return evd


def find_step(obj, typ):
    return obj.blueprint.steps[typ.name]


def create_message(channel, **data):
    data.setdefault('id', uuid())
    m = Message(channel, body=pickle.dumps(dict(**data)),
                content_type='application/x-python-serialize',
                content_encoding='binary',
                delivery_info={'consumer_tag': 'mock'})
    m.accept = ['application/x-python-serialize']
    return m


class ConsumerCase:

    def create_task_message(self, channel, *args, **kwargs):
        m = self.TaskMessage(*args, **kwargs)
        m.channel = channel
        m.delivery_info = {'consumer_tag': 'mock'}
        return m


class test_Consumer(ConsumerCase):

    def setup(self):
        self.buffer = FastQueue()
        self.timer = Timer()

        @self.app.task(shared=False)
        def foo_task(x, y, z):
            return x * y * z
        self.foo_task = foo_task

    def teardown(self):
        self.timer.stop()

    def LoopConsumer(self, buffer=None, controller=None, timer=None, app=None,
                     without_mingle=True, without_gossip=True,
                     without_heartbeat=True, **kwargs):
        if controller is None:
            controller = Mock(name='.controller')
        buffer = buffer if buffer is not None else self.buffer.put
        timer = timer if timer is not None else self.timer
        app = app if app is not None else self.app
        c = Consumer(
            buffer,
            timer=timer,
            app=app,
            controller=controller,
            without_mingle=without_mingle,
            without_gossip=without_gossip,
            without_heartbeat=without_heartbeat,
            **kwargs
        )
        c.task_consumer = Mock(name='.task_consumer')
        c.qos = QoS(c.task_consumer.qos, 10)
        c.connection = Mock(name='.connection')
        c.controller = c.app.WorkController()
        c.heart = Mock(name='.heart')
        c.controller.consumer = c
        c.pool = c.controller.pool = Mock(name='.controller.pool')
        c.node = Mock(name='.node')
        c.event_dispatcher = mock_event_dispatcher()
        return c

    def NoopConsumer(self, *args, **kwargs):
        c = self.LoopConsumer(*args, **kwargs)
        c.loop = Mock(name='.loop')
        return c

    def test_info(self):
        c = self.NoopConsumer()
        c.connection.info.return_value = {'foo': 'bar'}
        c.controller.pool.info.return_value = [Mock(), Mock()]
        info = c.controller.stats()
        assert info['prefetch_count'] == 10
        assert info['broker']

    def test_start_when_closed(self):
        c = self.NoopConsumer()
        c.blueprint.state = CLOSE
        c.start()

    def test_connection(self):
        c = self.NoopConsumer()

        c.blueprint.start(c)
        assert isinstance(c.connection, Connection)

        c.blueprint.state = RUN
        c.event_dispatcher = None
        c.blueprint.restart(c)
        assert c.connection

        c.blueprint.state = RUN
        c.shutdown()
        assert c.connection is None
        assert c.task_consumer is None

        c.blueprint.start(c)
        assert isinstance(c.connection, Connection)
        c.blueprint.restart(c)

        c.stop()
        c.shutdown()
        assert c.connection is None
        assert c.task_consumer is None

    def test_close_connection(self):
        c = self.NoopConsumer()
        c.blueprint.state = RUN
        step = find_step(c, consumer.Connection)
        connection = c.connection
        step.shutdown(c)
        connection.close.assert_called()
        assert c.connection is None

    def test_close_connection__heart_shutdown(self):
        c = self.NoopConsumer()
        event_dispatcher = c.event_dispatcher
        heart = c.heart
        c.event_dispatcher.enabled = True
        c.blueprint.state = RUN
        Events = find_step(c, consumer.Events)
        Events.shutdown(c)
        Heart = find_step(c, consumer.Heart)
        Heart.shutdown(c)
        event_dispatcher.close.assert_called()
        heart.stop.assert_called_with()

    @patch('celery.worker.consumer.consumer.warn')
    def test_receive_message_unknown(self, warn):
        c = self.LoopConsumer()
        c.blueprint.state = RUN
        c.steps.pop()
        channel = Mock(name='.channeol')
        m = create_message(channel, unknown={'baz': '!!!'})

        callback = self._get_on_message(c)
        callback(m)
        warn.assert_called()

    @patch('celery.worker.strategy.to_timestamp')
    def test_receive_message_eta_OverflowError(self, to_timestamp):
        to_timestamp.side_effect = OverflowError()
        c = self.LoopConsumer()
        c.blueprint.state = RUN
        c.steps.pop()
        m = self.create_task_message(
            Mock(), self.foo_task.name,
            args=('2, 2'), kwargs={},
            eta=datetime.now().isoformat(),
        )
        c.update_strategies()
        callback = self._get_on_message(c)
        callback(m)
        assert m.acknowledged

    @patch('celery.worker.consumer.consumer.error')
    def test_receive_message_InvalidTaskError(self, error):
        c = self.LoopConsumer()
        c.blueprint.state = RUN
        c.steps.pop()
        m = self.create_task_message(
            Mock(), self.foo_task.name,
            args=(1, 2), kwargs='foobarbaz', id=1)
        c.update_strategies()
        strat = c.strategies[self.foo_task.name] = Mock(name='strategy')
        strat.side_effect = InvalidTaskError()

        callback = self._get_on_message(c)
        callback(m)
        error.assert_called()
        assert 'Received invalid task message' in error.call_args[0][0]

    @patch('celery.worker.consumer.consumer.crit')
    def test_on_decode_error(self, crit):
        c = self.LoopConsumer()

        class MockMessage(Mock):
            content_type = 'application/x-msgpack'
            content_encoding = 'binary'
            body = 'foobarbaz'

        message = MockMessage()
        c.on_decode_error(message, KeyError('foo'))
        assert message.ack.call_count
        assert "Can't decode message body" in crit.call_args[0][0]

    def _get_on_message(self, c):
        if c.qos is None:
            c.qos = Mock()
        c.task_consumer = Mock()
        c.event_dispatcher = mock_event_dispatcher()
        c.connection = Mock(name='.connection')
        c.connection.drain_events.side_effect = WorkerShutdown()

        with pytest.raises(WorkerShutdown):
            c.loop(*c.loop_args())
        assert c.task_consumer.on_message
        return c.task_consumer.on_message

    def test_receieve_message(self):
        c = self.LoopConsumer()
        c.blueprint.state = RUN
        m = self.create_task_message(
            Mock(), self.foo_task.name,
            args=[2, 4, 8], kwargs={},
        )
        c.update_strategies()
        callback = self._get_on_message(c)
        callback(m)

        in_bucket = self.buffer.get_nowait()
        assert isinstance(in_bucket, Request)
        assert in_bucket.name == self.foo_task.name
        assert in_bucket.execute() == 2 * 4 * 8
        assert self.timer.empty()

    def test_start_channel_error(self):
        c = self.NoopConsumer(task_events=False, pool=BasePool())
        c.loop.on_nth_call_do_raise(KeyError('foo'), SyntaxError('bar'))
        c.channel_errors = (KeyError,)
        try:
            with pytest.raises(KeyError):
                c.start()
        finally:
            c.timer and c.timer.stop()

    def test_start_connection_error(self):
        c = self.NoopConsumer(task_events=False, pool=BasePool())
        c.loop.on_nth_call_do_raise(KeyError('foo'), SyntaxError('bar'))
        c.connection_errors = (KeyError,)
        try:
            with pytest.raises(SyntaxError):
                c.start()
        finally:
            c.timer and c.timer.stop()

    def test_loop_ignores_socket_timeout(self):

        class Connection(self.app.connection_for_read().__class__):
            obj = None

            def drain_events(self, **kwargs):
                self.obj.connection = None
                raise socket.timeout(10)

        c = self.NoopConsumer()
        c.connection = Connection()
        c.connection.obj = c
        c.qos = QoS(c.task_consumer.qos, 10)
        c.loop(*c.loop_args())

    def test_loop_when_socket_error(self):

        class Connection(self.app.connection_for_read().__class__):
            obj = None

            def drain_events(self, **kwargs):
                self.obj.connection = None
                raise socket.error('foo')

        c = self.LoopConsumer()
        c.blueprint.state = RUN
        conn = c.connection = Connection()
        c.connection.obj = c
        c.qos = QoS(c.task_consumer.qos, 10)
        with pytest.raises(socket.error):
            c.loop(*c.loop_args())

        c.blueprint.state = CLOSE
        c.connection = conn
        c.loop(*c.loop_args())

    def test_loop(self):

        class Connection(self.app.connection_for_read().__class__):
            obj = None

            def drain_events(self, **kwargs):
                self.obj.connection = None

            @property
            def supports_heartbeats(self):
                return False

        c = self.LoopConsumer()
        c.blueprint.state = RUN
        c.connection = Connection()
        c.connection.obj = c
        c.qos = QoS(c.task_consumer.qos, 10)

        c.loop(*c.loop_args())
        c.loop(*c.loop_args())
        assert c.task_consumer.consume.call_count
        c.task_consumer.qos.assert_called_with(prefetch_count=10)
        assert c.qos.value == 10
        c.qos.decrement_eventually()
        assert c.qos.value == 9
        c.qos.update()
        assert c.qos.value == 9
        c.task_consumer.qos.assert_called_with(prefetch_count=9)

    def test_ignore_errors(self):
        c = self.NoopConsumer()
        c.connection_errors = (AttributeError, KeyError,)
        c.channel_errors = (SyntaxError,)
        ignore_errors(c, Mock(side_effect=AttributeError('foo')))
        ignore_errors(c, Mock(side_effect=KeyError('foo')))
        ignore_errors(c, Mock(side_effect=SyntaxError('foo')))
        with pytest.raises(IndexError):
            ignore_errors(c, Mock(side_effect=IndexError('foo')))

    def test_apply_eta_task(self):
        c = self.NoopConsumer()
        c.qos = QoS(None, 10)
        task = Mock(name='task', id='1234213')
        qos = c.qos.value
        c.apply_eta_task(task)
        assert task in state.reserved_requests
        assert c.qos.value == qos - 1
        assert self.buffer.get_nowait() is task

    def test_receieve_message_eta_isoformat(self):
        c = self.LoopConsumer()
        c.blueprint.state = RUN
        c.steps.pop()
        m = self.create_task_message(
            Mock(), self.foo_task.name,
            eta=(datetime.now() + timedelta(days=1)).isoformat(),
            args=[2, 4, 8], kwargs={},
        )

        c.qos = QoS(c.task_consumer.qos, 1)
        current_pcount = c.qos.value
        c.event_dispatcher.enabled = False
        c.update_strategies()
        callback = self._get_on_message(c)
        callback(m)
        c.timer.stop()
        c.timer.join(1)

        items = [entry[2] for entry in self.timer.queue]
        found = 0
        for item in items:
            if item.args[0].name == self.foo_task.name:
                found = True
        assert found
        assert c.qos.value > current_pcount
        c.timer.stop()

    def test_pidbox_callback(self):
        c = self.NoopConsumer()
        con = find_step(c, consumer.Control).box
        con.node = Mock()
        con.reset = Mock()

        con.on_message('foo', 'bar')
        con.node.handle_message.assert_called_with('foo', 'bar')

        con.node = Mock()
        con.node.handle_message.side_effect = KeyError('foo')
        con.on_message('foo', 'bar')
        con.node.handle_message.assert_called_with('foo', 'bar')

        con.node = Mock()
        con.node.handle_message.side_effect = ValueError('foo')
        con.on_message('foo', 'bar')
        con.node.handle_message.assert_called_with('foo', 'bar')
        con.reset.assert_called()

    def test_revoke(self):
        c = self.LoopConsumer()
        c.blueprint.state = RUN
        c.steps.pop()
        channel = Mock(name='channel')
        id = uuid()
        t = self.create_task_message(
            channel, self.foo_task.name,
            args=[2, 4, 8], kwargs={}, id=id,
        )

        state.revoked.add(id)

        callback = self._get_on_message(c)
        callback(t)
        assert self.buffer.empty()

    def test_receieve_message_not_registered(self):
        c = self.LoopConsumer()
        c.blueprint.state = RUN
        c.steps.pop()
        channel = Mock(name='channel')
        m = self.create_task_message(
            channel, 'x.X.31x', args=[2, 4, 8], kwargs={},
        )

        callback = self._get_on_message(c)
        assert not callback(m)
        with pytest.raises(Empty):
            self.buffer.get_nowait()
        assert self.timer.empty()

    @patch('celery.worker.consumer.consumer.warn')
    @patch('celery.worker.consumer.consumer.logger')
    def test_receieve_message_ack_raises(self, logger, warn):
        c = self.LoopConsumer()
        c.blueprint.state = RUN
        channel = Mock(name='channel')
        m = self.create_task_message(
            channel, self.foo_task.name,
            args=[2, 4, 8], kwargs={},
        )
        m.headers = None

        c.update_strategies()
        c.connection_errors = (socket.error,)
        m.reject = Mock()
        m.reject.side_effect = socket.error('foo')
        callback = self._get_on_message(c)
        assert not callback(m)
        warn.assert_called()
        with pytest.raises(Empty):
            self.buffer.get_nowait()
        assert self.timer.empty()
        m.reject_log_error.assert_called_with(logger, c.connection_errors)

    def test_receive_message_eta(self):
        if os.environ.get('C_DEBUG_TEST'):
            pp = partial(print, file=sys.__stderr__)
        else:
            def pp(*args, **kwargs):
                pass
        pp('TEST RECEIVE MESSAGE ETA')
        pp('+CREATE MYKOMBUCONSUMER')
        c = self.LoopConsumer()
        pp('-CREATE MYKOMBUCONSUMER')
        c.steps.pop()
        channel = Mock(name='channel')
        pp('+ CREATE MESSAGE')
        m = self.create_task_message(
            channel, self.foo_task.name,
            args=[2, 4, 8], kwargs={},
            eta=(datetime.now() + timedelta(days=1)).isoformat(),
        )
        pp('- CREATE MESSAGE')

        try:
            pp('+ BLUEPRINT START 1')
            c.blueprint.start(c)
            pp('- BLUEPRINT START 1')
            p = c.app.conf.broker_connection_retry
            c.app.conf.broker_connection_retry = False
            pp('+ BLUEPRINT START 2')
            c.blueprint.start(c)
            pp('- BLUEPRINT START 2')
            c.app.conf.broker_connection_retry = p
            pp('+ BLUEPRINT RESTART')
            c.blueprint.restart(c)
            pp('- BLUEPRINT RESTART')
            pp('+ GET ON MESSAGE')
            callback = self._get_on_message(c)
            pp('- GET ON MESSAGE')
            pp('+ CALLBACK')
            callback(m)
            pp('- CALLBACK')
        finally:
            pp('+ STOP TIMER')
            c.timer.stop()
            pp('- STOP TIMER')
            try:
                pp('+ JOIN TIMER')
                c.timer.join()
                pp('- JOIN TIMER')
            except RuntimeError:
                pass

        in_hold = c.timer.queue[0]
        assert len(in_hold) == 3
        eta, priority, entry = in_hold
        task = entry.args[0]
        assert isinstance(task, Request)
        assert task.name == self.foo_task.name
        assert task.execute() == 2 * 4 * 8
        with pytest.raises(Empty):
            self.buffer.get_nowait()

    def test_reset_pidbox_node(self):
        c = self.NoopConsumer()
        con = find_step(c, consumer.Control).box
        con.node = Mock()
        chan = con.node.channel = Mock()
        chan.close.side_effect = socket.error('foo')
        c.connection_errors = (socket.error,)
        con.reset()
        chan.close.assert_called_with()

    def test_reset_pidbox_node_green(self):
        c = self.NoopConsumer(pool=Mock(is_green=True))
        con = find_step(c, consumer.Control)
        assert isinstance(con.box, gPidbox)
        con.start(c)
        c.pool.spawn_n.assert_called_with(con.box.loop, c)

    def test_green_pidbox_node(self):
        pool = Mock()
        pool.is_green = True
        c = self.NoopConsumer(pool=Mock(is_green=True))
        controller = find_step(c, consumer.Control)

        class BConsumer(Mock):

            def __enter__(self):
                self.consume()
                return self

            def __exit__(self, *exc_info):
                self.cancel()

        controller.box.node.listen = BConsumer()
        connections = []

        class Connection(object):
            calls = 0

            def __init__(self, obj):
                connections.append(self)
                self.obj = obj
                self.default_channel = self.channel()
                self.closed = False

            def __enter__(self):
                return self

            def __exit__(self, *exc_info):
                self.close()

            def channel(self):
                return Mock()

            def as_uri(self):
                return 'dummy://'

            def drain_events(self, **kwargs):
                if not self.calls:
                    self.calls += 1
                    raise socket.timeout()
                self.obj.connection = None
                controller.box._node_shutdown.set()

            def close(self):
                self.closed = True

        c.connect = lambda: Connection(obj=c)
        controller = find_step(c, consumer.Control)
        controller.box.loop(c)

        controller.box.node.listen.assert_called()
        assert controller.box.consumer
        controller.box.consumer.consume.assert_called_with()

        assert c.connection is None
        assert connections[0].closed

    @patch('kombu.connection.Connection._establish_connection')
    @patch('kombu.utils.functional.sleep')
    def test_connect_errback(self, sleep, connect):
        c = self.NoopConsumer()
        Transport.connection_errors = (ChannelError,)
        connect.on_nth_call_do(ChannelError('error'), n=1)
        c.connect()
        connect.assert_called_with()

    def test_stop_pidbox_node(self):
        c = self.NoopConsumer()
        cont = find_step(c, consumer.Control)
        cont._node_stopped = Event()
        cont._node_shutdown = Event()
        cont._node_stopped.set()
        cont.stop(c)

    def test_start__loop(self):

        class _QoS(object):
            prev = 3
            value = 4

            def update(self):
                self.prev = self.value

        init_callback = Mock(name='init_callback')
        c = self.NoopConsumer(init_callback=init_callback)
        c.qos = _QoS()
        c.connection = Connection()
        c.iterations = 0

        def raises_KeyError(*args, **kwargs):
            c.iterations += 1
            if c.qos.prev != c.qos.value:
                c.qos.update()
            if c.iterations >= 2:
                raise KeyError('foo')

        c.loop = raises_KeyError
        with pytest.raises(KeyError):
            c.start()
        assert c.iterations == 2
        assert c.qos.prev == c.qos.value

        init_callback.reset_mock()
        c = self.NoopConsumer(task_events=False, init_callback=init_callback)
        c.qos = _QoS()
        c.connection = Connection()
        c.loop = Mock(side_effect=socket.error('foo'))
        with pytest.raises(socket.error):
            c.start()
        c.loop.assert_called()

    def test_reset_connection_with_no_node(self):
        c = self.NoopConsumer()
        c.steps.pop()
        c.blueprint.start(c)


class test_WorkController(ConsumerCase):

    def setup(self):
        self.worker = self.create_worker()
        self._logger = worker_module.logger
        self._comp_logger = components.logger
        self.logger = worker_module.logger = Mock()
        self.comp_logger = components.logger = Mock()

        @self.app.task(shared=False)
        def foo_task(x, y, z):
            return x * y * z
        self.foo_task = foo_task

    def teardown(self):
        worker_module.logger = self._logger
        components.logger = self._comp_logger

    def create_worker(self, **kw):
        worker = self.app.WorkController(concurrency=1, loglevel=0, **kw)
        worker.blueprint.shutdown_complete.set()
        return worker

    def test_on_consumer_ready(self):
        self.worker.on_consumer_ready(Mock())

    def test_setup_queues_worker_direct(self):
        self.app.conf.worker_direct = True
        self.app.amqp.__dict__['queues'] = Mock()
        self.worker.setup_queues({})
        self.app.amqp.queues.select_add.assert_called_with(
            worker_direct(self.worker.hostname),
        )

    def test_setup_queues__missing_queue(self):
        self.app.amqp.queues.select = Mock(name='select')
        self.app.amqp.queues.deselect = Mock(name='deselect')
        self.app.amqp.queues.select.side_effect = KeyError()
        self.app.amqp.queues.deselect.side_effect = KeyError()
        with pytest.raises(ImproperlyConfigured):
            self.worker.setup_queues('x,y', exclude='foo,bar')
        self.app.amqp.queues.select = Mock(name='select')
        with pytest.raises(ImproperlyConfigured):
            self.worker.setup_queues('x,y', exclude='foo,bar')

    def test_send_worker_shutdown(self):
        with patch('celery.signals.worker_shutdown') as ws:
            self.worker._send_worker_shutdown()
            ws.send.assert_called_with(sender=self.worker)

    @skip.todo('unstable test')
    def test_process_shutdown_on_worker_shutdown(self):
        from celery.concurrency.prefork import process_destructor
        from celery.concurrency.asynpool import Worker
        with patch('celery.signals.worker_process_shutdown') as ws:
            with patch('os._exit') as _exit:
                worker = Worker(None, None, on_exit=process_destructor)
                worker._do_exit(22, 3.1415926)
                ws.send.assert_called_with(
                    sender=None, pid=22, exitcode=3.1415926,
                )
                _exit.assert_called_with(3.1415926)

    def test_process_task_revoked_release_semaphore(self):
        self.worker._quick_release = Mock()
        req = Mock()
        req.execute_using_pool.side_effect = TaskRevokedError
        self.worker._process_task(req)
        self.worker._quick_release.assert_called_with()

        delattr(self.worker, '_quick_release')
        self.worker._process_task(req)

    def test_shutdown_no_blueprint(self):
        self.worker.blueprint = None
        self.worker._shutdown()

    @patch('celery.worker.create_pidlock')
    def test_use_pidfile(self, create_pidlock):
        create_pidlock.return_value = Mock()
        worker = self.create_worker(pidfile='pidfilelockfilepid')
        worker.steps = []
        worker.start()
        create_pidlock.assert_called()
        worker.stop()
        worker.pidlock.release.assert_called()

    def test_attrs(self):
        worker = self.worker
        assert worker.timer is not None
        assert isinstance(worker.timer, Timer)
        assert worker.pool is not None
        assert worker.consumer is not None
        assert worker.steps

    def test_with_embedded_beat(self):
        worker = self.app.WorkController(concurrency=1, loglevel=0, beat=True)
        assert worker.beat
        assert worker.beat in [w.obj for w in worker.steps]

    def test_with_autoscaler(self):
        worker = self.create_worker(
            autoscale=[10, 3], send_events=False,
            timer_cls='celery.utils.timer2.Timer',
        )
        assert worker.autoscaler

    def test_dont_stop_or_terminate(self):
        worker = self.app.WorkController(concurrency=1, loglevel=0)
        worker.stop()
        assert worker.blueprint.state != CLOSE
        worker.terminate()
        assert worker.blueprint.state != CLOSE

        sigsafe, worker.pool.signal_safe = worker.pool.signal_safe, False
        try:
            worker.blueprint.state = RUN
            worker.stop(in_sighandler=True)
            assert worker.blueprint.state != CLOSE
            worker.terminate(in_sighandler=True)
            assert worker.blueprint.state != CLOSE
        finally:
            worker.pool.signal_safe = sigsafe

    def test_on_timer_error(self):
        worker = self.app.WorkController(concurrency=1, loglevel=0)

        try:
            raise KeyError('foo')
        except KeyError as exc:
            components.Timer(worker).on_timer_error(exc)
            msg, args = self.comp_logger.error.call_args[0]
            assert 'KeyError' in msg % args

    def test_on_timer_tick(self):
        worker = self.app.WorkController(concurrency=1, loglevel=10)

        components.Timer(worker).on_timer_tick(30.0)
        xargs = self.comp_logger.debug.call_args[0]
        fmt, arg = xargs[0], xargs[1]
        assert arg == 30.0
        assert 'Next ETA %s secs' in fmt

    def test_process_task(self):
        worker = self.worker
        worker.pool = Mock()
        channel = Mock()
        m = self.create_task_message(
            channel, self.foo_task.name,
            args=[4, 8, 10], kwargs={},
        )
        task = Request(m, app=self.app)
        worker._process_task(task)
        assert worker.pool.apply_async.call_count == 1
        worker.pool.stop()

    def test_process_task_raise_base(self):
        worker = self.worker
        worker.pool = Mock()
        worker.pool.apply_async.side_effect = KeyboardInterrupt('Ctrl+C')
        channel = Mock()
        m = self.create_task_message(
            channel, self.foo_task.name,
            args=[4, 8, 10], kwargs={},
        )
        task = Request(m, app=self.app)
        worker.steps = []
        worker.blueprint.state = RUN
        with pytest.raises(KeyboardInterrupt):
            worker._process_task(task)

    def test_process_task_raise_WorkerTerminate(self):
        worker = self.worker
        worker.pool = Mock()
        worker.pool.apply_async.side_effect = WorkerTerminate()
        channel = Mock()
        m = self.create_task_message(
            channel, self.foo_task.name,
            args=[4, 8, 10], kwargs={},
        )
        task = Request(m, app=self.app)
        worker.steps = []
        worker.blueprint.state = RUN
        with pytest.raises(SystemExit):
            worker._process_task(task)

    def test_process_task_raise_regular(self):
        worker = self.worker
        worker.pool = Mock()
        worker.pool.apply_async.side_effect = KeyError('some exception')
        channel = Mock()
        m = self.create_task_message(
            channel, self.foo_task.name,
            args=[4, 8, 10], kwargs={},
        )
        task = Request(m, app=self.app)
        worker._process_task(task)
        worker.pool.stop()

    def test_start_catches_base_exceptions(self):
        worker1 = self.create_worker()
        worker1.blueprint.state = RUN
        stc = MockStep()
        stc.start.side_effect = WorkerTerminate()
        worker1.steps = [stc]
        worker1.start()
        stc.start.assert_called_with(worker1)
        assert stc.terminate.call_count

        worker2 = self.create_worker()
        worker2.blueprint.state = RUN
        sec = MockStep()
        sec.start.side_effect = WorkerShutdown()
        sec.terminate = None
        worker2.steps = [sec]
        worker2.start()
        assert sec.stop.call_count

    def test_statedb(self):
        from celery.worker import state
        Persistent = state.Persistent

        state.Persistent = Mock()
        try:
            worker = self.create_worker(statedb='statefilename')
            assert worker._persistence
        finally:
            state.Persistent = Persistent

    def test_process_task_sem(self):
        worker = self.worker
        worker._quick_acquire = Mock()

        req = Mock()
        worker._process_task_sem(req)
        worker._quick_acquire.assert_called_with(worker._process_task, req)

    def test_signal_consumer_close(self):
        worker = self.worker
        worker.consumer = Mock()

        worker.signal_consumer_close()
        worker.consumer.close.assert_called_with()

        worker.consumer.close.side_effect = AttributeError()
        worker.signal_consumer_close()

    def test_rusage__no_resource(self):
        from celery import worker
        prev, worker.resource = worker.resource, None
        try:
            self.worker.pool = Mock(name='pool')
            with pytest.raises(NotImplementedError):
                self.worker.rusage()
            self.worker.stats()
        finally:
            worker.resource = prev

    def test_repr(self):
        assert repr(self.worker)

    def test_str(self):
        assert str(self.worker) == self.worker.hostname

    def test_start__stop(self):
        worker = self.worker
        worker.blueprint.shutdown_complete.set()
        worker.steps = [MockStep(StartStopStep(self)) for _ in range(4)]
        worker.blueprint.state = RUN
        worker.blueprint.started = 4
        for w in worker.steps:
            w.start = Mock()
            w.close = Mock()
            w.stop = Mock()

        worker.start()
        for w in worker.steps:
            w.start.assert_called()
        worker.consumer = Mock()
        worker.stop(exitcode=3)
        for stopstep in worker.steps:
            stopstep.close.assert_called()
            stopstep.stop.assert_called()

        # Doesn't close pool if no pool.
        worker.start()
        worker.pool = None
        worker.stop()

        # test that stop of None is not attempted
        worker.steps[-1] = None
        worker.start()
        worker.stop()

    def test_start__KeyboardInterrupt(self):
        worker = self.worker
        worker.blueprint = Mock(name='blueprint')
        worker.blueprint.start.side_effect = KeyboardInterrupt()
        worker.stop = Mock(name='stop')
        worker.start()
        worker.stop.assert_called_with(exitcode=EX_FAILURE)

    def test_register_with_event_loop(self):
        worker = self.worker
        hub = Mock(name='hub')
        worker.blueprint = Mock(name='blueprint')
        worker.register_with_event_loop(hub)
        worker.blueprint.send_all.assert_called_with(
            worker, 'register_with_event_loop', args=(hub,),
            description='hub.register',
        )

    def test_step_raises(self):
        worker = self.worker
        step = Mock()
        worker.steps = [step]
        step.start.side_effect = TypeError()
        worker.stop = Mock()
        worker.start()
        worker.stop.assert_called_with(exitcode=EX_FAILURE)

    def test_state(self):
        assert self.worker.state

    def test_start__terminate(self):
        worker = self.worker
        worker.blueprint.shutdown_complete.set()
        worker.blueprint.started = 5
        worker.blueprint.state = RUN
        worker.steps = [MockStep() for _ in range(5)]
        worker.start()
        for w in worker.steps[:3]:
            w.start.assert_called()
        assert worker.blueprint.started == len(worker.steps)
        assert worker.blueprint.state == RUN
        worker.terminate()
        for step in worker.steps:
            step.terminate.assert_called()
        worker.blueprint.state = TERMINATE
        worker.terminate()

    def test_Hub_create(self):
        w = Mock()
        x = components.Hub(w)
        x.create(w)
        assert w.timer.max_interval

    def test_Pool_create_threaded(self):
        w = Mock()
        w._conninfo.connection_errors = w._conninfo.channel_errors = ()
        w.pool_cls = Mock()
        w.use_eventloop = False
        pool = components.Pool(w)
        pool.create(w)

    def test_Pool_pool_no_sem(self):
        w = Mock()
        w.pool_cls.uses_semaphore = False
        components.Pool(w).create(w)
        assert w.process_task is w._process_task

    def test_Pool_create(self):
        from kombu.async.semaphore import LaxBoundedSemaphore
        w = Mock()
        w._conninfo.connection_errors = w._conninfo.channel_errors = ()
        w.hub = Mock()

        PoolImp = Mock()
        poolimp = PoolImp.return_value = Mock()
        poolimp._pool = [Mock(), Mock()]
        poolimp._cache = {}
        poolimp._fileno_to_inq = {}
        poolimp._fileno_to_outq = {}

        from celery.concurrency.prefork import TaskPool as _TaskPool

        class MockTaskPool(_TaskPool):
            Pool = PoolImp

            @property
            def timers(self):
                return {Mock(): 30}

        w.pool_cls = MockTaskPool
        w.use_eventloop = True
        w.consumer.restart_count = -1
        pool = components.Pool(w)
        pool.create(w)
        pool.register_with_event_loop(w, w.hub)
        if sys.platform != 'win32':
            assert isinstance(w.semaphore, LaxBoundedSemaphore)
            P = w.pool
            P.start()
