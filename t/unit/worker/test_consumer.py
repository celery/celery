import errno
import socket
from collections import deque

import pytest
from billiard.exceptions import RestartFreqExceeded
from case import ContextMock, Mock, call, patch

from celery.utils.collections import LimitedSet
from celery.worker.consumer.agent import Agent
from celery.worker.consumer.consumer import CLOSE, TERMINATE, Consumer
from celery.worker.consumer.gossip import Gossip
from celery.worker.consumer.heart import Heart
from celery.worker.consumer.mingle import Mingle
from celery.worker.consumer.tasks import Tasks


class test_Consumer:

    def get_consumer(self, no_hub=False, **kwargs):
        consumer = Consumer(
            on_task_request=Mock(),
            init_callback=Mock(),
            pool=Mock(),
            app=self.app,
            timer=Mock(),
            controller=Mock(),
            hub=None if no_hub else Mock(),
            **kwargs
        )
        consumer.blueprint = Mock(name='blueprint')
        consumer._restart_state = Mock(name='_restart_state')
        consumer.connection = _amqp_connection()
        consumer.connection_errors = (socket.error, OSError,)
        consumer.conninfo = consumer.connection
        return consumer

    def test_repr(self):
        assert repr(self.get_consumer())

    def test_taskbuckets_defaultdict(self):
        c = self.get_consumer()
        assert c.task_buckets['fooxasdwx.wewe'] is None

    def test_sets_heartbeat(self):
        c = self.get_consumer(amqheartbeat=10)
        assert c.amqheartbeat == 10
        self.app.conf.broker_heartbeat = 20
        c = self.get_consumer(amqheartbeat=None)
        assert c.amqheartbeat == 20

    def test_gevent_bug_disables_connection_timeout(self):
        with patch('celery.worker.consumer.consumer._detect_environment') as d:
            d.return_value = 'gevent'
            self.app.conf.broker_connection_timeout = 33.33
            self.get_consumer()
            assert self.app.conf.broker_connection_timeout is None

    def test_limit_moved_to_pool(self):
        with patch('celery.worker.consumer.consumer.task_reserved') as reserv:
            c = self.get_consumer()
            c.on_task_request = Mock(name='on_task_request')
            request = Mock(name='request')
            c._limit_move_to_pool(request)
            reserv.assert_called_with(request)
            c.on_task_request.assert_called_with(request)

    def test_update_prefetch_count(self):
        c = self.get_consumer()
        c._update_qos_eventually = Mock(name='update_qos')
        c.initial_prefetch_count = None
        c.pool.num_processes = None
        c.prefetch_multiplier = 10
        assert c._update_prefetch_count(1) is None
        c.initial_prefetch_count = 10
        c.pool.num_processes = 10
        c._update_prefetch_count(8)
        c._update_qos_eventually.assert_called_with(8)
        assert c.initial_prefetch_count == 10 * 10

    def test_flush_events(self):
        c = self.get_consumer()
        c.event_dispatcher = None
        c._flush_events()
        c.event_dispatcher = Mock(name='evd')
        c._flush_events()
        c.event_dispatcher.flush.assert_called_with()

    def test_on_send_event_buffered(self):
        c = self.get_consumer()
        c.hub = None
        c.on_send_event_buffered()
        c.hub = Mock(name='hub')
        c.on_send_event_buffered()
        c.hub._ready.add.assert_called_with(c._flush_events)

    def test_schedule_bucket_request(self):
        c = self.get_consumer()
        c.timer = Mock()

        bucket = Mock()
        request = Mock()
        bucket.pop = lambda: bucket.contents.popleft()
        bucket.can_consume.return_value = True
        bucket.contents = deque()

        with patch(
            'celery.worker.consumer.consumer.Consumer._limit_move_to_pool'
        ) as reserv:
            bucket.contents.append((request, 3))
            c._schedule_bucket_request(bucket)
            bucket.can_consume.assert_called_with(3)
            reserv.assert_called_with(request)

        bucket.can_consume.return_value = False
        bucket.contents = deque()
        bucket.expected_time.return_value = 3.33
        bucket.contents.append((request, 4))
        limit_order = c._limit_order
        c._schedule_bucket_request(bucket)
        assert c._limit_order == limit_order + 1
        bucket.can_consume.assert_called_with(4)
        c.timer.call_after.assert_called_with(
            3.33, c._schedule_bucket_request, (bucket,),
            priority=c._limit_order,
        )
        bucket.expected_time.assert_called_with(4)
        assert bucket.pop() == (request, 4)

        bucket.contents = deque()
        bucket.can_consume.reset_mock()
        c._schedule_bucket_request(bucket)
        bucket.can_consume.assert_not_called()

    def test_limit_task(self):
        c = self.get_consumer()
        bucket = Mock()
        request = Mock()

        with patch(
            'celery.worker.consumer.consumer.Consumer._schedule_bucket_request'
        ) as reserv:
            c._limit_task(request, bucket, 1)
            bucket.add.assert_called_with((request, 1))
            reserv.assert_called_with(bucket)

    def test_post_eta(self):
        c = self.get_consumer()
        c.qos = Mock()
        bucket = Mock()
        request = Mock()

        with patch(
            'celery.worker.consumer.consumer.Consumer._schedule_bucket_request'
        ) as reserv:
            c._limit_post_eta(request, bucket, 1)
            c.qos.decrement_eventually.assert_called_with()
            bucket.add.assert_called_with((request, 1))
            reserv.assert_called_with(bucket)

    def test_start_blueprint_raises_EMFILE(self):
        c = self.get_consumer()
        exc = c.blueprint.start.side_effect = OSError()
        exc.errno = errno.EMFILE

        with pytest.raises(OSError):
            c.start()

    def test_max_restarts_exceeded(self):
        c = self.get_consumer()

        def se(*args, **kwargs):
            c.blueprint.state = CLOSE
            raise RestartFreqExceeded()
        c._restart_state.step.side_effect = se
        c.blueprint.start.side_effect = socket.error()

        with patch('celery.worker.consumer.consumer.sleep') as sleep:
            c.start()
            sleep.assert_called_with(1)

    def test_do_not_restart_when_closed(self):
        c = self.get_consumer()

        c.blueprint.state = None

        def bp_start(*args, **kwargs):
            c.blueprint.state = CLOSE

        c.blueprint.start.side_effect = bp_start
        with patch('celery.worker.consumer.consumer.sleep'):
            c.start()

        c.blueprint.start.assert_called_once_with(c)

    def test_do_not_restart_when_terminated(self):
        c = self.get_consumer()

        c.blueprint.state = None

        def bp_start(*args, **kwargs):
            c.blueprint.state = TERMINATE

        c.blueprint.start.side_effect = bp_start

        with patch('celery.worker.consumer.consumer.sleep'):
            c.start()

        c.blueprint.start.assert_called_once_with(c)

    def test_no_retry_raises_error(self):
        self.app.conf.broker_connection_retry = False
        c = self.get_consumer()
        c.blueprint.start.side_effect = socket.error()
        with pytest.raises(socket.error):
            c.start()

    def _closer(self, c):
        def se(*args, **kwargs):
            c.blueprint.state = CLOSE
        return se

    def test_collects_at_restart(self):
        c = self.get_consumer()
        c.connection.collect.side_effect = MemoryError()
        c.blueprint.start.side_effect = socket.error()
        c.blueprint.restart.side_effect = self._closer(c)
        c.start()
        c.connection.collect.assert_called_with()

    def test_register_with_event_loop(self):
        c = self.get_consumer()
        c.register_with_event_loop(Mock(name='loop'))

    def test_on_close_clears_semaphore_timer_and_reqs(self):
        with patch('celery.worker.consumer.consumer.reserved_requests') as res:
            c = self.get_consumer()
            c.on_close()
            c.controller.semaphore.clear.assert_called_with()
            c.timer.clear.assert_called_with()
            res.clear.assert_called_with()
            c.pool.flush.assert_called_with()

            c.controller = None
            c.timer = None
            c.pool = None
            c.on_close()

    def test_connect_error_handler(self):
        self.app._connection = _amqp_connection()
        conn = self.app._connection.return_value
        c = self.get_consumer()
        assert c.connect()
        conn.ensure_connection.assert_called()
        errback = conn.ensure_connection.call_args[0][0]
        errback(Mock(), 0)

    @patch('celery.worker.consumer.consumer.error')
    def test_connect_error_handler_progress(self, error):
        self.app.conf.broker_connection_retry = True
        self.app.conf.broker_connection_max_retries = 3
        self.app._connection = _amqp_connection()
        conn = self.app._connection.return_value
        c = self.get_consumer()
        assert c.connect()
        errback = conn.ensure_connection.call_args[0][0]
        errback(Mock(), 2)
        assert error.call_args[0][3] == 'Trying again in 2.00 seconds... (1/3)'
        errback(Mock(), 4)
        assert error.call_args[0][3] == 'Trying again in 4.00 seconds... (2/3)'
        errback(Mock(), 6)
        assert error.call_args[0][3] == 'Trying again in 6.00 seconds... (3/3)'


class test_Heart:

    def test_start(self):
        c = Mock()
        c.timer = Mock()
        c.event_dispatcher = Mock()

        with patch('celery.worker.heartbeat.Heart') as hcls:
            h = Heart(c)
            assert h.enabled
            assert h.heartbeat_interval is None
            assert c.heart is None

            h.start(c)
            assert c.heart
            hcls.assert_called_with(c.timer, c.event_dispatcher,
                                    h.heartbeat_interval)
            c.heart.start.assert_called_with()

    def test_start_heartbeat_interval(self):
        c = Mock()
        c.timer = Mock()
        c.event_dispatcher = Mock()

        with patch('celery.worker.heartbeat.Heart') as hcls:
            h = Heart(c, False, 20)
            assert h.enabled
            assert h.heartbeat_interval == 20
            assert c.heart is None

            h.start(c)
            assert c.heart
            hcls.assert_called_with(c.timer, c.event_dispatcher,
                                    h.heartbeat_interval)
            c.heart.start.assert_called_with()


class test_Tasks:

    def test_stop(self):
        c = Mock()
        tasks = Tasks(c)
        assert c.task_consumer is None
        assert c.qos is None

        c.task_consumer = Mock()
        tasks.stop(c)

    def test_stop_already_stopped(self):
        c = Mock()
        tasks = Tasks(c)
        tasks.stop(c)


class test_Agent:

    def test_start(self):
        c = Mock()
        agent = Agent(c)
        agent.instantiate = Mock()
        agent.agent_cls = 'foo:Agent'
        assert agent.create(c) is not None
        agent.instantiate.assert_called_with(agent.agent_cls, c.connection)


class test_Mingle:

    def test_start_no_replies(self):
        c = Mock()
        c.app.connection_for_read = _amqp_connection()
        mingle = Mingle(c)
        I = c.app.control.inspect.return_value = Mock()
        I.hello.return_value = {}
        mingle.start(c)

    def test_start(self):
        c = Mock()
        c.app.connection_for_read = _amqp_connection()
        mingle = Mingle(c)
        assert mingle.enabled

        Aig = LimitedSet()
        Big = LimitedSet()
        Aig.add('Aig-1')
        Aig.add('Aig-2')
        Big.add('Big-1')

        I = c.app.control.inspect.return_value = Mock()
        I.hello.return_value = {
            'A@example.com': {
                'clock': 312,
                'revoked': Aig._data,
            },
            'B@example.com': {
                'clock': 29,
                'revoked': Big._data,
            },
            'C@example.com': {
                'error': 'unknown method',
            },
        }

        our_revoked = c.controller.state.revoked = LimitedSet()

        mingle.start(c)
        I.hello.assert_called_with(c.hostname, our_revoked._data)
        c.app.clock.adjust.assert_has_calls([
            call(312), call(29),
        ], any_order=True)
        assert 'Aig-1' in our_revoked
        assert 'Aig-2' in our_revoked
        assert 'Big-1' in our_revoked


def _amqp_connection():
    connection = ContextMock(name='Connection')
    connection.return_value = ContextMock(name='connection')
    connection.return_value.transport.driver_type = 'amqp'
    return connection


class test_Gossip:

    def test_init(self):
        c = self.Consumer()
        c.app.connection_for_read = _amqp_connection()
        g = Gossip(c)
        assert g.enabled
        assert c.gossip is g

    def test_callbacks(self):
        c = self.Consumer()
        c.app.connection_for_read = _amqp_connection()
        g = Gossip(c)
        on_node_join = Mock(name='on_node_join')
        on_node_join2 = Mock(name='on_node_join2')
        on_node_leave = Mock(name='on_node_leave')
        on_node_lost = Mock(name='on.node_lost')
        g.on.node_join.add(on_node_join)
        g.on.node_join.add(on_node_join2)
        g.on.node_leave.add(on_node_leave)
        g.on.node_lost.add(on_node_lost)

        worker = Mock(name='worker')
        g.on_node_join(worker)
        on_node_join.assert_called_with(worker)
        on_node_join2.assert_called_with(worker)
        g.on_node_leave(worker)
        on_node_leave.assert_called_with(worker)
        g.on_node_lost(worker)
        on_node_lost.assert_called_with(worker)

    def test_election(self):
        c = self.Consumer()
        c.app.connection_for_read = _amqp_connection()
        g = Gossip(c)
        g.start(c)
        g.election('id', 'topic', 'action')
        assert g.consensus_replies['id'] == []
        g.dispatcher.send.assert_called_with(
            'worker-elect', id='id', topic='topic', cver=1, action='action',
        )

    def test_call_task(self):
        c = self.Consumer()
        c.app.connection_for_read = _amqp_connection()
        g = Gossip(c)
        g.start(c)
        signature = g.app.signature = Mock(name='app.signature')
        task = Mock()
        g.call_task(task)
        signature.assert_called_with(task)
        signature.return_value.apply_async.assert_called_with()

        signature.return_value.apply_async.side_effect = MemoryError()
        with patch('celery.worker.consumer.gossip.logger') as logger:
            g.call_task(task)
            logger.exception.assert_called()

    def Event(self, id='id', clock=312,
              hostname='foo@example.com', pid=4312,
              topic='topic', action='action', cver=1):
        return {
            'id': id,
            'clock': clock,
            'hostname': hostname,
            'pid': pid,
            'topic': topic,
            'action': action,
            'cver': cver,
        }

    def test_on_elect(self):
        c = self.Consumer()
        c.app.connection_for_read = _amqp_connection()
        g = Gossip(c)
        g.start(c)

        event = self.Event('id1')
        g.on_elect(event)
        in_heap = g.consensus_requests['id1']
        assert in_heap
        g.dispatcher.send.assert_called_with('worker-elect-ack', id='id1')

        event.pop('clock')
        with patch('celery.worker.consumer.gossip.logger') as logger:
            g.on_elect(event)
            logger.exception.assert_called()

    def Consumer(self, hostname='foo@x.com', pid=4312):
        c = Mock()
        c.app.connection = _amqp_connection()
        c.hostname = hostname
        c.pid = pid
        return c

    def setup_election(self, g, c):
        g.start(c)
        g.clock = self.app.clock
        assert 'idx' not in g.consensus_replies
        assert g.on_elect_ack({'id': 'idx'}) is None

        g.state.alive_workers.return_value = [
            'foo@x.com', 'bar@x.com', 'baz@x.com',
        ]
        g.consensus_replies['id1'] = []
        g.consensus_requests['id1'] = []
        e1 = self.Event('id1', 1, 'foo@x.com')
        e2 = self.Event('id1', 2, 'bar@x.com')
        e3 = self.Event('id1', 3, 'baz@x.com')
        g.on_elect(e1)
        g.on_elect(e2)
        g.on_elect(e3)
        assert len(g.consensus_requests['id1']) == 3

        with patch('celery.worker.consumer.gossip.info'):
            g.on_elect_ack(e1)
            assert len(g.consensus_replies['id1']) == 1
            g.on_elect_ack(e2)
            assert len(g.consensus_replies['id1']) == 2
            g.on_elect_ack(e3)
            with pytest.raises(KeyError):
                g.consensus_replies['id1']

    def test_on_elect_ack_win(self):
        c = self.Consumer(hostname='foo@x.com')  # I will win
        c.app.connection_for_read = _amqp_connection()
        g = Gossip(c)
        handler = g.election_handlers['topic'] = Mock()
        self.setup_election(g, c)
        handler.assert_called_with('action')

    def test_on_elect_ack_lose(self):
        c = self.Consumer(hostname='bar@x.com')  # I will lose
        c.app.connection_for_read = _amqp_connection()
        g = Gossip(c)
        handler = g.election_handlers['topic'] = Mock()
        self.setup_election(g, c)
        handler.assert_not_called()

    def test_on_elect_ack_win_but_no_action(self):
        c = self.Consumer(hostname='foo@x.com')  # I will win
        c.app.connection_for_read = _amqp_connection()
        g = Gossip(c)
        g.election_handlers = {}
        with patch('celery.worker.consumer.gossip.logger') as logger:
            self.setup_election(g, c)
            logger.exception.assert_called()

    def test_on_node_join(self):
        c = self.Consumer()
        c.app.connection_for_read = _amqp_connection()
        g = Gossip(c)
        with patch('celery.worker.consumer.gossip.debug') as debug:
            g.on_node_join(c)
            debug.assert_called_with('%s joined the party', 'foo@x.com')

    def test_on_node_leave(self):
        c = self.Consumer()
        c.app.connection_for_read = _amqp_connection()
        g = Gossip(c)
        with patch('celery.worker.consumer.gossip.debug') as debug:
            g.on_node_leave(c)
            debug.assert_called_with('%s left', 'foo@x.com')

    def test_on_node_lost(self):
        c = self.Consumer()
        c.app.connection_for_read = _amqp_connection()
        g = Gossip(c)
        with patch('celery.worker.consumer.gossip.info') as info:
            g.on_node_lost(c)
            info.assert_called_with('missed heartbeat from %s', 'foo@x.com')

    def test_register_timer(self):
        c = self.Consumer()
        c.app.connection_for_read = _amqp_connection()
        g = Gossip(c)
        g.register_timer()
        c.timer.call_repeatedly.assert_called_with(g.interval, g.periodic)
        tref = g._tref
        g.register_timer()
        tref.cancel.assert_called_with()

    def test_periodic(self):
        c = self.Consumer()
        c.app.connection_for_read = _amqp_connection()
        g = Gossip(c)
        g.on_node_lost = Mock()
        state = g.state = Mock()
        worker = Mock()
        state.workers = {'foo': worker}
        worker.alive = True
        worker.hostname = 'foo'
        g.periodic()

        worker.alive = False
        g.periodic()
        g.on_node_lost.assert_called_with(worker)
        with pytest.raises(KeyError):
            state.workers['foo']

    def test_on_message__task(self):
        c = self.Consumer()
        c.app.connection_for_read = _amqp_connection()
        g = Gossip(c)
        assert g.enabled
        message = Mock(name='message')
        message.delivery_info = {'routing_key': 'task.failed'}
        g.on_message(Mock(name='prepare'), message)

    def test_on_message(self):
        c = self.Consumer()
        c.app.connection_for_read = _amqp_connection()
        g = Gossip(c)
        assert g.enabled
        prepare = Mock()
        prepare.return_value = 'worker-online', {}
        c.app.events.State.assert_called_with(
            on_node_join=g.on_node_join,
            on_node_leave=g.on_node_leave,
            max_tasks_in_memory=1,
        )
        g.update_state = Mock()
        worker = Mock()
        g.on_node_join = Mock()
        g.on_node_leave = Mock()
        g.update_state.return_value = worker, 1
        message = Mock()
        message.delivery_info = {'routing_key': 'worker-online'}
        message.headers = {'hostname': 'other'}

        handler = g.event_handlers['worker-online'] = Mock()
        g.on_message(prepare, message)
        handler.assert_called_with(message.payload)
        g.event_handlers = {}

        g.on_message(prepare, message)

        message.delivery_info = {'routing_key': 'worker-offline'}
        prepare.return_value = 'worker-offline', {}
        g.on_message(prepare, message)

        message.delivery_info = {'routing_key': 'worker-baz'}
        prepare.return_value = 'worker-baz', {}
        g.update_state.return_value = worker, 0
        g.on_message(prepare, message)

        message.headers = {'hostname': g.hostname}
        g.on_message(prepare, message)
        g.clock.forward.assert_called_with()
