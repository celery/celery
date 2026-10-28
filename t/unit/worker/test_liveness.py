import os
from unittest.mock import Mock, patch

import pytest

from celery.concurrency.prefork import TaskPool as PreforkPool
from celery.exceptions import ImproperlyConfigured
from celery.worker.consumer.consumer import Consumer
from celery.worker.consumer.liveness import Liveness
from celery.worker.consumer.tasks import Tasks


def test_off_by_default(app):
    assert app.conf.worker_liveness_file is None
    assert app.conf.worker_liveness_interval == 30.0


def test_registered_after_the_task_consumer():
    assert 'celery.worker.consumer.liveness:Liveness' in Consumer.Blueprint.default_steps
    assert Liveness.requires == (Tasks,)


class FakeRequest:

    def __init__(self, id, time_limit=None, task_time_limit=None):
        self.id = id
        self.name = 'proj.add'
        self.time_limits = (time_limit, None)
        self.task = Mock(time_limit=task_time_limit)


class LivenessCase:

    @pytest.fixture(autouse=True)
    def setup_conf(self, tmp_path):
        self.path = str(tmp_path / 'alive')
        self.app.conf.worker_liveness_file = self.path
        self.app.conf.worker_liveness_interval = 2.0

    def Consumer(self, driver='amqp', pool=None):
        c = Mock(name='consumer')
        c.app = self.app
        c.hostname = 'w1@example.com'
        c.connection.transport.driver_type = driver
        c.task_consumer.channel = Mock(name='task_channel', is_open=True)
        c.pool = pool if pool is not None else Mock(is_green=False, num_processes=2)
        c.controller.time_limit = None
        return c

    def started(self, driver='amqp', pool=None):
        c = self.Consumer(driver, pool)
        step = Liveness(c)
        with patch('celery.worker.consumer.liveness.Consumer') as consumer, \
                patch('celery.worker.consumer.liveness.Producer') as producer:
            step.start(c)
        return c, step, consumer, producer

    def own_probe(self, step, sent):
        return {'publisher_id': step.publisher_id, 'sent': sent}


class test_Liveness(LivenessCase):

    def test_off_without_a_path(self):
        self.app.conf.worker_liveness_file = None
        c = self.Consumer()
        step = Liveness(c)
        assert not step.include_if(c)

    def test_path_from_setting_expands_node_name(self, tmp_path):
        self.app.conf.worker_liveness_file = str(tmp_path / '%n.alive')
        step = Liveness(self.Consumer())
        assert step.path == str(tmp_path / 'w1.alive')

    def test_path_from_command_line_wins(self, tmp_path):
        step = Liveness(self.Consumer(), liveness_file=str(tmp_path / '%n.cli'))
        assert step.path == str(tmp_path / 'w1.cli')

    @pytest.mark.parametrize('interval', [0, -1.0, None])
    def test_interval_must_be_positive(self, interval):
        self.app.conf.worker_liveness_interval = interval
        with pytest.raises(ImproperlyConfigured):
            Liveness(self.Consumer())
        self.app.conf.worker_liveness_file = None
        Liveness(self.Consumer())  # fine while the check is off

    def test_create_removes_a_leftover_file(self):
        open(self.path, 'w').close()
        c = self.Consumer()
        step = Liveness(c)
        step.create(c)
        assert not os.path.exists(self.path)
        step.create(c)  # no file is fine too

    def test_start_amqp_probe_queue(self):
        c, step, consumer, producer = self.started('amqp')
        queue = consumer.call_args[0][1][0]
        assert queue.name.startswith('w1@example.com.')
        assert queue.name.endswith('.celery.liveness')
        assert queue.exchange.name == ''
        assert queue.routing_key == queue.name
        assert queue.exclusive and queue.auto_delete and not queue.durable
        assert queue.expires == 8.0
        assert queue.message_ttl == 4.0
        consumer.return_value.consume.assert_called_once_with()
        c.timer.call_repeatedly.assert_called_once_with(2.0, step.send_probe)
        producer.return_value.publish.assert_called_once()
        assert producer.return_value.publish.call_args[1]['routing_key'] == queue.name

    def test_probe_consumer_ignores_undecodable_messages(self):
        _, step, consumer, _ = self.started()
        assert consumer.call_args[1]['on_decode_error'] == step.on_decode_error
        step.on_decode_error(Mock(), ValueError('not json'))

    def test_start_redis_probe_uses_pubsub(self):
        c, step, consumer, producer = self.started('redis')
        queue = consumer.call_args[0][1][0]
        assert queue.exchange.type == 'fanout'
        assert queue.exchange.name == queue.name
        assert queue.expires == 8.0
        assert queue.message_ttl is None
        # The first probe waits until the channel has subscribed.
        c.timer.call_after.assert_called_once_with(1.0, step.send_probe)
        producer.return_value.publish.assert_not_called()

    def test_start_other_transport_uses_the_timer(self):
        c, step, consumer, producer = self.started('sqs')
        consumer.assert_not_called()
        c.timer.call_repeatedly.assert_called_once_with(2.0, step.check)
        assert os.path.exists(self.path)

    def test_queue_name_differs_per_connection(self):
        c, step, consumer1, _ = self.started()
        step.stop(c)
        with patch('celery.worker.consumer.liveness.Consumer') as consumer2, \
                patch('celery.worker.consumer.liveness.Producer'):
            step.start(c)
        assert consumer1.call_args[0][1][0].name != consumer2.call_args[0][1][0].name
        _, other, _, _ = self.started()
        assert other.publisher_id != step.publisher_id

    def test_no_probe_when_the_timer_is_a_thread(self):
        c = self.Consumer('amqp')
        c.hub = None  # no event loop: the timer runs in its own thread
        step = Liveness(c)
        with patch('celery.worker.consumer.liveness.Consumer') as consumer:
            step.start(c)
        consumer.assert_not_called()
        c.timer.call_repeatedly.assert_called_once_with(2.0, step.check)

    def test_probe_on_a_green_pool(self):
        c = self.Consumer('amqp', pool=Mock(is_green=True, num_processes=2))
        c.hub = None  # green pools use their own timer greenlets
        step = Liveness(c)
        with patch('celery.worker.consumer.liveness.Consumer') as consumer, \
                patch('celery.worker.consumer.liveness.Producer'):
            step.start(c)
        consumer.return_value.consume.assert_called_once_with()

    def test_on_probe_touches_the_file(self):
        _, step, _, _ = self.started()
        with patch('celery.worker.consumer.liveness.monotonic', return_value=100.0):
            step.on_probe(self.own_probe(step, 99.0), Mock())
        assert os.path.exists(self.path)

    @pytest.mark.parametrize('body', [
        {'publisher_id': 'someone else', 'sent': 99.0},
        {'sent': 99.0},
        ['not', 'a', 'probe'],
        'not a probe',
    ])
    def test_on_probe_ignores_other_publishers(self, body):
        _, step, _, _ = self.started()
        with patch('celery.worker.consumer.liveness.monotonic', return_value=100.0):
            step.on_probe(body, Mock())
        assert not os.path.exists(self.path)

    @pytest.mark.parametrize('sent', ['99.0', None, [99.0]])
    def test_on_probe_ignores_a_bad_send_time(self, sent):
        _, step, _, _ = self.started()
        with patch('celery.worker.consumer.liveness.monotonic', return_value=100.0):
            step.on_probe({'publisher_id': step.publisher_id, 'sent': sent}, Mock())
        assert not os.path.exists(self.path)

    def test_on_probe_ignores_an_old_probe(self):
        _, step, _, _ = self.started()
        with patch('celery.worker.consumer.liveness.monotonic', return_value=100.0):
            step.on_probe(self.own_probe(step, 95.9), Mock())
        assert not os.path.exists(self.path)

    @pytest.mark.parametrize('channel', [
        Mock(is_open=False),
        Mock(spec=['closed'], closed=True),
        None,
    ])
    def test_no_touch_while_the_task_channel_is_closed(self, channel):
        c, step, _, _ = self.started()
        c.task_consumer.channel = channel
        with patch('celery.worker.consumer.liveness.monotonic', return_value=100.0):
            step.on_probe(self.own_probe(step, 99.0), Mock())
        assert not os.path.exists(self.path)

    def test_no_touch_without_a_task_consumer(self):
        c, step, _, _ = self.started()
        c.task_consumer = None
        step.check()
        assert not os.path.exists(self.path)

    def test_virtual_transport_channel_open(self):
        c, step, _, _ = self.started('sqs')
        os.unlink(self.path)
        c.task_consumer.channel = Mock(spec=['closed'], closed=False)
        step.check()
        assert os.path.exists(self.path)

    def test_send_probe_survives_a_broker_error(self):
        _, step, _, producer = self.started()
        producer.return_value.publish.side_effect = ConnectionError()
        step.send_probe()

    def test_stop(self):
        c, step, consumer, _ = self.started('redis')
        tref, first_tref, channel = step.tref, step.first_tref, step.channel
        step.stop(c)
        tref.cancel.assert_called_once_with()
        first_tref.cancel.assert_called_once_with()
        consumer.return_value.cancel.assert_called_once_with()
        channel.close.assert_called_once_with()
        assert step.channel is None and step.consumer is None
        step.stop(c)  # a second stop is fine

    @pytest.mark.parametrize('is_green,running,waiting,touched', [
        (True, 2, 1, True),  # a task waits for a full green pool: busy
        (True, 2, 0, False),  # full but nothing waits: the probe must come back
        (True, 1, 1, False),  # a free slot: the probe must come back
        (False, 2, 1, False),  # prefork doesn't block the event loop
    ])
    def test_task_waiting_for_a_full_green_pool_counts_as_busy(self, is_green, running, waiting, touched):
        _, step, _, _ = self.started(pool=Mock(is_green=is_green, num_processes=2))
        active = {FakeRequest(f'run{i}') for i in range(running)}
        reserved = active | {FakeRequest(f'wait{i}') for i in range(waiting)}
        with patch('celery.worker.state.active_requests', active), \
                patch('celery.worker.state.reserved_requests', reserved):
            step.send_probe()
        assert os.path.exists(self.path) == touched

    @pytest.mark.skipif(not hasattr(os, 'O_NOFOLLOW'), reason='needs O_NOFOLLOW')
    def test_touch_does_not_follow_a_symlink(self, tmp_path):
        _, step, _, _ = self.started()
        target = tmp_path / 'target'
        os.symlink(target, self.path)
        with patch('celery.worker.consumer.liveness.warning') as warning:
            step.touch()
        warning.assert_called_once()
        assert not target.exists()

    @pytest.mark.skipif(not hasattr(os, 'mkfifo'), reason='needs mkfifo')
    def test_touch_does_not_block_on_a_fifo(self):
        _, step, _, _ = self.started()
        os.mkfifo(self.path)
        with patch('celery.worker.consumer.liveness.warning') as warning:
            step.touch()  # would block forever without O_NONBLOCK
        warning.assert_called_once()

    def test_touch_updates_an_existing_file(self):
        _, step, _, _ = self.started()
        open(self.path, 'w').close()
        os.utime(self.path, (1000, 1000))
        step.touch()
        assert os.stat(self.path).st_mtime > 1000

    def test_touch_failure_is_logged_once(self, tmp_path):
        c, step, _, _ = self.started()
        step.path = str(tmp_path / 'missing' / 'alive')
        with patch('celery.worker.consumer.liveness.warning') as warning:
            step.touch()
            step.touch()
            warning.assert_called_once()
            os.mkdir(tmp_path / 'missing')
            step.touch()
            step.path = str(tmp_path / 'missing' / 'gone' / 'alive')
            step.touch()
            assert warning.call_count == 2


class test_Liveness_time_limits(LivenessCase):

    def check_limits(self, step, requests, now):
        with patch('celery.worker.state.active_requests', set(requests)), \
                patch('celery.worker.consumer.liveness.monotonic', return_value=now):
            step.check_time_limits()

    def prefork_step(self):
        c, step, _, _ = self.started(pool=Mock(spec=PreforkPool, is_green=False, num_processes=2))
        assert step.enforces_limits
        return c, step

    @pytest.mark.parametrize('limits,worker_limit', [
        ({'time_limit': 10}, None),
        ({'task_time_limit': 10}, None),
        ({}, 10),
    ])
    def test_task_past_its_hard_limit_fails_the_check(self, limits, worker_limit):
        c, step = self.prefork_step()
        c.controller.time_limit = worker_limit
        request = FakeRequest('id1', **limits)
        self.check_limits(step, [request], now=100.0)
        self.check_limits(step, [request], now=111.9)  # within limit + one interval
        assert step.healthy()
        with patch('celery.worker.consumer.liveness.warning') as warning:
            self.check_limits(step, [request], now=112.1)
            self.check_limits(step, [request], now=114.0)
        warning.assert_called_once()
        assert not step.healthy()
        self.check_limits(step, [], now=116.0)  # the task finished
        assert step.healthy()

    def test_task_without_a_limit_is_fine(self):
        _, step = self.prefork_step()
        request = FakeRequest('id1')
        self.check_limits(step, [request], now=100.0)
        self.check_limits(step, [request], now=100000.0)
        assert step.healthy()

    def test_only_prefork_enforces_limits(self):
        _, step, _, _ = self.started(pool=Mock(is_green=False, num_processes=2))
        assert not step.enforces_limits
        request = FakeRequest('id1', time_limit=1)
        self.check_limits(step, [request], now=100.0)
        self.check_limits(step, [request], now=200.0)
        assert step.healthy()

    def test_running_time_survives_a_reconnect(self):
        c, step = self.prefork_step()
        request = FakeRequest('id1', time_limit=10)
        with patch('celery.worker.state.active_requests', {request}):
            self.check_limits(step, [request], now=100.0)
            step.stop(c)
            # start() sends a probe right away, which checks the limits too.
            with patch('celery.worker.consumer.liveness.Consumer'), \
                    patch('celery.worker.consumer.liveness.Producer'), \
                    patch('celery.worker.consumer.liveness.monotonic', return_value=105.0):
                step.start(c)
            self.check_limits(step, [request], now=113.0)
        assert not step.healthy()

    def test_requests_changing_on_another_thread(self):
        _, step = self.prefork_step()
        step.overrun = 'proj.add[id1]'

        class Changing:
            def __iter__(self):
                raise RuntimeError('Set changed size during iteration')

        with patch('celery.worker.state.active_requests', Changing()):
            step.check_time_limits()
        assert step.overrun == 'proj.add[id1]'  # unchanged until next time
