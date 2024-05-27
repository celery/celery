import re
from unittest.mock import Mock, patch

import pytest

import t.skip
from celery.exceptions import ImproperlyConfigured
from celery.worker.components import Beat, Consumer, Hub, Pool, Timer

# some of these are tested in test_worker, so I've only written tests
# here to complete coverage.  Should move everything to this module at some
# point [-ask]


class test_Timer:

    def test_create__eventloop(self):
        w = Mock(name='w')
        w.use_eventloop = True
        Timer(w).create(w)
        assert not w.timer.queue


class test_Hub:

    def setup_method(self):
        self.w = Mock(name='w')
        self.hub = Hub(self.w)
        self.w.hubs = [Mock(name='w.hub')]

    def test_init(self):
        assert self.w.concurrent_readers_delimiter == '|'
        assert self.w.concurrent_readers == 1

    @patch('celery.worker.components.set_event_loop')
    @patch('celery.worker.components.get_event_loop')
    def test_create(self, get_event_loop, set_event_loop):
        self.hub._patch_thread_primitives = Mock(name='ptp')
        assert self.hub.create(self.w) is self.hub
        self.hub._patch_thread_primitives.assert_called_with(self.w)

    def test_start(self):
        self.hub.start(self.w)

    def test_stop(self):
        self.hub.stop(self.w)
        self.w.hubs[0].close.assert_called_with()

    def test_terminate(self):
        self.hub.terminate(self.w)
        self.w.hubs[0].close.assert_called_with()

    def test_is_concurrency_needed(self):
        assert self.hub._is_concurrency_needed(self.w) is False


class test_Hubs:

    def setup_method(self):
        self.w = Mock(name='w')
        self.w.app.conf.broker_url = 'amqp://|amqp://'
        self.hub = Hub(self.w)

    def test_init(self):
        assert self.w.concurrent_readers == 2

    def test_create(self):
        self.hub.create(self.w)
        assert len(self.w.hubs) == 2

    def test_is_concurrency_needed(self):
        assert self.hub._is_concurrency_needed(self.w) is True


class test_Pool:

    def test_close_terminate(self):
        w = Mock()
        comp = Pool(w)
        pool = w.pool = Mock()
        comp.close(w)
        pool.close.assert_called_with()
        comp.terminate(w)
        pool.terminate.assert_called_with()

        w.pool = None
        comp.close(w)
        comp.terminate(w)

    @t.skip.if_win32
    def test_create_when_eventloop(self):
        w = Mock()
        w.use_eventloop = w.pool_putlocks = w.pool_cls.uses_semaphore = True
        w.concurrent_readers = 1
        comp = Pool(w)
        w.pool = Mock()
        comp.create(w)
        assert w.process_task is w._process_task_sem

    def test_create_calls_instantiate_with_max_memory(self):
        w = Mock()
        w.use_eventloop = w.pool_putlocks = w.pool_cls.uses_semaphore = True
        w.concurrent_readers = 1
        comp = Pool(w)
        comp.instantiate = Mock()
        w.max_memory_per_child = 32

        comp.create(w)

        assert comp.instantiate.call_args[1]['max_memory_per_child'] == 32

    def test_threaded_enabled_with_concurrent_readers(self):
        w = Mock()
        w.concurrent_readers = 2
        comp = Pool(w)
        w.pool = Mock()
        comp.create(w)
        assert w.process_task is not w._process_task_sem


class test_Beat:

    def test_create__green(self):
        w = Mock(name='w')
        w.pool_cls.__module__ = 'foo_gevent'
        with pytest.raises(ImproperlyConfigured):
            Beat(w).create(w)


class test_Consumers:

    def setup_method(self):
        self.w = Mock(name='w')
        self.w.app.conf.broker_url = 'amqp://|amqp://|amqp://'
        self.w.concurrent_readers_delimiter = '|'
        self.hub = Hub(self.w)
        self.hub.create(self.w)
        self.consumer = Consumer(self.w)

    def test_create_consumers(self):
        consumers = self.consumer._create_consumers(
            self.w,
            concurrency=2,
            prefetch_multiplier=2,
            consumers_count=3,
        )
        assert len(consumers) == 3

    @patch('threading.Thread.start', autospec=True)
    def test_start(self, mock_thread_start):
        consumers = self.consumer._create_consumers(
            self.w,
            concurrency=2,
            prefetch_multiplier=2,
            consumers_count=3,
        )
        self.consumer.obj = consumers
        returned_threads = self.consumer.start(self.w)
        assert mock_thread_start.call_count == len(consumers)
        assert len(returned_threads) == len(consumers)

    @patch('celery.worker.consumer.Consumer.stop', autospec=True)
    def test_stop(self, mock_consumer_stop):
        consumers = self.consumer._create_consumers(
            self.w,
            concurrency=2,
            prefetch_multiplier=2,
            consumers_count=3,
        )
        self.consumer.obj = consumers
        returned_stopped = self.consumer.stop(self.w)
        assert len(returned_stopped) == len(consumers)

    def test_create_consumers_without_matching_broker_url(self):
        consumers_count = 2
        length = len(self.w.app.conf.broker_url.split(self.w.concurrent_readers_delimiter))
        assert length != consumers_count, "Test setup error"
        error_msg = f"Number of consumers ({consumers_count}) does not match the number of brokers ({length})."
        with pytest.raises(ValueError, match=re.escape(error_msg)):
            self.consumer._create_consumers(
                self.w,
                concurrency=2,
                prefetch_multiplier=2,
                consumers_count=consumers_count,
            )

    def test_distribute_prefetch_no_concurrency(self):
        with pytest.raises(ValueError):
            self.consumer.distribute_prefetch(
                concurrency=0,
                prefetch_multiplier=1,
                consumers_count=1,
            )

    def test_distribute_prefetch_no_prefetch_multiplier(self):
        with pytest.raises(ValueError):
            self.consumer.distribute_prefetch(
                concurrency=1,
                prefetch_multiplier=0,
                consumers_count=1,
            )

    def test_distribute_prefetch_no_consumers_count(self):
        with pytest.raises(ValueError):
            self.consumer.distribute_prefetch(
                concurrency=1,
                prefetch_multiplier=1,
                consumers_count=0,
            )

    @pytest.mark.parametrize('concurrency, prefetch_multiplier, consumers_count, expected', [
        (1, 1, 1, [1]),
        (1, 1, 2, [1, 1]),
        (1, 2, 1, [2]),
        (1, 2, 2, [1, 1]),
        (2, 1, 1, [2]),
        (2, 1, 2, [1, 1]),
        (2, 1, 3, [1, 1, 1]),
        (3, 4, 1, [12]),
        (3, 4, 2, [6, 6]),
        (3, 4, 3, [4, 4, 4]),
        (3, 4, 4, [3, 3, 3, 3]),
        (3, 4, 5, [3, 3, 2, 2, 2]),
        (3, 4, 6, [2, 2, 2, 2, 2, 2]),
        (3, 4, 7, [2, 2, 2, 2, 2, 1, 1]),
        (3, 4, 8, [2, 2, 2, 2, 1, 1, 1, 1]),
        (3, 4, 9, [2, 2, 2, 1, 1, 1, 1, 1, 1]),
        (3, 4, 10, [2, 2, 1, 1, 1, 1, 1, 1, 1, 1]),
        (3, 4, 11, [2, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]),
        (3, 4, 12, [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]),
        (3, 4, 13, [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]),
        (3, 4, 14, [1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1]),
    ])
    def test_distribute_prefetch(self, concurrency, prefetch_multiplier, consumers_count, expected):
        result = self.consumer.distribute_prefetch(concurrency, prefetch_multiplier, consumers_count)
        assert result == expected
