import os
import re
from datetime import datetime, timedelta
from time import sleep
from unittest.mock import ANY

import pytest

from celery.utils.nodenames import anon_nodename

from .tasks import add, sleeping

NODENAME = anon_nodename()

_flaky = pytest.mark.flaky(reruns=5, reruns_delay=2)
_timeout = pytest.mark.timeout(timeout=300)


def flaky(fn):
    return _timeout(_flaky(fn))


@pytest.fixture()
def inspect(manager):
    return manager.app.control.inspect()


class test_Inspect:
    """Integration tests fo app.control.inspect() API"""

    @flaky
    def test_ping(self, inspect):
        """Tests pinging the worker"""
        ret = inspect.ping()
        assert len(ret) == 1
        assert ret[NODENAME] == {'ok': 'pong'}
        # TODO: Check ping() is returning None after stopping worker.
        # This is tricky since current test suite does not support stopping of
        # the worker.

    @flaky
    def test_clock(self, inspect):
        """Tests getting clock information from worker"""
        ret = inspect.clock()
        assert len(ret) == 1
        assert ret[NODENAME]['clock'] > 0

    @flaky
    def test_registered(self, inspect):
        """Tests listing registered tasks"""
        # TODO: We can check also the exact values of the registered methods
        ret = inspect.registered()
        assert len(ret) == 1
        len(ret[NODENAME]) > 0
        for task_name in ret[NODENAME]:
            assert isinstance(task_name, str)

        ret = inspect.registered('name')
        for task_info in ret[NODENAME]:
            # task_info is in form 'TASK_NAME [name=TASK_NAME]'
            assert re.fullmatch(r'\S+ \[name=\S+\]', task_info)

    @flaky
    def test_active_queues(self, inspect):
        """Tests listing active queues"""
        ret = inspect.active_queues()
        assert len(ret) == 1
        assert ret[NODENAME] == [
            {
                'alias': None,
                'auto_delete': False,
                'binding_arguments': None,
                'bindings': [],
                'consumer_arguments': None,
                'durable': True,
                'exchange': {
                    'arguments': None,
                    'auto_delete': False,
                    'delivery_mode': None,
                    'durable': True,
                    'name': 'celery',
                    'no_declare': False,
                    'passive': False,
                    'type': 'direct'
                },
                'exclusive': False,
                'expires': None,
                'max_length': None,
                'max_length_bytes': None,
                'max_priority': None,
                'message_ttl': None,
                'name': 'celery',
                'no_ack': False,
                'no_declare': None,
                'queue_arguments': None,
                'routing_key': 'celery'}
        ]

    @flaky
    def test_active(self, inspect):
        """Tests listing active tasks"""
        res = sleeping.delay(5)
        sleep(1)
        ret = inspect.active()
        assert len(ret) == 1
        assert ret[NODENAME] == [
            {
                'id': res.task_id,
                'name': 't.integration.tasks.sleeping',
                'args': [5],
                'kwargs': {},
                'type': 't.integration.tasks.sleeping',
                'hostname': ANY,
                'time_start': ANY,
                'acknowledged': True,
                'delivery_info': {
                    'exchange': '',
                    'routing_key': 'celery',
                    'priority': 0,
                    'redelivered': False
                },
                'worker_pid': ANY
            }
        ]

    @flaky
    def test_scheduled(self, inspect):
        """Tests listing scheduled tasks"""
        exec_time = datetime.utcnow() + timedelta(seconds=5)
        res = add.apply_async([1, 2], {'z': 3}, eta=exec_time)
        ret = inspect.scheduled()
        assert len(ret) == 1
        assert ret[NODENAME] == [
            {
                'eta': exec_time.strftime('%Y-%m-%dT%H:%M:%S.%f') + '+00:00',
                'priority': 6,
                'request': {
                    'id': res.task_id,
                    'name': 't.integration.tasks.add',
                    'args': [1, 2],
                    'kwargs': {'z': 3},
                    'type': 't.integration.tasks.add',
                    'hostname': ANY,
                    'time_start': None,
                    'acknowledged': False,
                    'delivery_info': {
                        'exchange': '',
                        'routing_key': 'celery',
                        'priority': 0,
                        'redelivered': False
                    },
                    'worker_pid': None
                }
            }
        ]

    @flaky
    def test_query_task(self, inspect):
        """Task that does not exist or is finished"""
        ret = inspect.query_task('d08b257e-a7f1-4b92-9fea-be911441cb2a')
        assert len(ret) == 1
        assert ret[NODENAME] == {}

        # Task in progress
        res = sleeping.delay(5)
        sleep(1)
        ret = inspect.query_task(res.task_id)
        assert len(ret) == 1
        assert ret[NODENAME] == {
            res.task_id: [
                'active', {
                    'id': res.task_id,
                    'name': 't.integration.tasks.sleeping',
                    'args': [5],
                    'kwargs': {},
                    'type': 't.integration.tasks.sleeping',
                    'hostname': NODENAME,
                    'time_start': ANY,
                    'acknowledged': True,
                    'delivery_info': {
                        'exchange': '',
                        'routing_key': 'celery',
                        'priority': 0,
                        'redelivered': False
                    },
                    # worker is running in the same process as separate thread
                    'worker_pid': ANY
                }
            ]
        }

    @flaky
    def test_stats(self, inspect):
        """tests fetching statistics"""
        ret = inspect.stats()
        assert len(ret) == 1
        assert ret[NODENAME]['pool']['max-concurrency'] == 1
        assert len(ret[NODENAME]['pool']['processes']) == 1
        assert ret[NODENAME]['uptime'] > 0
        # worker is running in the same process as separate thread
        assert ret[NODENAME]['pid'] == os.getpid()

    @flaky
    def test_report(self, inspect):
        """Tests fetching report"""
        ret = inspect.report()
        assert len(ret) == 1
        assert ret[NODENAME] == {'ok': ANY}

    @flaky
    def test_revoked(self, inspect):
        """Testing revoking of task"""
        # Fill the queue with tasks to fill the queue
        for _ in range(4):
            sleeping.delay(2)
        # Execute task and revoke it
        result = add.apply_async((1, 1))
        result.revoke()
        ret = inspect.revoked()
        assert len(ret) == 1
        assert result.task_id in ret[NODENAME]

    @flaky
    def test_conf(self, inspect):
        """Tests getting configuration"""
        ret = inspect.conf()
        assert len(ret) == 1
        assert ret[NODENAME]['worker_hijack_root_logger'] == ANY
        assert ret[NODENAME]['worker_log_color'] == ANY
        assert ret[NODENAME]['accept_content'] == ANY
        assert ret[NODENAME]['enable_utc'] == ANY
        assert ret[NODENAME]['timezone'] == ANY
        assert ret[NODENAME]['broker_url'] == ANY
        assert ret[NODENAME]['result_backend'] == ANY
        assert ret[NODENAME]['broker_heartbeat'] == ANY
        assert ret[NODENAME]['deprecated_settings'] == ANY
        assert ret[NODENAME]['include'] == ANY
