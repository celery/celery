from __future__ import absolute_import, unicode_literals

from datetime import datetime
from pickle import dumps, loads

import pytest

from case import Mock, mock
from celery import states
from celery.exceptions import ImproperlyConfigured
from celery.utils.objects import Bunch

CASSANDRA_MODULES = ['cassandra', 'cassandra.auth', 'cassandra.cluster']


@mock.module(*CASSANDRA_MODULES)
class test_CassandraBackend:

    def setup(self):
        self.app.conf.update(
            cassandra_servers=['example.com'],
            cassandra_keyspace='celery',
            cassandra_table='task_results',
        )

    def test_init_no_cassandra(self, *modules):
        # should raise ImproperlyConfigured when no python-driver
        # installed.
        from celery.backends import cassandra as mod
        prev, mod.cassandra = mod.cassandra, None
        try:
            with pytest.raises(ImproperlyConfigured):
                mod.CassandraBackend(app=self.app)
        finally:
            mod.cassandra = prev

    def test_init_with_and_without_LOCAL_QUROM(self, *modules):
        from celery.backends import cassandra as mod
        mod.cassandra = Mock()

        cons = mod.cassandra.ConsistencyLevel = Bunch(
            LOCAL_QUORUM='foo',
        )

        self.app.conf.cassandra_read_consistency = 'LOCAL_FOO'
        self.app.conf.cassandra_write_consistency = 'LOCAL_FOO'

        mod.CassandraBackend(app=self.app)
        cons.LOCAL_FOO = 'bar'
        mod.CassandraBackend(app=self.app)

        # no servers raises ImproperlyConfigured
        with pytest.raises(ImproperlyConfigured):
            self.app.conf.cassandra_servers = None
            mod.CassandraBackend(
                app=self.app, keyspace='b', column_family='c',
            )

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_reduce(self, *modules):
        from celery.backends.cassandra import CassandraBackend
        assert loads(dumps(CassandraBackend(app=self.app)))

    def test_get_task_meta_for(self, *modules):
        from celery.backends import cassandra as mod
        mod.cassandra = Mock()

        x = mod.CassandraBackend(app=self.app)
        x._connection = True
        session = x._session = Mock()
        execute = session.execute = Mock()
        execute.return_value = [
            [states.SUCCESS, '1', datetime.now(), b'', b'']
        ]
        x.decode = Mock()
        meta = x._get_task_meta_for('task_id')
        assert meta['status'] == states.SUCCESS

        x._session.execute.return_value = []
        meta = x._get_task_meta_for('task_id')
        assert meta['status'] == states.PENDING

    def test_store_result(self, *modules):
        from celery.backends import cassandra as mod
        mod.cassandra = Mock()

        x = mod.CassandraBackend(app=self.app)
        x._connection = True
        session = x._session = Mock()
        session.execute = Mock()
        x._store_result('task_id', 'result', states.SUCCESS)

    def test_process_cleanup(self, *modules):
        from celery.backends import cassandra as mod
        x = mod.CassandraBackend(app=self.app)
        x.process_cleanup()

        assert x._connection is None
        assert x._session is None

    def test_timeouting_cluster(self):
        # Tests behavior when Cluster.connect raises
        # cassandra.OperationTimedOut.
        from celery.backends import cassandra as mod

        class OTOExc(Exception):
            pass

        class VeryFaultyCluster(object):
            def __init__(self, *args, **kwargs):
                pass

            def connect(self, *args, **kwargs):
                raise OTOExc()

            def shutdown(self):
                pass

        mod.cassandra = Mock()
        mod.cassandra.OperationTimedOut = OTOExc
        mod.cassandra.cluster = Mock()
        mod.cassandra.cluster.Cluster = VeryFaultyCluster

        x = mod.CassandraBackend(app=self.app)

        with pytest.raises(OTOExc):
            x._store_result('task_id', 'result', states.SUCCESS)
        assert x._connection is None
        assert x._session is None

        x.process_cleanup()  # shouldn't raise

    def test_please_free_memory(self):
        # Ensure that Cluster object IS shut down.
        from celery.backends import cassandra as mod

        class RAMHoggingCluster(object):

            objects_alive = 0

            def __init__(self, *args, **kwargs):
                pass

            def connect(self, *args, **kwargs):
                RAMHoggingCluster.objects_alive += 1
                return Mock()

            def shutdown(self):
                RAMHoggingCluster.objects_alive -= 1

        mod.cassandra = Mock()

        mod.cassandra.cluster = Mock()
        mod.cassandra.cluster.Cluster = RAMHoggingCluster

        for x in range(0, 10):
            x = mod.CassandraBackend(app=self.app)
            x._store_result('task_id', 'result', states.SUCCESS)
            x.process_cleanup()

        assert RAMHoggingCluster.objects_alive == 0

    def test_auth_provider(self):
        # Ensure valid auth_provider works properly, and invalid one raises
        # ImproperlyConfigured exception.
        from celery.backends import cassandra as mod

        class DummyAuth(object):
            ValidAuthProvider = Mock()

        mod.cassandra = Mock()
        mod.cassandra.auth = DummyAuth

        # Valid auth_provider
        self.app.conf.cassandra_auth_provider = 'ValidAuthProvider'
        self.app.conf.cassandra_auth_kwargs = {
            'username': 'stuff'
        }
        mod.CassandraBackend(app=self.app)

        # Invalid auth_provider
        self.app.conf.cassandra_auth_provider = 'SpiderManAuth'
        self.app.conf.cassandra_auth_kwargs = {
            'username': 'Jack'
        }
        with pytest.raises(ImproperlyConfigured):
            mod.CassandraBackend(app=self.app)

    def test_options(self):
        # Ensure valid options works properly
        from celery.backends import cassandra as mod

        mod.cassandra = Mock()
        # Valid options
        self.app.conf.cassandra_options = {
            'cql_version': '3.2.1',
            'protocol_version': 3
        }
        mod.CassandraBackend(app=self.app)
