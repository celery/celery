from datetime import datetime
from pickle import dumps, loads
from unittest.mock import Mock

import pytest
from case import mock

from celery import states
from celery.exceptions import ImproperlyConfigured
from celery.utils.objects import Bunch

CASSANDRA_MODULES = [
    'cassandra',
    'cassandra.auth',
    'cassandra.cluster',
    'cassandra.query',
]


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
        session = x._session = Mock()
        execute = session.execute = Mock()
        result_set = Mock()
        result_set.one.return_value = [
            states.SUCCESS, '1', datetime.now(), b'', b''
        ]
        execute.return_value = result_set
        x.decode = Mock()
        meta = x._get_task_meta_for('task_id')
        assert meta['status'] == states.SUCCESS

        result_set.one.return_value = []
        x._session.execute.return_value = result_set
        meta = x._get_task_meta_for('task_id')
        assert meta['status'] == states.PENDING

    def test_as_uri(self):
        # Just ensure as_uri works properly
        from celery.backends import cassandra as mod
        mod.cassandra = Mock()

        x = mod.CassandraBackend(app=self.app)
        x.as_uri()
        x.as_uri(include_password=False)

    def test_store_result(self, *modules):
        from celery.backends import cassandra as mod
        mod.cassandra = Mock()

        x = mod.CassandraBackend(app=self.app)
        session = x._session = Mock()
        session.execute = Mock()
        x._store_result('task_id', 'result', states.SUCCESS)

    def test_timeouting_cluster(self):
        # Tests behavior when Cluster.connect raises
        # cassandra.OperationTimedOut.
        from celery.backends import cassandra as mod

        class OTOExc(Exception):
            pass

        class VeryFaultyCluster:
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
        assert x._cluster is None
        assert x._session is None

    def test_create_result_table(self):
        # Tests behavior when session.execute raises
        # cassandra.AlreadyExists.
        from celery.backends import cassandra as mod

        class OTOExc(Exception):
            pass

        class FaultySession:
            def __init__(self, *args, **kwargs):
                pass

            def execute(self, *args, **kwargs):
                raise OTOExc()

        class DummyCluster:

            def __init__(self, *args, **kwargs):
                pass

            def connect(self, *args, **kwargs):
                return FaultySession()

        mod.cassandra = Mock()
        mod.cassandra.cluster = Mock()
        mod.cassandra.cluster.Cluster = DummyCluster
        mod.cassandra.AlreadyExists = OTOExc

        x = mod.CassandraBackend(app=self.app)
        x._get_connection(write=True)
        assert x._session is not None

    def test_init_session(self):
        # Tests behavior when Cluster.connect works properly
        from celery.backends import cassandra as mod

        class DummyCluster:

            def __init__(self, *args, **kwargs):
                pass

            def connect(self, *args, **kwargs):
                return Mock()

        mod.cassandra = Mock()
        mod.cassandra.cluster = Mock()
        mod.cassandra.cluster.Cluster = DummyCluster

        x = mod.CassandraBackend(app=self.app)
        assert x._session is None
        x._get_connection(write=True)
        assert x._session is not None

        s = x._session
        x._get_connection()
        assert s is x._session

    def test_auth_provider(self):
        # Ensure valid auth_provider works properly, and invalid one raises
        # ImproperlyConfigured exception.
        from celery.backends import cassandra as mod

        class DummyAuth:
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
