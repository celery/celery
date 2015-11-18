from __future__ import absolute_import

from pickle import loads, dumps
from datetime import datetime

from celery import states
from celery.exceptions import ImproperlyConfigured
from celery.tests.case import (
    AppCase, Mock, mock_module, depends_on_current_app
)

CASSANDRA_MODULES = ['cassandra', 'cassandra.cluster']


class Object(object):
    pass


class test_CassandraBackend(AppCase):

    def setup(self):
        self.app.conf.update(
            cassandra_servers=['example.com'],
            cassandra_keyspace='celery',
            cassandra_table='task_results',
        )

    def test_init_no_cassandra(self):
        """should raise ImproperlyConfigured when no python-driver
        installed."""
        with mock_module(*CASSANDRA_MODULES):
            from celery.backends import cassandra as mod
            prev, mod.cassandra = mod.cassandra, None
            try:
                with self.assertRaises(ImproperlyConfigured):
                    mod.CassandraBackend(app=self.app)
            finally:
                mod.cassandra = prev

    def test_init_with_and_without_LOCAL_QUROM(self):
        with mock_module(*CASSANDRA_MODULES):
            from celery.backends import cassandra as mod
            mod.cassandra = Mock()

            cons = mod.cassandra.ConsistencyLevel = Object()
            cons.LOCAL_QUORUM = 'foo'

            self.app.conf.cassandra_read_consistency = 'LOCAL_FOO'
            self.app.conf.cassandra_write_consistency = 'LOCAL_FOO'

            mod.CassandraBackend(app=self.app)
            cons.LOCAL_FOO = 'bar'
            mod.CassandraBackend(app=self.app)

            # no servers raises ImproperlyConfigured
            with self.assertRaises(ImproperlyConfigured):
                self.app.conf.cassandra_servers = None
                mod.CassandraBackend(
                    app=self.app, keyspace='b', column_family='c',
                )

    @depends_on_current_app
    def test_reduce(self):
        with mock_module(*CASSANDRA_MODULES):
            from celery.backends.cassandra import CassandraBackend
            self.assertTrue(loads(dumps(CassandraBackend(app=self.app))))

    def test_get_task_meta_for(self):
        with mock_module(*CASSANDRA_MODULES):
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
            self.assertEqual(meta['status'], states.SUCCESS)

            x._session.execute.return_value = []
            meta = x._get_task_meta_for('task_id')
            self.assertEqual(meta['status'], states.PENDING)

    def test_store_result(self):
        with mock_module(*CASSANDRA_MODULES):
            from celery.backends import cassandra as mod
            mod.cassandra = Mock()

            x = mod.CassandraBackend(app=self.app)
            x._connection = True
            session = x._session = Mock()
            session.execute = Mock()
            x._store_result('task_id', 'result', states.SUCCESS)

    def test_process_cleanup(self):
        with mock_module(*CASSANDRA_MODULES):
            from celery.backends import cassandra as mod
            x = mod.CassandraBackend(app=self.app)
            x.process_cleanup()

            self.assertIsNone(x._connection)
            self.assertIsNone(x._session)

    def test_timeouting_cluster(self):
        """Tests behaviour when Cluster.connect raises
        cassandra.OperationTimedOut."""
        with mock_module(*CASSANDRA_MODULES):
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

            with self.assertRaises(OTOExc):
                x._store_result('task_id', 'result', states.SUCCESS)
            self.assertIsNone(x._connection)
            self.assertIsNone(x._session)

            x.process_cleanup()  # should not raise

    def test_please_free_memory(self):
        """Ensure that Cluster object IS shut down."""
        with mock_module(*CASSANDRA_MODULES):
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

            self.assertEquals(RAMHoggingCluster.objects_alive, 0)
