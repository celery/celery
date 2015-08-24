from __future__ import absolute_import
from pickle import loads, dumps
from datetime import datetime

import six
from celery import states
from celery.exceptions import ImproperlyConfigured
from celery.tests.case import (
    AppCase, Mock, mock_module, depends_on_current_app
)

class Object(object):
    pass


class test_NewCassandraBackend(AppCase):

    def setup(self):
        self.app.conf.update(
            CASSANDRA_SERVERS=['example.com'],
            CASSANDRA_KEYSPACE='celery',
            CASSANDRA_TABLE='task_results',
        )

    def test_init_no_cassandra(self):
        """
        Tests behaviour when no python-driver is installed.
        new_cassandra should raise ImproperlyConfigured
        """
        with mock_module('cassandra'):
            from celery.backends import new_cassandra as mod
            prev, mod.cassandra = mod.cassandra, None
            try:
                with self.assertRaises(ImproperlyConfigured):
                    mod.NewCassandraBackend(app=self.app)
            finally:
                mod.cassandra = prev

    def test_init_with_and_without_LOCAL_QUROM(self):
        with mock_module('cassandra'):
            from celery.backends import new_cassandra as mod
            mod.cassandra = Mock()
            cons = mod.cassandra.ConsistencyLevel = Object()
            cons.LOCAL_QUORUM = 'foo'

            self.app.conf.CASSANDRA_READ_CONSISTENCY = 'LOCAL_FOO'
            self.app.conf.CASSANDRA_WRITE_CONSISTENCY = 'LOCAL_FOO'

            mod.NewCassandraBackend(app=self.app)
            cons.LOCAL_FOO = 'bar'
            mod.NewCassandraBackend(app=self.app)

            # no servers raises ImproperlyConfigured
            with self.assertRaises(ImproperlyConfigured):
                self.app.conf.CASSANDRA_SERVERS = None
                mod.NewCassandraBackend(
                    app=self.app, keyspace='b', column_family='c',
                )

    @depends_on_current_app
    def test_reduce(self):
        with mock_module('cassandra'):
            from celery.backends.new_cassandra import NewCassandraBackend
            self.assertTrue(loads(dumps(NewCassandraBackend(app=self.app))))

    def test_get_task_meta_for(self):
        with mock_module('cassandra'):
            from celery.backends import new_cassandra as mod
            mod.cassandra = Mock()
            x = mod.NewCassandraBackend(app=self.app)
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
        with mock_module('cassandra'):
            from celery.backends import new_cassandra as mod
            mod.cassandra = Mock()

            x = mod.NewCassandraBackend(app=self.app)
            x._connection = True
            session = x._session = Mock()
            session.execute = Mock()
            x._store_result('task_id', 'result', states.SUCCESS)

    def test_process_cleanup(self):
        with mock_module('cassandra'):
            from celery.backends import new_cassandra as mod
            x = mod.NewCassandraBackend(app=self.app)
            x.process_cleanup()

            self.assertIsNone(x._connection)
            self.assertIsNone(x._session)
