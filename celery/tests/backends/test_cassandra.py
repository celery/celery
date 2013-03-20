from __future__ import absolute_import
from __future__ import with_statement

import socket

from mock import Mock
from pickle import loads, dumps

from celery import Celery
from celery import states
from celery.exceptions import ImproperlyConfigured
from celery.tests.utils import AppCase, mock_module


class Object(object):
    pass


def install_exceptions(mod):
    # py3k: cannot catch exceptions not ineheriting from BaseException.

    class NotFoundException(Exception):
        pass

    class TException(Exception):
        pass

    class InvalidRequestException(Exception):
        pass

    class UnavailableException(Exception):
        pass

    class TimedOutException(Exception):
        pass

    class AllServersUnavailable(Exception):
        pass

    mod.NotFoundException = NotFoundException
    mod.TException = TException
    mod.InvalidRequestException = InvalidRequestException
    mod.TimedOutException = TimedOutException
    mod.UnavailableException = UnavailableException
    mod.AllServersUnavailable = AllServersUnavailable


class test_CassandraBackend(AppCase):

    def test_init_no_pycassa(self):
        with mock_module('pycassa'):
            from celery.backends import cassandra as mod
            prev, mod.pycassa = mod.pycassa, None
            try:
                with self.assertRaises(ImproperlyConfigured):
                    mod.CassandraBackend(app=self.app)
            finally:
                mod.pycassa = prev

    def get_app(self):
        celery = Celery(set_as_current=False)
        celery.conf.CASSANDRA_SERVERS = ['example.com']
        celery.conf.CASSANDRA_KEYSPACE = 'keyspace'
        celery.conf.CASSANDRA_COLUMN_FAMILY = 'columns'
        return celery

    def test_init_with_and_without_LOCAL_QUROM(self):
        with mock_module('pycassa'):
            from celery.backends import cassandra as mod
            mod.pycassa = Mock()
            install_exceptions(mod.pycassa)
            cons = mod.pycassa.ConsistencyLevel = Object()
            cons.LOCAL_QUORUM = 'foo'

            app = self.get_app()
            app.conf.CASSANDRA_READ_CONSISTENCY = 'LOCAL_FOO'
            app.conf.CASSANDRA_WRITE_CONSISTENCY = 'LOCAL_FOO'

            mod.CassandraBackend(app=app)
            cons.LOCAL_FOO = 'bar'
            mod.CassandraBackend(app=app)

            # no servers raises ImproperlyConfigured
            with self.assertRaises(ImproperlyConfigured):
                app.conf.CASSANDRA_SERVERS = None
                mod.CassandraBackend(app=app, keyspace='b', column_family='c')

    def test_reduce(self):
        with mock_module('pycassa'):
            from celery.backends.cassandra import CassandraBackend
            self.assertTrue(loads(dumps(CassandraBackend(app=self.get_app()))))

    def test_get_task_meta_for(self):
        with mock_module('pycassa'):
            from celery.backends import cassandra as mod
            mod.pycassa = Mock()
            install_exceptions(mod.pycassa)
            mod.Thrift = Mock()
            install_exceptions(mod.Thrift)
            app = self.get_app()
            x = mod.CassandraBackend(app=app)
            Get_Column = x._get_column_family = Mock()
            get_column = Get_Column.return_value = Mock()
            get = get_column.get
            META = get.return_value = {
                'task_id': 'task_id',
                'status': states.SUCCESS,
                'result': '1',
                'date_done': 'date',
                'traceback': '',
                'children': None,
            }
            x.decode = Mock()
            x.detailed_mode = False
            meta = x._get_task_meta_for('task_id')
            self.assertEqual(meta['status'], states.SUCCESS)

            x.detailed_mode = True
            row = get.return_value = Mock()
            row.values.return_value = [Mock()]
            x.decode.return_value = META
            meta = x._get_task_meta_for('task_id')
            self.assertEqual(meta['status'], states.SUCCESS)
            x.decode.return_value = Mock()

            x.detailed_mode = False
            get.side_effect = KeyError()
            meta = x._get_task_meta_for('task_id')
            self.assertEqual(meta['status'], states.PENDING)

            calls = [0]
            end = [10]

            def work_eventually(*arg):
                try:
                    if calls[0] > end[0]:
                        return META
                    raise socket.error()
                finally:
                    calls[0] += 1
            get.side_effect = work_eventually
            x._retry_timeout = 10
            x._retry_wait = 0.01
            meta = x._get_task_meta_for('task')
            self.assertEqual(meta['status'], states.SUCCESS)

            x._retry_timeout = 0.1
            calls[0], end[0] = 0, 100
            with self.assertRaises(socket.error):
                x._get_task_meta_for('task')

    def test_store_result(self):
        with mock_module('pycassa'):
            from celery.backends import cassandra as mod
            mod.pycassa = Mock()
            install_exceptions(mod.pycassa)
            mod.Thrift = Mock()
            install_exceptions(mod.Thrift)
            app = self.get_app()
            x = mod.CassandraBackend(app=app)
            Get_Column = x._get_column_family = Mock()
            cf = Get_Column.return_value = Mock()
            x.detailed_mode = False
            x._store_result('task_id', 'result', states.SUCCESS)
            self.assertTrue(cf.insert.called)

            cf.insert.reset()
            x.detailed_mode = True
            x._store_result('task_id', 'result', states.SUCCESS)
            self.assertTrue(cf.insert.called)

    def test_process_cleanup(self):
        with mock_module('pycassa'):
            from celery.backends import cassandra as mod
            app = self.get_app()
            x = mod.CassandraBackend(app=app)
            x._column_family = None
            x.process_cleanup()

            x._column_family = True
            x.process_cleanup()
            self.assertIsNone(x._column_family)

    def test_get_column_family(self):
        with mock_module('pycassa'):
            from celery.backends import cassandra as mod
            mod.pycassa = Mock()
            install_exceptions(mod.pycassa)
            app = self.get_app()
            x = mod.CassandraBackend(app=app)
            self.assertTrue(x._get_column_family())
            self.assertIsNotNone(x._column_family)
            self.assertIs(x._get_column_family(), x._column_family)
