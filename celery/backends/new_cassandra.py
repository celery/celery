# -* coding: utf-8 -*-
"""
    celery.backends.new_cassandra
    ~~~~~~~~~~~~~~~~~~~~~~~~~

    Apache Cassandra result store backend using DataStax driver

"""
from __future__ import absolute_import

try:  # pragma: no cover
    import cassandra
except ImportError:  # pragma: no cover
    cassandra = None   # noqa

from celery import states
from celery.exceptions import ImproperlyConfigured
from celery.utils.log import get_logger

from .base import BaseBackend

__all__ = ['NewCassandraBackend']

logger = get_logger(__name__)


class NewCassandraBackend(BaseBackend):
    """New Cassandra backend utilizing DataStax driver

    .. attribute:: servers

        List of Cassandra servers with format: ``hostname``

    :raises celery.exceptions.ImproperlyConfigured: if
        module :mod:`cassandra` is not available.

    """
    servers = []
    keyspace = None
    table = None
    supports_autoexpire = True      # autoexpire supported via entry_ttl

    def __init__(self, servers=None, keyspace=None, table=None, entry_ttl=None,
                 port=9042, **kwargs):
        """Initialize Cassandra backend.

        Raises :class:`celery.exceptions.ImproperlyConfigured` if
        the :setting:`CASSANDRA_SERVERS` setting is not set.

        """
        super(NewCassandraBackend, self).__init__(**kwargs)

        if not cassandra:
            raise ImproperlyConfigured(
                'You need to install the cassandra library to use the '
                'Cassandra backend. See https://github.com/datastax/python-driver')

        conf = self.app.conf
        self.servers = (servers or
                        conf.get('CASSANDRA_SERVERS') or
                        self.servers)
        self.port = (port or
                     conf.get('CASSANDRA_PORT'))
        self.keyspace = (keyspace or
                         conf.get('CASSANDRA_KEYSPACE') or
                         self.keyspace)
        self.table = (table or
                      conf.get('CASSANDRA_TABLE') or
                      self.table)
        expires = (entry_ttl or conf.get('CASSANDRA_ENTRY_TTL', None))

        if expires is not None:
            self.cqlexpires = ' USING TTL %s' % (expires, )
        else:
            self.cqlexpires = ''

        read_cons = conf.get('CASSANDRA_READ_CONSISTENCY') or 'LOCAL_QUORUM'
        write_cons = conf.get('CASSANDRA_WRITE_CONSISTENCY') or 'LOCAL_QUORUM'
        try:
            self.read_consistency = getattr(cassandra.ConsistencyLevel,
                                            read_cons)
        except AttributeError:
            self.read_consistency = cassandra.ConsistencyLevel.LOCAL_QUORUM
        try:
            self.write_consistency = getattr(cassandra.ConsistencyLevel,
                                             write_cons)
        except AttributeError:
            self.write_consistency = cassandra.ConsistencyLevel.LOCAL_QUORUM

        if not self.servers or not self.keyspace or not self.table:
            raise ImproperlyConfigured(
                'Cassandra backend not configured.')

        self._connection = None
        self._session = None
        self._write_stmt = None
        self._read_stmt = None

    def process_cleanup(self):
        if self._connection is not None:
            self._session.shutdown()
            self._connection = None
            self._session = None

    def _get_connection(self, write=False):
        """
        Prepare the connection for action

        :param write: bool - are we a writer?
        """
        if self._connection is None:
            self._connection = cassandra.cluster.Cluster(self.servers,
                                                         port=self.port)
            self._session = self._connection.connect(self.keyspace)

            self._write_stmt = cassandra.query.SimpleStatement(
                'INSERT INTO '+self.table+' (task_id, status, result,'''
                ''' date_done, traceback, children) VALUES'''
                ' (%s, %s, %s, %s, %s, %s) '+self.cqlexpires+';')
            self._write_stmt.consistency_level = self.write_consistency

            self._read_stmt = cassandra.query.SimpleStatement(
                '''SELECT status, result, date_done, traceback, children
                   FROM '''+self.table+'''
                   WHERE task_id=%s''')
            self._read_stmt.consistency_level = self.read_consistency

            if write:
                # Only possible writers "workers" are allowed to issue
                # CREATE TABLE. This is to prevent conflicting situations
                # where both task-creator and task-executor would issue it
                # at the same time.

                # Anyway, if you are doing anything critical, you should
                # have probably created this table in advance, in which case
                # this query will be a no-op (instant fail with AlreadyExists)
                self._make_stmt = cassandra.query.SimpleStatement(
                    '''CREATE TABLE '''+self.table+''' (
                        task_id text,
                        status text,
                        result blob,
                        date_done text,
                        traceback blob,
                        children blob,
                        PRIMARY KEY (task_id)
                    );''')
                self._make_stmt.consistency_level = self.write_consistency
                try:
                    self._session.execute(self._make_stmt)
                except cassandra.AlreadyExists:
                    pass

    def _store_result(self, task_id, result, status,
                      traceback=None, request=None, **kwargs):
        """Store return value and status of an executed task."""
        self._get_connection(write=True)

        self._session.execute(self._write_stmt, (
            task_id,
            status,
            buffer(self.encode(result)),
            self.app.now().strftime('%Y-%m-%dT%H:%M:%SZ'),
            buffer(self.encode(traceback)),
            buffer(self.encode(self.current_task_children(request)))
        ))

    def _get_task_meta_for(self, task_id):
        """Get task metadata for a task by id."""
        self._get_connection()

        res = self._session.execute(self._read_stmt, (task_id, ))
        if not res:
            return {'status': states.PENDING, 'result': None}

        status, result, date_done, traceback, children = res[0]

        return self.meta_from_decoded({
            'task_id': task_id,
            'status': str(status),
            'result': self.decode(str(result)),
            'date_done': date_done,
            'traceback': self.decode(str(traceback)),
            'children': self.decode(str(children)),
        })

    def __reduce__(self, args=(), kwargs={}):
        kwargs.update(
            dict(servers=self.servers,
                 keyspace=self.keyspace,
                 table=self.table))
        return super(NewCassandraBackend, self).__reduce__(args, kwargs)
