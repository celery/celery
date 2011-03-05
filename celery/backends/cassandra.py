"""celery.backends.cassandra"""
try:
    import pycassa
    from thrift import Thrift
    C = pycassa.cassandra.ttypes
except ImportError:
    pycassa = None

import itertools
import random
import socket
import time

from datetime import datetime

from celery.backends.base import BaseDictBackend
from celery.exceptions import ImproperlyConfigured
from celery.utils.serialization import pickle
from celery.utils.timeutils import maybe_timedelta
from celery import states


class CassandraBackend(BaseDictBackend):
    """Highly fault tolerant Cassandra backend.

    .. attribute:: servers

        List of Cassandra servers with format: "hostname:port".

    :raises celery.exceptions.ImproperlyConfigured: if
        module :mod:`pycassa` is not available.

    """
    servers = []
    keyspace = None
    column_family = None
    _retry_timeout = 300
    _retry_wait = 3
    _index_shards = 64
    _index_keys = ["celery.results.index!%02x" % i
                        for i in range(_index_shards)]

    def __init__(self, servers=None, keyspace=None, column_family=None,
            cassandra_options=None, **kwargs):
        """Initialize Cassandra backend.

        Raises :class:`celery.exceptions.ImproperlyConfigured` if
        the :setting:`CASSANDRA_SERVERS` setting is not set.

        """
        super(CassandraBackend, self).__init__(**kwargs)
        self.logger = self.app.log.setup_logger(
                            name="celery.backends.cassandra")

        self.result_expires = kwargs.get("result_expires") or \
                                maybe_timedelta(
                                    self.app.conf.CELERY_TASK_RESULT_EXPIRES)

        if not pycassa:
            raise ImproperlyConfigured(
                "You need to install the pycassa library to use the "
                "Cassandra backend. See https://github.com/pycassa/pycassa")

        self.servers = servers or \
                        self.app.conf.get("CASSANDRA_SERVERS", self.servers)
        self.keyspace = keyspace or \
                            self.app.conf.get("CASSANDRA_KEYSPACE",
                                              self.keyspace)
        self.column_family = column_family or \
                                self.app.conf.get("CASSANDRA_COLUMN_FAMILY",
                                                  self.column_family)
        self.cassandra_options = dict(cassandra_options or {},
                                   **self.app.conf.get("CASSANDRA_OPTIONS",
                                                       {}))
        read_cons = self.app.conf.get("CASSANDRA_READ_CONSISTENCY",
                                      "LOCAL_QUORUM")
        write_cons = self.app.conf.get("CASSANDRA_WRITE_CONSISTENCY",
                                       "LOCAL_QUORUM")
        try:
            self.read_consistency = getattr(pycassa.ConsistencyLevel,
                                            read_cons)
        except AttributeError:
            self.read_consistency = pycassa.ConsistencyLevel.LOCAL_QUORUM
        try:
            self.write_consistency = getattr(pycassa.ConsistencyLevel,
                                             write_cons)
        except AttributeError:
            self.write_consistency = pycassa.ConsistencyLevel.LOCAL_QUORUM

        if not self.servers or not self.keyspace or not self.column_family:
            raise ImproperlyConfigured(
                    "Cassandra backend not configured.")

        self._column_family = None

    def _retry_on_error(func):
        def wrapper(*args, **kwargs):
            self = args[0]
            ts = time.time() + self._retry_timeout
            while 1:
                try:
                    return func(*args, **kwargs)
                except (pycassa.InvalidRequestException,
                        pycassa.TimedOutException,
                        pycassa.UnavailableException,
                        socket.error,
                        socket.timeout,
                        Thrift.TException), exc:
                    self.logger.warn('Cassandra error: %s. Retrying...' % exc)
                    if time.time() > ts:
                        raise
                    time.sleep(self._retry_wait)
        return wrapper

    def _get_column_family(self):
        if self._column_family is None:
            conn = pycassa.connect(self.keyspace, servers=self.servers,
                                   **self.cassandra_options)
            self._column_family = \
              pycassa.ColumnFamily(conn, self.column_family,
                    read_consistency_level=self.read_consistency,
                    write_consistency_level=self.write_consistency)
        return self._column_family

    def process_cleanup(self):
        if self._column_family is not None:
            self._column_family = None

    @_retry_on_error
    def _store_result(self, task_id, result, status, traceback=None):
        """Store return value and status of an executed task."""
        cf = self._get_column_family()
        date_done = datetime.utcnow()
        index_key = 'celery.results.index!%02x' % (
                random.randrange(self._index_shards))
        index_column_name = '%8x!%s' % (time.mktime(date_done.timetuple()),
                                        task_id)
        meta = {"status": status,
                "result": pickle.dumps(result),
                "date_done": date_done.strftime('%Y-%m-%dT%H:%M:%SZ'),
                "traceback": pickle.dumps(traceback)}
        cf.insert(task_id, meta)
        cf.insert(index_key, {index_column_name: status})

    @_retry_on_error
    def _get_task_meta_for(self, task_id):
        """Get task metadata for a task by id."""
        cf = self._get_column_family()
        try:
            obj = cf.get(task_id)
            meta = {
                "task_id": task_id,
                "status": obj["status"],
                "result": pickle.loads(str(obj["result"])),
                "date_done": obj["date_done"],
                "traceback": pickle.loads(str(obj["traceback"])),
            }
        except (KeyError, pycassa.NotFoundException):
            meta = {"status": states.PENDING, "result": None}
        return meta

    def cleanup(self):
        """Delete expired metadata."""
        self.logger.debug('Running cleanup...')
        expires = datetime.utcnow() - self.result_expires
        end_column = '%8x"' % (time.mktime(expires.timetuple()))

        cf = self._get_column_family()
        column_parent = C.ColumnParent(cf.column_family)
        slice_pred = C.SlicePredicate(
                            slice_range=C.SliceRange('', end_column,
                                                     count=2 ** 30))
        columns = cf.client.multiget_slice(cf.keyspace, self._index_keys,
                                           column_parent, slice_pred,
                                           self.read_consistency)

        index_cols = [c.column.name
                        for c in itertools.chain(*columns.values())]
        for k in self._index_keys:
            cf.remove(k, index_cols)

        task_ids = [c[9:] for c in index_cols]
        for k in task_ids:
            cf.remove(k)

        self.logger.debug('Cleaned %i expired results' % len(task_ids))
