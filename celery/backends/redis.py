# -*- coding: utf-8 -*-
"""
    celery.backends.redis
    ~~~~~~~~~~~~~~~~~~~~~

    Redis result store backend.

"""
from __future__ import absolute_import

from functools import partial

from kombu.utils import cached_property, retry_over_time
from kombu.utils.url import _parse_url

from celery import states
from celery.canvas import maybe_signature
from celery.exceptions import ChordError, ImproperlyConfigured
from celery.five import string_t
from celery.utils import deprecated_property, strtobool
from celery.utils.functional import dictfilter
from celery.utils.log import get_logger
from celery.utils.timeutils import humanize_seconds

from .base import KeyValueStoreBackend

try:
    import redis
    from redis.exceptions import ConnectionError
    from kombu.transport.redis import get_redis_error_classes
    from redis import sentinel as sentinel_mod
except ImportError:                 # pragma: no cover
    redis = None                    # noqa
    ConnectionError = None          # noqa
    get_redis_error_classes = None  # noqa
    sentinel_mod = None             # noqa

__all__ = ['RedisBackend']

REDIS_MISSING = """\
You need to install the redis library in order to use \
the Redis result store backend."""

logger = get_logger(__name__)
error = logger.error


class RedisBackend(KeyValueStoreBackend):
    """Redis task result store."""

    #: redis-py client module.
    redis = redis

    #: Maximium number of connections in the pool.
    max_connections = None

    supports_autoexpire = True
    supports_native_join = True
    implements_incr = True

    REDIS_DEFAULT_PORT = 6379
    SENTINEL_DEFAULT_PORT = 26379

    def __init__(self, host=None, port=None, db=None, password=None,
                 expires=None, max_connections=None, url=None,
                 connection_pool=None, new_join=False,
                 sentinel=False, extra_sentinels=None, cluster_name=None, **kwargs):
        super(RedisBackend, self).__init__(**kwargs)
        conf = self.app.conf
        if self.redis is None:
            raise ImproperlyConfigured(REDIS_MISSING)

        # For compatibility with the old REDIS_* configuration keys.
        def _get(key):
            for prefix in 'CELERY_REDIS_{0}', 'REDIS_{0}':
                try:
                    return conf[prefix.format(key)]
                except KeyError:
                    pass
        if host and '://' in host:
            url = host
            host = None

        self.max_connections = (
            max_connections or _get('MAX_CONNECTIONS') or self.max_connections
        )
        self._ConnectionPool = connection_pool

        self.connparams = {
            'host': _get('HOST') or 'localhost',
            'db': _get('DB') or 0,
            'password': _get('PASSWORD'),
            'max_connections': self.max_connections,
            'sentinel': _get('SENTINEL') or str(sentinel),
        }
        if url:
            self.connparams = self._params_from_url(url, self.connparams)

        self.connparams['sentinel'] = strtobool(self.connparams.get('sentinel', False))
        # resolve default port
        if 'port' not in self.connparams:
            self.connparams['port'] = _get('DB') or (\
                RedisBackend.SENTINEL_DEFAULT_PORT \
                if self.connparams['sentinel']
                else RedisBackend.REDIS_DEFAULT_PORT
            )

        if not self.connparams['sentinel']:
            self.connparams.pop('sentinel')
            self.sentinel = None
        else:
            self.connparams.setdefault('cluster_name', _get("CLUSTER_NAME") or 'mymaster'),
            self.connparams.setdefault('extra_sentinels', _get('EXTRA_SENTINELS') or extra_sentinels)
            self.connparams.setdefault(
                'min_other_sentinels',
                int(_get('MIN_OTHER_SENTINELS') or 0)
            )
            self.sentinel = sentinel_mod

        self.url = url
        self.expires = self.prepare_expires(expires, type=int)

        try:
            new_join = strtobool(self.connparams.pop('new_join'))
        except KeyError:
            pass
        if new_join:
            self.apply_chord = self._new_chord_apply
            self.on_chord_part_return = self._new_chord_return

        self.connection_errors, self.channel_errors = (
            get_redis_error_classes() if get_redis_error_classes
            else ((), ()))

    def _params_from_url(self, url, defaults):
        scheme, host, port, user, password, path, query = _parse_url(url)
        connparams = dict(
            defaults, **dictfilter({
                'host': host, 'port': port, 'password': password,
                'db': query.pop('virtual_host', None)})
        )

        if scheme == 'socket':
            # use 'path' as path to the socket… in this case
            # the database number should be given in 'query'
            connparams.update({
                'connection_class': self.redis.UnixDomainSocketConnection,
                'path': '/' + path,
            })
            # host+port are invalid options when using this connection type.
            connparams.pop('host', None)
            connparams.pop('port', None)
        else:
            connparams['db'] = path

        # db may be string and start with / like in kombu.
        db = connparams.get('db') or 0
        db = db.strip('/') if isinstance(db, string_t) else db
        connparams['db'] = int(db)

        # Query parameters override other parameters
        connparams.update(query)
        return connparams

    def get(self, key):
        if self.sentinel:
            return self.ensure(self._get, (key,))
        else:
            return self._get(key)

    def mget(self, keys):
        if self.sentinel:
            return self.ensure(self._mget, (keys,))
        else:
            return self._mget(keys)

    def _get(self, key):
        return self.client.get(key)

    def _mget(self, keys):
        return self.client.mget(keys)

    def ensure(self, fun, args, **policy):
        retry_policy = dict(self.retry_policy, **policy)
        max_retries = retry_policy.get('max_retries')
        return retry_over_time(
            fun, self.connection_errors, args, {},
            partial(self.on_connection_error, max_retries),
            **retry_policy
        )

    def on_connection_error(self, max_retries, exc, intervals, retries):
        if self.connparams['sentinel']:
            # reset cache property so that sentinel provides a new connection during next call
            del self.client
            return None
        else:
            tts = next(intervals)
            error('Connection to Redis lost: Retry (%s/%s) %s.',
                  retries, max_retries or 'Inf',
                  humanize_seconds(tts, 'in '))
            return tts

    def set(self, key, value, **retry_policy):
        return self.ensure(self._set, (key, value), **retry_policy)

    def _set(self, key, value):
        pipe = self.client.pipeline()
        if self.expires:
            pipe.setex(key, value, self.expires)
        else:
            pipe.set(key, value)
        pipe.publish(key, value)
        pipe.execute()

    def delete(self, key):
        self.client.delete(key)

    def incr(self, key):
        return self.client.incr(key)

    def expire(self, key, value):
        return self.client.expire(key, value)

    def _unpack_chord_result(self, tup, decode,
                             EXCEPTION_STATES=states.EXCEPTION_STATES,
                             PROPAGATE_STATES=states.PROPAGATE_STATES):
        _, tid, state, retval = decode(tup)
        if state in EXCEPTION_STATES:
            retval = self.exception_to_python(retval)
        if state in PROPAGATE_STATES:
            raise ChordError('Dependency {0} raised {1!r}'.format(tid, retval))
        return retval

    def _new_chord_apply(self, header, partial_args, group_id, body,
                         result=None, **options):
        # avoids saving the group in the redis db.
        return header(*partial_args, task_id=group_id)

    def _new_chord_return(self, task, state, result, propagate=None,
                          PROPAGATE_STATES=states.PROPAGATE_STATES):
        app = self.app
        if propagate is None:
            propagate = self.app.conf.CELERY_CHORD_PROPAGATES
        request = task.request
        tid, gid = request.id, request.group
        if not gid or not tid:
            return

        client = self.client
        jkey = self.get_key_for_group(gid, '.j')
        result = self.encode_result(result, state)
        _, readycount, _ = client.pipeline()                            \
            .rpush(jkey, self.encode([1, tid, state, result]))          \
            .llen(jkey)                                                 \
            .expire(jkey, 86400)                                        \
            .execute()

        try:
            callback = maybe_signature(request.chord, app=app)
            total = callback['chord_size']
            if readycount == total:
                decode, unpack = self.decode, self._unpack_chord_result
                resl, _ = client.pipeline()     \
                    .lrange(jkey, 0, total)     \
                    .delete(jkey)               \
                    .execute()
                try:
                    callback.delay([unpack(tup, decode) for tup in resl])
                except Exception as exc:
                    error('Chord callback for %r raised: %r',
                          request.group, exc, exc_info=1)
                    app._tasks[callback.task].backend.fail_from_current_stack(
                        callback.id,
                        exc=ChordError('Callback error: {0!r}'.format(exc)),
                    )
        except ChordError as exc:
            error('Chord %r raised: %r', request.group, exc, exc_info=1)
            app._tasks[callback.task].backend.fail_from_current_stack(
                callback.id, exc=exc,
            )
        except Exception as exc:
            error('Chord %r raised: %r', request.group, exc, exc_info=1)
            app._tasks[callback.task].backend.fail_from_current_stack(
                callback.id, exc=ChordError('Join error: {0!r}'.format(exc)),
            )

    @property
    def ConnectionPool(self):
        if self._ConnectionPool is None:
            self._ConnectionPool = self.redis.ConnectionPool
        return self._ConnectionPool

    def _dict_filter_prefix(self, dico, prefix):
        return dict((k[len(prefix):], v) for k, v in dico.items() if k.startswith(prefix))

    @cached_property
    def sentinel_client(self):
        sentinels = [(self.connparams['host'], self.connparams['port'])]
        sentinels.extend([hostport.split(':') for hostport in \
            self.connparams.get('extra_sentinels', "").split(',')
        ])

        sentinel_args = self._dict_filter_prefix(self.connparams, 'sentinel_')
        redis_connection_args = self._dict_filter_prefix(self.connparams, 'redis_')

        return self.sentinel.Sentinel(
            sentinels,
            min_other_sentinels=self.connparams['min_other_sentinels'],
            sentinel_kwargs=sentinel_args,
            **redis_connection_args
        )


    @cached_property
    def client(self):
        if self.sentinel is not None:
            return self.sentinel_client.master_for(
                self.connparams['cluster_name'],
                redis_class=redis.Redis
            )
        else:
            return self.redis.Redis(
                connection_pool=self.ConnectionPool(**self.connparams),
            )


    def __reduce__(self, args=(), kwargs={}):
        return super(RedisBackend, self).__reduce__(
            (self.url, ), {'expires': self.expires},
        )

    @deprecated_property(3.2, 3.3)
    def host(self):
        return self.connparams['host']

    @deprecated_property(3.2, 3.3)
    def port(self):
        return self.connparams['port']

    @deprecated_property(3.2, 3.3)
    def db(self):
        return self.connparams['db']

    @deprecated_property(3.2, 3.3)
    def password(self):
        return self.connparams['password']
