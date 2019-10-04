# -*- coding: utf-8 -*-
"""
    celery.backends.redis
    ~~~~~~~~~~~~~~~~~~~~~

    Redis result store backend.

"""
from __future__ import absolute_import

from functools import partial
from ssl import CERT_NONE, CERT_OPTIONAL, CERT_REQUIRED
from urllib.parse import unquote

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
except ImportError:                 # pragma: no cover
    redis = None                    # noqa
    ConnectionError = None          # noqa
    get_redis_error_classes = None  # noqa

__all__ = ['RedisBackend']

REDIS_MISSING = """\
You need to install the redis library in order to use \
the Redis result store backend."""


E_REDIS_SSL_PARAMS_AND_SCHEME_MISMATCH = """
SSL connection parameters have been provided but the specified URL scheme \
is redis://. A Redis SSL connection URL should use the scheme rediss://.
"""

E_REDIS_SSL_CERT_REQS_MISSING_INVALID = """
A rediss:// URL must have parameter ssl_cert_reqs and this must be set to \
CERT_REQUIRED, CERT_OPTIONAL, or CERT_NONE
"""

W_REDIS_SSL_CERT_OPTIONAL = """
Setting ssl_cert_reqs=CERT_OPTIONAL when connecting to redis means that \
celery might not valdate the identity of the redis broker when connecting. \
This leaves you vulnerable to man in the middle attacks.
"""

W_REDIS_SSL_CERT_NONE = """
Setting ssl_cert_reqs=CERT_NONE when connecting to redis means that celery \
will not valdate the identity of the redis broker when connecting. This \
leaves you vulnerable to man in the middle attacks.
"""

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

    def __init__(self, host=None, port=None, db=None, password=None,
                 expires=None, max_connections=None, url=None,
                 connection_pool=None, new_join=False, **kwargs):
        super(RedisBackend, self).__init__(**kwargs)
        conf = self.app.conf
        if self.redis is None:
            raise ImproperlyConfigured(REDIS_MISSING)
        self._client_capabilities = self._detect_client_capabilities()

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
            'port': _get('PORT') or 6379,
            'db': _get('DB') or 0,
            'password': _get('PASSWORD'),
            'max_connections': self.max_connections,
        }
        if url:
            self.connparams = self._params_from_url(url, self.connparams)
        # If we've received SSL parameters via query string or the
        # redis_backend_use_ssl dict, check ssl_cert_reqs is valid. If set
        # via query string ssl_cert_reqs will be a string so convert it here
        if ('connection_class' in self.connparams and
                self.connparams['connection_class'] is redis.SSLConnection):
            ssl_cert_reqs_missing = 'MISSING'
            ssl_string_to_constant = {'CERT_REQUIRED': CERT_REQUIRED,
                                      'CERT_OPTIONAL': CERT_OPTIONAL,
                                      'CERT_NONE': CERT_NONE,
                                      'required': CERT_REQUIRED,
                                      'optional': CERT_OPTIONAL,
                                      'none': CERT_NONE}
            ssl_cert_reqs = self.connparams.get('ssl_cert_reqs', ssl_cert_reqs_missing)
            ssl_cert_reqs = ssl_string_to_constant.get(ssl_cert_reqs, ssl_cert_reqs)
            if ssl_cert_reqs not in ssl_string_to_constant.values():
                logger.fatal(E_REDIS_SSL_CERT_REQS_MISSING_INVALID)
                raise ImproperlyConfigured(E_REDIS_SSL_CERT_REQS_MISSING_INVALID)
            if ssl_cert_reqs == CERT_OPTIONAL:
                logger.warning(W_REDIS_SSL_CERT_OPTIONAL)
            elif ssl_cert_reqs == CERT_NONE:
                logger.warning(W_REDIS_SSL_CERT_NONE)

            self.connparams['ssl_cert_reqs'] = ssl_cert_reqs

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
        ssl_param_keys = ['ssl_ca_certs', 'ssl_certfile', 'ssl_keyfile', 'ssl_cert_reqs']

        if scheme == 'rediss':
            connparams['connection_class'] = redis.SSLConnection
            # The following parameters, if present in the URL, are encoded. We
            # must add the decoded values to connparams.
            for ssl_setting in ssl_param_keys:
                ssl_val = query.pop(ssl_setting, None)
                if ssl_val:
                    connparams[ssl_setting] = unquote(ssl_val)
        elif scheme == 'socket':
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
        return self.client.get(key)

    def mget(self, keys):
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
        tts = next(intervals)
        error('Connection to Redis lost: Retry (%s/%s) %s.',
              retries, max_retries or 'Inf',
              humanize_seconds(tts, 'in '))
        return tts

    def set(self, key, value, **retry_policy):
        return self.ensure(self._set, (key, value), **retry_policy)

    def _set(self, key, value):
        with self.client.pipeline() as pipe:
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
        with client.pipeline() as pipe:
            _, readycount, _ = pipe                                         \
                .rpush(jkey, self.encode([1, tid, state, result]))          \
                .llen(jkey)                                                 \
                .expire(jkey, 86400)                                        \
                .execute()

        try:
            callback = maybe_signature(request.chord, app=app)
            total = callback['chord_size']
            if readycount == total:
                decode, unpack = self.decode, self._unpack_chord_result
                with client.pipeline() as pipe:
                    resl, _, = pipe                 \
                        .lrange(jkey, 0, total)     \
                        .delete(jkey)               \
                        .execute()
                try:
                    callback.delay([unpack(tup, decode) for tup in resl])
                except Exception as exc:
                    error('Chord callback for %r raised: %r',
                          request.group, exc, exc_info=1)
                    return self.chord_error_from_stack(
                        callback,
                        ChordError('Callback error: {0!r}'.format(exc)),
                    )
        except ChordError as exc:
            error('Chord %r raised: %r', request.group, exc, exc_info=1)
            return self.chord_error_from_stack(callback, exc)
        except Exception as exc:
            error('Chord %r raised: %r', request.group, exc, exc_info=1)
            return self.chord_error_from_stack(
                callback, ChordError('Join error: {0!r}'.format(exc)),
            )

    def _detect_client_capabilities(self, socket_connect_timeout=False):
        if self.redis.VERSION < (2, 4, 4):
            raise ImproperlyConfigured(
                'Redis backend requires redis-py versions 2.4.4 or later. '
                'You have {0.__version__}'.format(redis))
        if self.redis.VERSION >= (2, 10):
            socket_connect_timeout = True
        return {'socket_connect_timeout': socket_connect_timeout}

    def _create_client(self, socket_timeout=None, socket_connect_timeout=None,
                       **params):
        return self._new_redis_client(
            socket_timeout=socket_timeout and float(socket_timeout),
            socket_connect_timeout=socket_connect_timeout and float(
                socket_connect_timeout), **params
        )

    def _new_redis_client(self, **params):
        if not self._client_capabilities['socket_connect_timeout']:
            params.pop('socket_connect_timeout', None)
        return self.redis.Redis(connection_pool=self.ConnectionPool(**params))

    @property
    def ConnectionPool(self):
        if self._ConnectionPool is None:
            self._ConnectionPool = self.redis.ConnectionPool
        return self._ConnectionPool

    @cached_property
    def client(self):
        return self._create_client(**self.connparams)

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
