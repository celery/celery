# -*- coding: utf-8 -*-
"""Redis result store backend."""
from __future__ import absolute_import, unicode_literals

import time
from functools import partial
from ssl import CERT_NONE, CERT_OPTIONAL, CERT_REQUIRED

from kombu.utils.functional import retry_over_time
from kombu.utils.objects import cached_property
from kombu.utils.url import _parse_url

from celery import states
from celery._state import task_join_will_block
from celery.canvas import maybe_signature
from celery.exceptions import ChordError, ImproperlyConfigured
from celery.five import string_t, text_t
from celery.utils import deprecated
from celery.utils.functional import dictfilter
from celery.utils.log import get_logger
from celery.utils.time import humanize_seconds

from .asynchronous import AsyncBackendMixin, BaseResultConsumer
from .base import BaseKeyValueStoreBackend

try:
    from urllib.parse import unquote
except ImportError:
    # Python 2
    from urlparse import unquote

try:
    import redis
    import redis.connection
    from kombu.transport.redis import get_redis_error_classes
except ImportError:  # pragma: no cover
    redis = None  # noqa
    get_redis_error_classes = None  # noqa

try:
    from redis import sentinel
except ImportError:
    sentinel = None

__all__ = ('RedisBackend', 'SentinelBackend')

E_REDIS_MISSING = """
You need to install the redis library in order to use \
the Redis result store backend.
"""

E_REDIS_SENTINEL_MISSING = """
You need to install the redis library with support of \
sentinel in order to use the Redis result store backend.
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

E_REDIS_SSL_PARAMS_AND_SCHEME_MISMATCH = """
SSL connection parameters have been provided but the specified URL scheme \
is redis://. A Redis SSL connection URL should use the scheme rediss://.
"""

E_REDIS_SSL_CERT_REQS_MISSING_INVALID = """
A rediss:// URL must have parameter ssl_cert_reqs and this must be set to \
CERT_REQUIRED, CERT_OPTIONAL, or CERT_NONE
"""

E_LOST = 'Connection to Redis lost: Retry (%s/%s) %s.'

logger = get_logger(__name__)


class ResultConsumer(BaseResultConsumer):
    _pubsub = None

    def __init__(self, *args, **kwargs):
        super(ResultConsumer, self).__init__(*args, **kwargs)
        self._get_key_for_task = self.backend.get_key_for_task
        self._decode_result = self.backend.decode_result
        self.subscribed_to = set()

    def on_after_fork(self):
        try:
            self.backend.client.connection_pool.reset()
            if self._pubsub is not None:
                self._pubsub.close()
        except KeyError as e:
            logger.warning(text_t(e))
        super(ResultConsumer, self).on_after_fork()

    def _maybe_cancel_ready_task(self, meta):
        if meta['status'] in states.READY_STATES:
            self.cancel_for(meta['task_id'])

    def on_state_change(self, meta, message):
        super(ResultConsumer, self).on_state_change(meta, message)
        self._maybe_cancel_ready_task(meta)

    def start(self, initial_task_id, **kwargs):
        self._pubsub = self.backend.client.pubsub(
            ignore_subscribe_messages=True,
        )
        self._consume_from(initial_task_id)

    def on_wait_for_pending(self, result, **kwargs):
        for meta in result._iter_meta():
            if meta is not None:
                self.on_state_change(meta, None)

    def stop(self):
        if self._pubsub is not None:
            self._pubsub.close()

    def drain_events(self, timeout=None):
        if self._pubsub:
            message = self._pubsub.get_message(timeout=timeout)
            if message and message['type'] == 'message':
                self.on_state_change(self._decode_result(message['data']), message)
        elif timeout:
            time.sleep(timeout)

    def consume_from(self, task_id):
        if self._pubsub is None:
            return self.start(task_id)
        self._consume_from(task_id)

    def _consume_from(self, task_id):
        key = self._get_key_for_task(task_id)
        if key not in self.subscribed_to:
            self.subscribed_to.add(key)
            self._pubsub.subscribe(key)

    def cancel_for(self, task_id):
        if self._pubsub:
            key = self._get_key_for_task(task_id)
            self.subscribed_to.discard(key)
            self._pubsub.unsubscribe(key)


class RedisBackend(BaseKeyValueStoreBackend, AsyncBackendMixin):
    """Redis task result store.

    It makes use of the following commands:
    GET, MGET, DEL, INCRBY, EXPIRE, SET, SETEX
    """

    ResultConsumer = ResultConsumer

    #: :pypi:`redis` client module.
    redis = redis

    #: Maximum number of connections in the pool.
    max_connections = None

    supports_autoexpire = True
    supports_native_join = True

    def __init__(self, host=None, port=None, db=None, password=None,
                 max_connections=None, url=None,
                 connection_pool=None, **kwargs):
        super(RedisBackend, self).__init__(expires_type=int, **kwargs)
        _get = self.app.conf.get
        if self.redis is None:
            raise ImproperlyConfigured(E_REDIS_MISSING.strip())

        if host and '://' in host:
            url, host = host, None

        self.max_connections = (
            max_connections or
            _get('redis_max_connections') or
            self.max_connections)
        self._ConnectionPool = connection_pool

        socket_timeout = _get('redis_socket_timeout')
        socket_connect_timeout = _get('redis_socket_connect_timeout')

        self.connparams = {
            'host': _get('redis_host') or 'localhost',
            'port': _get('redis_port') or 6379,
            'db': _get('redis_db') or 0,
            'password': _get('redis_password'),
            'max_connections': self.max_connections,
            'socket_timeout': socket_timeout and float(socket_timeout),
            'socket_connect_timeout':
                socket_connect_timeout and float(socket_connect_timeout),
        }

        # "redis_backend_use_ssl" must be a dict with the keys:
        # 'ssl_cert_reqs', 'ssl_ca_certs', 'ssl_certfile', 'ssl_keyfile'
        # (the same as "broker_use_ssl")
        ssl = _get('redis_backend_use_ssl')
        if ssl:
            self.connparams.update(ssl)
            self.connparams['connection_class'] = redis.SSLConnection

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
                raise ValueError(E_REDIS_SSL_CERT_REQS_MISSING_INVALID)

            if ssl_cert_reqs == CERT_OPTIONAL:
                logger.warning(W_REDIS_SSL_CERT_OPTIONAL)
            elif ssl_cert_reqs == CERT_NONE:
                logger.warning(W_REDIS_SSL_CERT_NONE)
            self.connparams['ssl_cert_reqs'] = ssl_cert_reqs

        self.url = url

        self.connection_errors, self.channel_errors = (
            get_redis_error_classes() if get_redis_error_classes
            else ((), ()))
        self.result_consumer = self.ResultConsumer(
            self, self.app, self.accept,
            self._pending_results, self._pending_messages,
        )

    def _params_from_url(self, url, defaults):
        scheme, host, port, _, password, path, query = _parse_url(url)
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
            connparams.pop('socket_connect_timeout')
        else:
            connparams['db'] = path

        ssl_param_keys = ['ssl_ca_certs', 'ssl_certfile', 'ssl_keyfile',
                          'ssl_cert_reqs']

        if scheme == 'redis':
            # If connparams or query string contain ssl params, raise error
            if (any(key in connparams for key in ssl_param_keys) or
                    any(key in query for key in ssl_param_keys)):
                raise ValueError(E_REDIS_SSL_PARAMS_AND_SCHEME_MISMATCH)

        if scheme == 'rediss':
            connparams['connection_class'] = redis.SSLConnection
            # The following parameters, if present in the URL, are encoded. We
            # must add the decoded values to connparams.
            for ssl_setting in ssl_param_keys:
                ssl_val = query.pop(ssl_setting, None)
                if ssl_val:
                    connparams[ssl_setting] = unquote(ssl_val)

        # db may be string and start with / like in kombu.
        db = connparams.get('db') or 0
        db = db.strip('/') if isinstance(db, string_t) else db
        connparams['db'] = int(db)

        for key, value in query.items():
            if key in redis.connection.URL_QUERY_ARGUMENT_PARSERS:
                query[key] = redis.connection.URL_QUERY_ARGUMENT_PARSERS[key](
                    value
                )

        # Query parameters override other parameters
        connparams.update(query)
        return connparams

    def on_task_call(self, producer, task_id):
        if not task_join_will_block():
            self.result_consumer.consume_from(task_id)

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
            **retry_policy)

    def on_connection_error(self, max_retries, exc, intervals, retries):
        tts = next(intervals)
        logger.error(
            E_LOST.strip(),
            retries, max_retries or 'Inf', humanize_seconds(tts, 'in '))
        return tts

    def set(self, key, value, **retry_policy):
        return self.ensure(self._set, (key, value), **retry_policy)

    def _set(self, key, value):
        with self.client.pipeline() as pipe:
            if self.expires:
                pipe.setex(key, self.expires, value)
            else:
                pipe.set(key, value)
            pipe.publish(key, value)
            pipe.execute()

    def forget(self, task_id):
        super(RedisBackend, self).forget(task_id)
        self.result_consumer.cancel_for(task_id)

    def delete(self, key):
        self.client.delete(key)

    def incr(self, key):
        return self.client.incr(key)

    def expire(self, key, value):
        return self.client.expire(key, value)

    def add_to_chord(self, group_id, result):
        self.client.incr(self.get_key_for_group(group_id, '.t'), 1)

    def _unpack_chord_result(self, tup, decode,
                             EXCEPTION_STATES=states.EXCEPTION_STATES,
                             PROPAGATE_STATES=states.PROPAGATE_STATES):
        _, tid, state, retval = decode(tup)
        if state in EXCEPTION_STATES:
            retval = self.exception_to_python(retval)
        if state in PROPAGATE_STATES:
            raise ChordError('Dependency {0} raised {1!r}'.format(tid, retval))
        return retval

    def apply_chord(self, header_result, body, **kwargs):
        # Overrides this to avoid calling GroupResult.save
        # pylint: disable=method-hidden
        # Note that KeyValueStoreBackend.__init__ sets self.apply_chord
        # if the implements_incr attr is set.  Redis backend doesn't set
        # this flag.
        pass

    def on_chord_part_return(self, request, state, result,
                             propagate=None, **kwargs):
        app = self.app
        tid, gid = request.id, request.group
        if not gid or not tid:
            return

        client = self.client
        jkey = self.get_key_for_group(gid, '.j')
        tkey = self.get_key_for_group(gid, '.t')
        result = self.encode_result(result, state)
        with client.pipeline() as pipe:
            pipeline = pipe \
                .rpush(jkey, self.encode([1, tid, state, result])) \
                .llen(jkey) \
                .get(tkey)

            if self.expires is not None:
                pipeline = pipeline \
                    .expire(jkey, self.expires) \
                    .expire(tkey, self.expires)

            _, readycount, totaldiff = pipeline.execute()[:3]

        totaldiff = int(totaldiff or 0)

        try:
            callback = maybe_signature(request.chord, app=app)
            total = callback['chord_size'] + totaldiff
            if readycount == total:
                decode, unpack = self.decode, self._unpack_chord_result
                with client.pipeline() as pipe:
                    resl, = pipe \
                        .lrange(jkey, 0, total) \
                        .execute()
                try:
                    callback.delay([unpack(tup, decode) for tup in resl])
                    with client.pipeline() as pipe:
                        _, _ = pipe \
                            .delete(jkey) \
                            .delete(tkey) \
                            .execute()
                except Exception as exc:  # pylint: disable=broad-except
                    logger.exception(
                        'Chord callback for %r raised: %r', request.group, exc)
                    return self.chord_error_from_stack(
                        callback,
                        ChordError('Callback error: {0!r}'.format(exc)),
                    )
        except ChordError as exc:
            logger.exception('Chord %r raised: %r', request.group, exc)
            return self.chord_error_from_stack(callback, exc)
        except Exception as exc:  # pylint: disable=broad-except
            logger.exception('Chord %r raised: %r', request.group, exc)
            return self.chord_error_from_stack(
                callback,
                ChordError('Join error: {0!r}'.format(exc)),
            )

    def _create_client(self, **params):
        return self._get_client()(
            connection_pool=self._get_pool(**params),
        )

    def _get_client(self):
        return self.redis.StrictRedis

    def _get_pool(self, **params):
        return self.ConnectionPool(**params)

    @property
    def ConnectionPool(self):
        if self._ConnectionPool is None:
            self._ConnectionPool = self.redis.ConnectionPool
        return self._ConnectionPool

    @cached_property
    def client(self):
        return self._create_client(**self.connparams)

    def __reduce__(self, args=(), kwargs=None):
        kwargs = {} if not kwargs else kwargs
        return super(RedisBackend, self).__reduce__(
            (self.url,), {'expires': self.expires},
        )

    @deprecated.Property(4.0, 5.0)
    def host(self):
        return self.connparams['host']

    @deprecated.Property(4.0, 5.0)
    def port(self):
        return self.connparams['port']

    @deprecated.Property(4.0, 5.0)
    def db(self):
        return self.connparams['db']

    @deprecated.Property(4.0, 5.0)
    def password(self):
        return self.connparams['password']


class SentinelBackend(RedisBackend):
    """Redis sentinel task result store."""

    sentinel = sentinel

    def __init__(self, *args, **kwargs):
        if self.sentinel is None:
            raise ImproperlyConfigured(E_REDIS_SENTINEL_MISSING.strip())

        super(SentinelBackend, self).__init__(*args, **kwargs)

    def _params_from_url(self, url, defaults):
        # URL looks like sentinel://0.0.0.0:26347/3;sentinel://0.0.0.0:26348/3.
        chunks = url.split(";")
        connparams = dict(defaults, hosts=[])
        for chunk in chunks:
            data = super(SentinelBackend, self)._params_from_url(
                url=chunk, defaults=defaults)
            connparams['hosts'].append(data)
        for param in ("host", "port", "db", "password"):
            connparams.pop(param)

        # Adding db/password in connparams to connect to the correct instance
        for param in ("db", "password"):
            if connparams['hosts'] and param in connparams['hosts'][0]:
                connparams[param] = connparams['hosts'][0].get(param)
        return connparams

    def _get_sentinel_instance(self, **params):
        connparams = params.copy()

        hosts = connparams.pop("hosts")
        result_backend_transport_opts = self.app.conf.get(
            "result_backend_transport_options", {})
        min_other_sentinels = result_backend_transport_opts.get(
            "min_other_sentinels", 0)
        sentinel_kwargs = result_backend_transport_opts.get(
            "sentinel_kwargs", {})

        sentinel_instance = self.sentinel.Sentinel(
            [(cp['host'], cp['port']) for cp in hosts],
            min_other_sentinels=min_other_sentinels,
            sentinel_kwargs=sentinel_kwargs,
            **connparams)

        return sentinel_instance

    def _get_pool(self, **params):
        sentinel_instance = self._get_sentinel_instance(**params)

        result_backend_transport_opts = self.app.conf.get(
            "result_backend_transport_options", {})
        master_name = result_backend_transport_opts.get("master_name", None)

        return sentinel_instance.master_for(
            service_name=master_name,
            redis_class=self._get_client(),
        ).connection_pool
