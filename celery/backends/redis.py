# -*- coding: utf-8 -*-
"""
    celery.backends.redis
    ~~~~~~~~~~~~~~~~~~~~~

    Redis result store backend.

"""
from __future__ import absolute_import

from functools import partial
from hashlib import sha1
from threading import local
from time import time
from uuid import uuid4

from kombu.utils import cached_property, retry_over_time
from kombu.utils.encoding import bytes_to_str, ensure_bytes
from kombu.utils.url import _parse_url

from celery import states
from celery.canvas import maybe_signature
from celery.exceptions import ChordError, TimeoutError, ImproperlyConfigured
from celery.five import items, string_t
from celery.utils import deprecated_property, strtobool
from celery.utils.functional import dictfilter
from celery.utils.log import get_logger
from celery.utils.timeutils import humanize_seconds

from .base import KeyValueStoreBackend

try:
    import redis
    from redis.client import PubSub, Script
    from redis.exceptions import ConnectionError, WatchError
    from kombu.transport.redis import get_redis_error_classes
    from select import select
except ImportError:                 # pragma: no cover
    redis = None                    # noqa
    PubSub = object                 # noqa
    Script = None                   # noqa
    ConnectionError = None          # noqa
    WatchError = None               # noqa
    get_redis_error_classes = None  # noqa

__all__ = ['RedisBackend']

REDIS_MISSING = """\
You need to install the redis library in order to use \
the Redis result store backend."""

logger = get_logger(__name__)
error = logger.error


class RedisClient(redis.StrictRedis):
    """Customized Redis client.

    Checks the version information of Redis server upon the first access
    of feature attributes (listed below), then sets the cached values of
    feature attributes in accordance with Redis version.

    :attribute:`supports_lua`: lua scripts are supported (2.6.0 or higher)
    :attribute:`supports_pttl`: millisecond precision of expiration timestamps
                                is supported (2.6.0 or higher)
    :attribute:`improved_ttl`: TTL and PTTL commands return -2 if a key
                               does not exist and -1 if no TTL specified
                               (2.8.0 or higher)

    This detection of features allows :meth:`RedisBackend.set` calls and
    :class:`RedisLock` methods to use lua scripts and millisecond timestamps
    if supported by the server, or fall back to WATCH/MULTI/EXEC pipelining.

    Also overrides :meth:`lock` to return :class:`RedisLock` objects by
    default and :meth:`pubsub` to return :class:`RedisPubSub` objects.
    """

    def __init__(self, *args, **kwargs):
        super(RedisClient, self).__init__(*args, **kwargs)
        self.features_detected = False
        self._supports_lua = False
        self._supports_pttl = False
        self._improved_ttl = False

    def _detect_features(self):
        """Check Redis server info and set feature support information."""
        if self.features_detected:
            return
        info = self.info()
        try:
            version_info = info.get('redis_version')
            if version_info is None:
                return
            version_info = version_info.split('.')
            major, minor = int(version_info[0]), int(version_info[1])
            self._supports_lua = major == 2 and minor >= 6 or major > 2
            self._supports_pttl = self._supports_lua
            self._improved_ttl = major == 2 and minor >= 8 or major > 2
        except ValueError:
            return
        finally:
            self.features_detected = True

    @cached_property
    def supports_lua(self):
        if not self.features_detected:
            self._detect_features()
        return self._supports_lua

    @cached_property
    def supports_pttl(self):
        if not self.features_detected:
            self._detect_features()
        return self._supports_pttl

    @cached_property
    def improved_ttl(self):
        if not self.features_detected:
            self._detect_features()
        return self._improved_ttl

    def lock(self, name, **kwargs):
        """Return :class:`RedisLock` objects instead of redis-py lock
        implementation by default."""
        lock_class = kwargs.pop('lock_class', RedisLock)
        return lock_class(self, name, **kwargs)

    def pubsub(self, **kwargs):
        """Return :class:`RedisPubSub` objects of custom PubSub
        implementation."""
        return RedisPubSub(self.connection_pool, **kwargs)


class RedisPubSub(PubSub):
    """Alternative implementation of redis-py PubSub interface class.

    redis-py up to version 2.10.3 supports only async calls of
    ``get_message()`` (i.e. if there is no message available, :const:`None`
    is returned immediately). As of version 2.10.3, there is a synchronous
    implementation of ``get_message()`` which waits for a specified number
    of seconds if no message is immediately avaiable, but it is still in a
    development trunk.
    So we re-implement it the same way it's implemented there, using
    :func:`select.select`.
    """

    def get_message(self, ignore_subscribe_messages=False, timeout=0):
        """Get the next message if one is available, otherwise None.

        :keyword timeout: If timeout is specified, the system will wait for
                          ``timeout`` seconds before returning.
                          Timeout should be specified as a floating point
                          number or :const:`None` (to block indefinitely).
        """

        pubsub = super(RedisPubSub, self)
        message = pubsub.get_message(ignore_subscribe_messages=
                                     ignore_subscribe_messages)
        if message is not None or timeout == 0:
            return message

        if timeout:
            started_at = time()
        block_for = timeout
        while message is None:
            if select([self.connection._sock], [], [], block_for)[0]:
                message = pubsub.get_message(ignore_subscribe_messages=
                                             ignore_subscribe_messages)
            if timeout:
                elapsed = time() - started_at
                if elapsed >= timeout or elapsed < 0.0:
                    break
                block_for = timeout - elapsed
        return message


class RedisLock(object):
    """An opportunistic implementation of locking with Redis.

    Supports lua scripts and traditional pipelining. Uses the feature
    detection of :class:`RedisClient` to choose the appropriate
    implementation.

    Also uses PUBLISH/SUBSCRIBE along with push/pop signals on a signal key
    to minimize possible waiting for the lock to be acquired.

    Other available implementations of Redis locks either do not support
    millisecond timeouts (BLPOP only supports second-precision timeouts)
    or are restricted to lua or pipelining only, or utilize less efficient
    polling spin-loops, which makes them sub-optimal.
    """
    ACQUIRED_SIGNAL = '1'
    RELEASED_SIGNAL = '0'

    ACQUIRE_SCRIPT = """
        if redis.call('setnx', KEYS[1], ARGV[1]) == 1 then
            redis.call('rpush', KEYS[2], '{0}')
            if ARGV[2] ~= '' then
                redis.call('pexpire', KEYS[1], ARGV[2])
                redis.call('pexpire', KEYS[2], ARGV[2])
            end
            return 1
        else
            return 0
        end
    """.format(ACQUIRED_SIGNAL)
    ACQUIRE_SCRIPT_SHA = sha1(ensure_bytes(ACQUIRE_SCRIPT)).hexdigest()

    RELEASE_SCRIPT = """
        local token = redis.call('get', KEYS[1])
        if token and token == ARGV[1] then
            if ARGV[2] ~= '' then
                redis.call('rpush', KEYS[2], '{0}')
            else
                redis.call('del', KEYS[2])
            end
            redis.call('publish', KEYS[1], '{0}')
            redis.call('del', KEYS[1])
            return 1
        else
            return 0
        end
    """.format(RELEASED_SIGNAL)
    RELEASE_SCRIPT_SHA = sha1(ensure_bytes(RELEASE_SCRIPT)).hexdigest()

    def __init__(self, client, name, expire=None, timeout=None,
                 thread_local=True, signal_key=None, **kwargs):
        """Creates a new :class:`RedisLock` instance.

        :param client: a :class:`RedisClient` instance to use.
        :param name: a unique identifier (name) of the lock.
        :keyword expire: a number of seconds after which the acquired lock
                         expires and is released automatically.
        :keyword timeout: a default number of seconds to wait until the lock
                          has been acquired.
        :keyword thread_local: use thread-local storage for the key token
                               (a compatibility option which should not affect
                               actual operation of the current implementation
                               of celery workers). :const:`True` by default.
        :keyword signal_key: use the specified key as a signal channel for
                             the lock. If not specified, the signal channel
                             key is generated automatically.
        """
        self.client = client
        self.name = name
        self.expire = expire
        self.timeout = timeout
        self.thread_local = bool(thread_local)
        self.signal_key = signal_key or 'lock-signal:' + name
        if thread_local:
            self.token = local()
            self.token.value = None
        else:
            self.token = None

    _not_specified = object()
    def acquire(self, timeout=_not_specified):
        """Try to acquire the lock in the specified amount of time.

        :keyword timeout: a number of seconds to wait for the lock. If not
                          specified, uses :attr:`timeout` default.
                          If :const:`None`, waits indefinitely.

        :returns :const:`True` if the lock has been acquired,
                 :const:`False` if the timeout has expired.
        """
        if timeout is self._not_specified:
            timeout = self.timeout
        token = self.token.value if self.thread_local else self.token
        if token is None:
            token = ensure_bytes(uuid4().hex)
        else:
            if ensure_bytes(self.client.get(self.name)) == token:
                # we already hold the lock
                return True

        started_at = time()
        block_for = timeout

        locked = self._acquire(token)
        if not locked and timeout == 0:
            return locked

        while not locked:
            if timeout:
                elapsed = time() - started_at
                if elapsed >= timeout or elapsed < 0.0:
                    break
                block_for = timeout - elapsed
            if self._wait_till_free(timeout=block_for):
                locked = self._acquire(token)

        if locked:
            if self.thread_local:
                self.token.value = token
            else:
                self.token = token
        return locked

    def release(self):
        """Try to release the lock if it is still locked by the same owner.

        :returns :const:`True` if unlocked successfully,
                 :const:`False` if the lock is not locked or already locked
                                by someone else.
        """
        if self.thread_local:
            token, self.token.value = self.token.value, None
        else:
            token, self.token = self.token, None
        if token is None:
            # not actually locked
            return False
        return self._release(token)

    def _acquire(self, token):
        if self.expire:
            if self.client.supports_pttl:
                lock_ttl = int(round(self.expire * 1000))
            else:
                # no millisecond precision, round to a minimum of 1 second
                lock_ttl = int(round(self.expire)) or 1
        else:
            lock_ttl = self.expire
        if self.client.supports_lua:
            # perform necessary actions with a lua script
            return self._lua_acquire(keys=(self.name, self.signal_key),
                                     args=(token, lock_ttl or ''))
        # try to create a lock key if it does not exist
        if self.client.setnx(self.name, token):
            # push the acquisition signal to the signal channel
            self.client.rpush(self.signal_key, self.ACQUIRED_SIGNAL)
            # set the expiration time for the lock and the signal channel
            if lock_ttl:
                # No lua means no millisecond precision, fall back to
                # expire. The actual value should already be correct.
                self.client.expire(self.name, lock_ttl)
                self.client.expire(self.signal_key, lock_ttl)
            return True
        # the lock already exists
        return False

    def _release(self, token):
        if self.client.supports_lua:
            # perform necessary actions with a lua script
            return self._lua_release(keys=(self.name, self.signal_key),
                                     args=(token, self.expire or ''))
        pipe = self.client.pipeline()
        pipe.watch(self.name)
        current_token = ensure_bytes(pipe.get(self.name))
        if current_token != token:
            # lock is already held by someone else
            pipe.reset()
            return False
        # queue the following actions in a transaction
        pipe.multi()
        if self.expire:
            # push the release signal to the signal channel
            pipe.rpush(self.signal_key, self.RELEASED_SIGNAL)
        else:
            # avoid clogging the database with signal 'tombstones'
            pipe.delete(self.signal_key)
        # send a notification to possible waiters via PUBLISH
        pipe.publish(self.name, self.RELEASED_SIGNAL)
        # release the lock
        pipe.delete(self.name)
        try:
            pipe.execute()
        except WatchError:
            return False
        else:
            return True

    @cached_property
    def _lua_acquire(self):
        script = Script(self.client, self.ACQUIRE_SCRIPT)
        script.sha = self.ACQUIRE_SCRIPT_SHA
        return script

    @cached_property
    def _lua_release(self):
        script = Script(self.client, self.RELEASE_SCRIPT)
        script.sha = self.RELEASE_SCRIPT_SHA
        return script

    def _wait_till_free(self, timeout=None):
        if timeout == 0:
            return not self.client.exists(self.name)

        pubsub = self.client.pubsub(ignore_subscribe_messages=True)
        pubsub.subscribe(self.name)
        try:
            # last chance to minimize the wait time in case we missed
            # the PUBLISH notification before actually subscrbing:
            # check the signal channel
            while 1:
                sig = self.client.lpop(self.signal_key)
                if sig is None:
                    break
                elif sig == self.RELEASED_SIGNAL:
                    return True

            started_at = time()
            block_for = timeout

            while 1:
                # check for how long we should actually wait
                if self.client.supports_pttl:
                    ttl = self.client.pttl(self.name)
                else:
                    ttl = self.client.ttl(self.name)

                if ttl == -2:
                    # the lock has expired between iterations
                    return True
                elif ttl == -1:
                    if (not self.client.improved_ttl and
                        not self.client.exists(self.name)):
                        # same as -2
                        return True
                    # no expiration; don't block indefinitely
                    block_for = block_for or 1
                else:
                    if self.client.supports_pttl:
                        # milliseconds to seconds
                        ttl /= 1000.0
                    block_for = min(block_for or ttl, ttl)

                message = pubsub.get_message(timeout=block_for)
                if (message and
                    message['type'] == 'message' and
                    message['channel'] == self.name and
                    message['data'] == self.RELEASED_SIGNAL):
                    return True

                if timeout:
                    elapsed = time() - started_at
                    if elapsed >= timeout or elapsed < 0.0:
                        return False
                    block_for = timeout - elapsed
        finally:
            pubsub.unsubscribe()
            pubsub.close()


class RedisBackend(KeyValueStoreBackend):
    """Redis task result store."""

    # lua script for faster set operation
    SET_SCRIPT = """
        if ARGV[2] == '' then
            redis.call('set', KEYS[1], ARGV[1])
        else
            redis.call('setex', KEYS[1], ARGV[2], ARGV[1])
        end
        redis.call('publish', KEYS[1], ARGV[1])
    """
    SET_SCRIPT_SHA = sha1(ensure_bytes(SET_SCRIPT)).hexdigest()

    #: redis-py client module.
    redis = redis
    #: redis client implementation.
    redis_client = RedisClient

    #: Maximium number of connections in the pool.
    max_connections = None

    supports_autoexpire = True
    supports_native_join = True
    implements_incr = True

    lock_keyprefix = 'celery-lock:'
    lock_signal_keyprefix = 'celery-lock-signal:'
    #: Default expiration time for a cache lock entry, in seconds.
    lock_ttl = 0.1
    #: Default timeout waiting for a cache lock before set operation.
    set_lock_timeout = 0.5

    def __init__(self, host=None, port=None, db=None, password=None,
                 expires=None, max_connections=None, url=None,
                 connection_pool=None, new_join=False, lock_ttl=None,
                 set_lock_timeout=None, **kwargs):
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
            'port': _get('PORT') or 6379,
            'db': _get('DB') or 0,
            'password': _get('PASSWORD'),
            'max_connections': self.max_connections,
        }
        if url:
            self.connparams = self._params_from_url(url, self.connparams)
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

        self.lock_ttl = (
            lock_ttl or _get('LOCK_TTL') or self.lock_ttl
        )
        self.set_lock_timeout = (
            set_lock_timeout or
            _get('SET_LOCK_TIMEOUT') or
            self.set_lock_timeout
        )

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

    def _encode_prefixes(self):
        super(RedisBackend, self)._encode_prefixes()
        self.lock_keyprefix = self.key_t(self.lock_keyprefix)
        self.lock_signal_keyprefix = self.key_t(self.lock_signal_keyprefix)

    def get_key_for_lock(self, cache_key, key=''):
        """Get the cache key for a cache lock on another cache key."""
        key_t = self.key_t
        return key_t('').join([
            self.lock_keyprefix, key_t(cache_key), key_t(key),
        ])

    def get_key_for_signal(self, cache_key, key=''):
        """Get the cache key for a signal entry of a cache lock."""
        key_t = self.key_t
        return key_t('').join([
            self.lock_signal_keyprefix, key_t(cache_key), key_t(key),
        ])

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
        lock_key = self.get_key_for_lock(key)
        signal_key = self.get_key_for_signal(key)
        lock = self.client.lock(lock_key, expire=self.lock_ttl,
                                timeout=self.set_lock_timeout,
                                signal_key=signal_key)
        # To use the PUBLISH notification we must place a lock to make
        # the operations of setting the task meta / sending the notification
        # and getting the task meta / subscribing to notifications
        # single transactions, otherwise we may miss the sent notification
        # before actually subscribing and wait much longer than needed.
        if not lock.acquire():
            raise TimeoutError('The operation timed out '
                               '({0}s).'.format(self.set_lock_timeout))
        try:
            if self.client.supports_lua:
                # perform necessary actions with a lua script
                return self._lua_set(keys=(key,), args=(value,
                                                        self.expires or ''))
            pipe = self.client.pipeline()
            pipe.multi()
            if self.expires:
                pipe.setex(key, self.expires, value)
            else:
                pipe.set(key, value)
            pipe.publish(key, value)
            pipe.execute()
        finally:
            lock.release()

    @cached_property
    def _lua_set(self):
        script = Script(self.client, self.SET_SCRIPT)
        script.sha = self.SET_SCRIPT_SHA
        return script

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

    @cached_property
    def client(self):
        return self.redis_client(
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

    def wait_for(self, task_id,
                 timeout=None, interval=0.5, no_ack=True, on_interval=None):
        if not interval:
            interval = 0.5
        if timeout:
            interval = min(timeout, interval)
            lock_ttl = min(timeout, self.lock_ttl) if self.lock_ttl \
                        else self.lock_ttl
        else:
            lock_ttl = self.lock_ttl
        task_key = self.get_key_for_task(task_id)
        lock_key = self.get_key_for_lock(task_key)
        signal_key = self.get_key_for_signal(task_key)
        lock = self.client.lock(lock_key, expire=lock_ttl,
                                signal_key=signal_key)

        # We need to lock the task meta entry to be sure that we won't miss
        # a PUBLISH notification in between checking the task state and
        # subscribing, forcing us to wait till the timeout.
        started_at = time()
        # avoid blocking on the lock indefinitely
        block_for = timeout or interval
        if not lock.acquire(timeout=block_for):
            raise TimeoutError('The operation timed out.')

        try:
            if on_interval:
                on_interval()

            meta = self.get_task_meta(task_id)
            if meta['status'] in states.READY_STATES:
                return meta

            pubsub = self.client.pubsub(ignore_subscribe_messages=True)
            pubsub.subscribe(task_key)
        finally:
            lock.release()

        if timeout:
            elapsed = abs(time() - started_at)
            block_for = min(interval, timeout - elapsed)
        try:
            while 1:
                message = pubsub.get_message(timeout=block_for)
                if (message and
                    message['type'] == 'message' and
                    message['channel'] == task_key
                    ):
                    meta = self.decode_result(message['data'])
                    if meta['status'] in states.READY_STATES:
                        break
                if on_interval:
                    on_interval()
                if timeout:
                    elapsed = time() - started_at
                    if elapsed >= timeout or elapsed < 0.0:
                        # ensure we have not missed the notification
                        meta = self.get_task_meta(task_id)
                        if meta['status'] in states.READY_STATES:
                            break
                        else:
                            raise TimeoutError('The operation timed out.')
                    block_for = min(interval, timeout - elapsed)
        finally:
            pubsub.unsubscribe()
            pubsub.close()

        return meta
    wait_for.__doc__ = KeyValueStoreBackend.wait_for.__doc__

    def get_many(self, task_ids, timeout=None, interval=0.5, no_ack=True,
                 READY_STATES=states.READY_STATES):
        if not interval:
            interval = 0.5
        if timeout:
            interval = min(timeout, interval)
            lock_ttl = min(timeout, self.lock_ttl) if self.lock_ttl \
                        else self.lock_ttl
        else:
            lock_ttl = self.lock_ttl

        ids = task_ids if isinstance(task_ids, set) else set(task_ids)
        cached_ids = set()
        cache = self._cache
        for task_id in ids:
            try:
                cached = cache[task_id]
            except KeyError:
                pass
            else:
                if cached['status'] in READY_STATES:
                    yield bytes_to_str(task_id), cached
                    cached_ids.add(task_id)
        ids.difference_update(cached_ids)
        if not ids:
            raise StopIteration

        id_list = list(ids)
        task_keys = [self.get_key_for_task(t) for t in id_list]
        key_tasks = dict(zip(task_keys, id_list))
        locks = [self.client.lock(self.get_key_for_lock(tk), expire=lock_ttl,
                                  signal_key=self.get_key_for_signal(tk))
                 for tk in task_keys]

        started_at = time()
        block_for = timeout or interval
        for lock in locks:
            while 1:
                if lock.acquire(timeout=block_for):
                    break
                if timeout:
                    elapsed = time() - started_at
                    if elapsed >= timeout or elapsed < 0.0:
                        raise TimeoutError('The operation timed out '
                                           '({0}s).'.format(timeout))
                    block_for = timeout - elapsed

        try:
            r = self.mget(task_keys)

            pubsub = self.client.pubsub(ignore_subscribe_messages=True)
            for task_key in task_keys:
                pubsub.subscribe(task_key)
        finally:
            for lock in locks:
                lock.release()

        if timeout:
            elapsed = abs(time() - started_at)
            block_for = timeout - elapsed
        try:
            r = self._mget_to_results(r, id_list)
            cache.update(r)
            ids.difference_update(set(bytes_to_str(v) for v in r))
            for key, value in items(r):
                yield bytes_to_str(key), value

            while ids:
                message = pubsub.get_message(timeout=block_for)
                if message and message['type'] == 'message':
                    key = key_tasks.get(message['channel'])
                    if key in ids:
                        value = self.decode_result(message['data'])
                        ids.remove(key)
                        yield bytes_to_str(key), value
                if timeout:
                    elapsed = time() - started_at
                    if elapsed >= timeout or elapsed < 0.0:
                        raise TimeoutError('The operation timed out '
                                           '({0}s).'.format(timeout))
                    block_for = timeout - elapsed
        finally:
            pubsub.unsubscribe()
            pubsub.close()
