# -*- coding: utf-8 -*-
"""
    celery.app.base
    ~~~~~~~~~~~~~~~

    Actual App instance implementation.

"""
from __future__ import absolute_import

import os
import threading
import warnings

from collections import defaultdict, deque
from copy import deepcopy
from operator import attrgetter
from functools import wraps

from amqp import promise
try:
    from billiard.util import register_after_fork
except ImportError:
    register_after_fork = None
from kombu.clocks import LamportClock
from kombu.common import oid_from
from kombu.utils import cached_property, uuid

from celery import platforms
from celery import signals
from celery._state import (
    _task_stack, get_current_app, _set_current_app, set_default_app,
    _register_app, get_current_worker_task, connect_on_app_finalize,
    _announce_app_finalized,
)
from celery.exceptions import AlwaysEagerIgnored, ImproperlyConfigured
from celery.five import items, values
from celery.loaders import get_loader_cls
from celery.local import PromiseProxy, maybe_evaluate
from celery.utils import abstract
from celery.utils import gen_task_name
from celery.utils.dispatch import Signal
from celery.utils.functional import first, maybe_list, head_from_fun
from celery.utils.imports import instantiate, symbol_by_name
from celery.utils.objects import FallbackContext, mro_lookup

from .annotations import prepare as prepare_annotations
from .defaults import DEFAULTS, find_deprecated_settings
from .registry import TaskRegistry
from .utils import (
    AppPickler, Settings, bugreport, _unpickle_app, _unpickle_app_v2, appstr,
)

# Load all builtin tasks
from . import builtins  # noqa

__all__ = ['Celery']

_EXECV = os.environ.get('FORKED_BY_MULTIPROCESSING')
BUILTIN_FIXUPS = {
    'celery.fixups.django:fixup',
}

ERR_ENVVAR_NOT_SET = """\
The environment variable {0!r} is not set,
and as such the configuration could not be loaded.
Please set this variable and make it point to
a configuration module."""

_after_fork_registered = False


def app_has_custom(app, attr):
    return mro_lookup(app.__class__, attr, stop=(Celery, object),
                      monkey_patched=[__name__])


def _unpickle_appattr(reverse_name, args):
    """Given an attribute name and a list of args, gets
    the attribute from the current app and calls it."""
    return get_current_app()._rgetattr(reverse_name)(*args)


def _global_after_fork(obj):
    # Previously every app would call:
    #    `register_after_fork(app, app._after_fork)`
    # but this created a leak as `register_after_fork` stores concrete object
    # references and once registered an object cannot be removed without
    # touching and iterating over the private afterfork registry list.
    #
    # See Issue #1949
    from celery import _state
    from multiprocessing import util as mputil
    for app in _state._apps:
        try:
            app._after_fork(obj)
        except Exception as exc:
            if mputil._logger:
                mputil._logger.info(
                    'after forker raised exception: %r', exc, exc_info=1)


def _ensure_after_fork():
    global _after_fork_registered
    _after_fork_registered = True
    if register_after_fork is not None:
        register_after_fork(_global_after_fork, _global_after_fork)


class Celery(object):
    """Celery application.

    :param main: Name of the main module if running as `__main__`.
        This is used as a prefix for task names.
    :keyword broker: URL of the default broker used.
    :keyword loader: The loader class, or the name of the loader class to use.
                     Default is :class:`celery.loaders.app.AppLoader`.
    :keyword backend: The result store backend class, or the name of the
                      backend class to use. Default is the value of the
                      :setting:`CELERY_RESULT_BACKEND` setting.
    :keyword amqp: AMQP object or class name.
    :keyword events: Events object or class name.
    :keyword log: Log object or class name.
    :keyword control: Control object or class name.
    :keyword set_as_current:  Make this the global current app.
    :keyword tasks: A task registry or the name of a registry class.
    :keyword include: List of modules every worker should import.
    :keyword fixups: List of fixup plug-ins (see e.g.
        :mod:`celery.fixups.django`).
    :keyword autofinalize: If set to False a :exc:`RuntimeError`
        will be raised if the task registry or tasks are used before
        the app is finalized.

    """
    #: This is deprecated, use :meth:`reduce_keys` instead
    Pickler = AppPickler

    SYSTEM = platforms.SYSTEM
    IS_OSX, IS_WINDOWS = platforms.IS_OSX, platforms.IS_WINDOWS

    #: Name of the `__main__` module.  Required for standalone scripts.
    #:
    #: If set this will be used instead of `__main__` when automatically
    #: generating task names.
    main = None

    #: Custom options for command-line programs.
    #: See :ref:`extending-commandoptions`
    user_options = None

    #: Custom bootsteps to extend and modify the worker.
    #: See :ref:`extending-bootsteps`.
    steps = None

    amqp_cls = 'celery.app.amqp:AMQP'
    backend_cls = None
    events_cls = 'celery.events:Events'
    loader_cls = 'celery.loaders.app:AppLoader'
    log_cls = 'celery.app.log:Logging'
    control_cls = 'celery.app.control:Control'
    task_cls = 'celery.app.task:Task'
    registry_cls = TaskRegistry
    _fixups = None
    _pool = None
    _conf = None
    builtin_fixups = BUILTIN_FIXUPS

    #: Signal sent when app is loading configuration.
    on_configure = None

    #: Signal sent after app has prepared the configuration.
    on_after_configure = None

    #: Signal sent after app has been finalized.
    on_after_finalize = None

    #: ignored
    accept_magic_kwargs = False

    def __init__(self, main=None, loader=None, backend=None,
                 amqp=None, events=None, log=None, control=None,
                 set_as_current=True, tasks=None, broker=None, include=None,
                 changes=None, config_source=None, fixups=None, task_cls=None,
                 autofinalize=True, **kwargs):
        self.clock = LamportClock()
        self.main = main
        self.amqp_cls = amqp or self.amqp_cls
        self.events_cls = events or self.events_cls
        self.loader_cls = loader or self.loader_cls
        self.log_cls = log or self.log_cls
        self.control_cls = control or self.control_cls
        self.task_cls = task_cls or self.task_cls
        self.set_as_current = set_as_current
        self.registry_cls = symbol_by_name(self.registry_cls)
        self.user_options = defaultdict(set)
        self.steps = defaultdict(set)
        self.autofinalize = autofinalize

        self.configured = False
        self._config_source = config_source
        self._pending_defaults = deque()
        self._pending_periodic_tasks = deque()

        self.finalized = False
        self._finalize_mutex = threading.Lock()
        self._pending = deque()
        self._tasks = tasks
        if not isinstance(self._tasks, TaskRegistry):
            self._tasks = TaskRegistry(self._tasks or {})

        # If the class defines a custom __reduce_args__ we need to use
        # the old way of pickling apps, which is pickling a list of
        # args instead of the new way that pickles a dict of keywords.
        self._using_v1_reduce = app_has_custom(self, '__reduce_args__')

        # these options are moved to the config to
        # simplify pickling of the app object.
        self._preconf = changes or {}
        if broker:
            self._preconf['BROKER_URL'] = broker
        if backend:
            self._preconf['CELERY_RESULT_BACKEND'] = backend
        if include:
            self._preconf['CELERY_IMPORTS'] = include

        # - Apply fixups.
        self.fixups = set(self.builtin_fixups) if fixups is None else fixups
        # ...store fixup instances in _fixups to keep weakrefs alive.
        self._fixups = [symbol_by_name(fixup)(self) for fixup in self.fixups]

        if self.set_as_current:
            self.set_current()

        # Signals
        if self.on_configure is None:
            # used to be a method pre 4.0
            self.on_configure = Signal()
        self.on_after_configure = Signal()
        self.on_after_finalize = Signal()

        self.on_init()
        _register_app(self)

    def set_current(self):
        """Makes this the current app for this thread."""
        _set_current_app(self)

    def set_default(self):
        """Makes this the default app for all threads."""
        set_default_app(self)

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        self.close()

    def close(self):
        """Close any open pool connections and do any other steps necessary
        to clean up after the application.

        Only necessary for dynamically created apps for which you can
        use the with statement instead::

            with Celery(set_as_current=False) as app:
                with app.connection() as conn:
                    pass
        """
        self._maybe_close_pool()

    def on_init(self):
        """Optional callback called at init."""
        pass

    def start(self, argv=None):
        """Run :program:`celery` using `argv`.

        Uses :data:`sys.argv` if `argv` is not specified.

        """
        return instantiate(
            'celery.bin.celery:CeleryCommand',
            app=self).execute_from_commandline(argv)

    def worker_main(self, argv=None):
        """Run :program:`celery worker` using `argv`.

        Uses :data:`sys.argv` if `argv` is not specified.

        """
        return instantiate(
            'celery.bin.worker:worker',
            app=self).execute_from_commandline(argv)

    def task(self, *args, **opts):
        """Decorator to create a task class out of any callable.

        Examples:

        .. code-block:: python

            @app.task
            def refresh_feed(url):
                return …

        with setting extra options:

        .. code-block:: python

            @app.task(exchange="feeds")
            def refresh_feed(url):
                return …

        .. admonition:: App Binding

            For custom apps the task decorator will return a proxy
            object, so that the act of creating the task is not performed
            until the task is used or the task registry is accessed.

            If you are depending on binding to be deferred, then you must
            not access any attributes on the returned object until the
            application is fully set up (finalized).

        """
        if _EXECV and opts.get('lazy', True):
            # When using execv the task in the original module will point to a
            # different app, so doing things like 'add.request' will point to
            # a different task instance.  This makes sure it will always use
            # the task instance from the current app.
            # Really need a better solution for this :(
            from . import shared_task
            return shared_task(*args, lazy=False, **opts)

        def inner_create_task_cls(shared=True, filter=None, lazy=True, **opts):
            _filt = filter  # stupid 2to3

            def _create_task_cls(fun):
                if shared:
                    def cons(app):
                        return app._task_from_fun(fun, **opts)
                    cons.__name__ = fun.__name__
                    connect_on_app_finalize(cons)
                if not lazy or self.finalized:
                    ret = self._task_from_fun(fun, **opts)
                else:
                    # return a proxy object that evaluates on first use
                    ret = PromiseProxy(self._task_from_fun, (fun,), opts,
                                       __doc__=fun.__doc__)
                    self._pending.append(ret)
                if _filt:
                    return _filt(ret)
                return ret

            return _create_task_cls

        if len(args) == 1:
            if callable(args[0]):
                return inner_create_task_cls(**opts)(*args)
            raise TypeError('argument 1 to @task() must be a callable')
        if args:
            raise TypeError(
                '@task() takes exactly 1 argument ({0} given)'.format(
                    sum([len(args), len(opts)])))
        return inner_create_task_cls(**opts)

    def _task_from_fun(self, fun, name=None, base=None, bind=False, **options):
        if not self.finalized and not self.autofinalize:
            raise RuntimeError('Contract breach: app not finalized')
        name = name or self.gen_task_name(fun.__name__, fun.__module__)
        base = base or self.Task

        if name not in self._tasks:
            run = fun if bind else staticmethod(fun)
            task = type(fun.__name__, (base,), dict({
                'app': self,
                'name': name,
                'run': run,
                '_decorated': True,
                '__doc__': fun.__doc__,
                '__module__': fun.__module__,
                '__header__': staticmethod(head_from_fun(fun, bound=bind)),
                '__wrapped__': run}, **options))()
            self._tasks[task.name] = task
            task.bind(self)  # connects task to this app

            autoretry_for = tuple(options.get('autoretry_for', ()))
            retry_kwargs = options.get('retry_kwargs', {})

            if autoretry_for and not hasattr(task, '_orig_run'):

                @wraps(task.run)
                def run(*args, **kwargs):
                    try:
                        return task._orig_run(*args, **kwargs)
                    except autoretry_for as exc:
                        raise task.retry(exc=exc, **retry_kwargs)

                task._orig_run, task.run = task.run, run
        else:
            task = self._tasks[name]
        return task

    def gen_task_name(self, name, module):
        return gen_task_name(self, name, module)

    def finalize(self, auto=False):
        """Finalizes the app by loading built-in tasks,
        and evaluating pending task decorators."""
        with self._finalize_mutex:
            if not self.finalized:
                if auto and not self.autofinalize:
                    raise RuntimeError('Contract breach: app not finalized')
                self.finalized = True
                _announce_app_finalized(self)

                pending = self._pending
                while pending:
                    maybe_evaluate(pending.popleft())

                for task in values(self._tasks):
                    task.bind(self)

                self.on_after_finalize.send(sender=self)

    def add_defaults(self, fun):
        """Add default configuration from dict ``d``.

        If the argument is a callable function then it will be regarded
        as a promise, and it won't be loaded until the configuration is
        actually needed.

        This method can be compared to::

            >>> celery.conf.update(d)

        with a difference that 1) no copy will be made and 2) the dict will
        not be transferred when the worker spawns child processes, so
        it's important that the same configuration happens at import time
        when pickle restores the object on the other side.

        """
        if not callable(fun):
            d, fun = fun, lambda: d
        if self.configured:
            return self._conf.add_defaults(fun())
        self._pending_defaults.append(fun)

    def config_from_object(self, obj, silent=False, force=False):
        """Reads configuration from object, where object is either
        an object or the name of a module to import.

        :keyword silent: If true then import errors will be ignored.

        :keyword force:  Force reading configuration immediately.
            By default the configuration will be read only when required.

        .. code-block:: pycon

            >>> celery.config_from_object("myapp.celeryconfig")

            >>> from myapp import celeryconfig
            >>> celery.config_from_object(celeryconfig)

        """
        self._config_source = obj
        if force or self.configured:
            self._conf = None
            return self.loader.config_from_object(obj, silent=silent)

    def config_from_envvar(self, variable_name, silent=False, force=False):
        """Read configuration from environment variable.

        The value of the environment variable must be the name
        of a module to import.

        .. code-block:: pycon

            >>> os.environ["CELERY_CONFIG_MODULE"] = "myapp.celeryconfig"
            >>> celery.config_from_envvar("CELERY_CONFIG_MODULE")

        """
        module_name = os.environ.get(variable_name)
        if not module_name:
            if silent:
                return False
            raise ImproperlyConfigured(
                ERR_ENVVAR_NOT_SET.format(variable_name))
        return self.config_from_object(module_name, silent=silent, force=force)

    def config_from_cmdline(self, argv, namespace='celery'):
        (self._conf if self.configured else self.conf).update(
            self.loader.cmdline_config_parser(argv, namespace)
        )

    def setup_security(self, allowed_serializers=None, key=None, cert=None,
                       store=None, digest='sha1', serializer='json'):
        """Setup the message-signing serializer.

        This will affect all application instances (a global operation).

        Disables untrusted serializers and if configured to use the ``auth``
        serializer will register the auth serializer with the provided settings
        into the Kombu serializer registry.

        :keyword allowed_serializers:  List of serializer names, or content_types
            that should be exempt from being disabled.
        :keyword key: Name of private key file to use.
            Defaults to the :setting:`CELERY_SECURITY_KEY` setting.
        :keyword cert: Name of certificate file to use.
            Defaults to the :setting:`CELERY_SECURITY_CERTIFICATE` setting.
        :keyword store: Directory containing certificates.
            Defaults to the :setting:`CELERY_SECURITY_CERT_STORE` setting.
        :keyword digest: Digest algorithm used when signing messages.
            Default is ``sha1``.
        :keyword serializer: Serializer used to encode messages after
            they have been signed.  See :setting:`CELERY_TASK_SERIALIZER` for
            the serializers supported.
            Default is ``json``.

        """
        from celery.security import setup_security
        return setup_security(allowed_serializers, key, cert,
                              store, digest, serializer, app=self)

    def autodiscover_tasks(self, packages=None,
                           related_name='tasks', force=False):
        """Try to autodiscover and import modules with a specific name (by
        default 'tasks').

        If the name is empty, this will be delegated to fixups (e.g. Django).

        For example if you have an (imagined) directory tree like this::

            foo/__init__.py
               tasks.py
               models.py

            bar/__init__.py
                tasks.py
                models.py

            baz/__init__.py
                models.py

        Then calling ``app.autodiscover_tasks(['foo', bar', 'baz'])`` will
        result in the modules ``foo.tasks`` and ``bar.tasks`` being imported.

        :param packages: List of packages to search.
            This argument may also be a callable, in which case the
            value returned is used (for lazy evaluation).
        :keyword related_name: The name of the module to find.  Defaults
            to "tasks", which means it look for "module.tasks" for every
            module in ``packages``.
        :keyword force: By default this call is lazy so that the actual
            autodiscovery will not happen until an application imports the
            default modules.  Forcing will cause the autodiscovery to happen
            immediately.

        """
        if force:
            return self._autodiscover_tasks(packages, related_name)
        signals.import_modules.connect(promise(
            self._autodiscover_tasks, (packages, related_name),
        ), weak=False, sender=self)

    def _autodiscover_tasks(self, packages, related_name, **kwargs):
        if packages:
            return self._autodiscover_tasks_from_names(packages, related_name)
        return self._autodiscover_tasks_from_fixups(related_name)

    def _autodiscover_tasks_from_names(self, packages, related_name):
        # packages argument can be lazy
        return self.loader.autodiscover_tasks(
            packages() if callable(packages) else packages, related_name,
        )

    def _autodiscover_tasks_from_fixups(self, related_name):
        return self._autodiscover_tasks_from_names([
            pkg for fixup in self._fixups
            for pkg in fixup.autodiscover_tasks()
            if hasattr(fixup, 'autodiscover_tasks')
        ], related_name=related_name)

    def send_task(self, name, args=None, kwargs=None, countdown=None,
                  eta=None, task_id=None, producer=None, connection=None,
                  router=None, result_cls=None, expires=None,
                  publisher=None, link=None, link_error=None,
                  add_to_parent=True, group_id=None, retries=0, chord=None,
                  reply_to=None, time_limit=None, soft_time_limit=None,
                  root_id=None, parent_id=None, route_name=None,
                  shadow=None, **options):
        """Send task by name.

        :param name: Name of task to call (e.g. `"tasks.add"`).
        :keyword result_cls: Specify custom result class. Default is
            using :meth:`AsyncResult`.

        Otherwise supports the same arguments as :meth:`@-Task.apply_async`.

        """
        amqp = self.amqp
        task_id = task_id or uuid()
        producer = producer or publisher  # XXX compat
        router = router or amqp.router
        conf = self.conf
        if conf.CELERY_ALWAYS_EAGER:  # pragma: no cover
            warnings.warn(AlwaysEagerIgnored(
                'CELERY_ALWAYS_EAGER has no effect on send_task',
            ), stacklevel=2)
        options = router.route(options, route_name or name, args, kwargs)

        message = amqp.create_task_message(
            task_id, name, args, kwargs, countdown, eta, group_id,
            expires, retries, chord,
            maybe_list(link), maybe_list(link_error),
            reply_to or self.oid, time_limit, soft_time_limit,
            self.conf.CELERY_SEND_TASK_SENT_EVENT,
            root_id, parent_id, shadow,
        )

        if connection:
            producer = amqp.Producer(connection)
        with self.producer_or_acquire(producer) as P:
            self.backend.on_task_call(P, task_id)
            amqp.send_task_message(P, name, message, **options)
        result = (result_cls or self.AsyncResult)(task_id)
        if add_to_parent:
            parent = get_current_worker_task()
            if parent:
                parent.add_trail(result)
        return result

    def connection(self, hostname=None, userid=None, password=None,
                   virtual_host=None, port=None, ssl=None,
                   connect_timeout=None, transport=None,
                   transport_options=None, heartbeat=None,
                   login_method=None, failover_strategy=None, **kwargs):
        """Establish a connection to the message broker.

        :param url: Either the URL or the hostname of the broker to use.

        :keyword hostname: URL, Hostname/IP-address of the broker.
            If an URL is used, then the other argument below will
            be taken from the URL instead.
        :keyword userid: Username to authenticate as.
        :keyword password: Password to authenticate with
        :keyword virtual_host: Virtual host to use (domain).
        :keyword port: Port to connect to.
        :keyword ssl: Defaults to the :setting:`BROKER_USE_SSL` setting.
        :keyword transport: defaults to the :setting:`BROKER_TRANSPORT`
                 setting.

        :returns :class:`kombu.Connection`:

        """
        conf = self.conf
        return self.amqp.Connection(
            hostname or conf.BROKER_URL,
            userid or conf.BROKER_USER,
            password or conf.BROKER_PASSWORD,
            virtual_host or conf.BROKER_VHOST,
            port or conf.BROKER_PORT,
            transport=transport or conf.BROKER_TRANSPORT,
            ssl=self.either('BROKER_USE_SSL', ssl),
            heartbeat=heartbeat,
            login_method=login_method or conf.BROKER_LOGIN_METHOD,
            failover_strategy=(
                failover_strategy or conf.BROKER_FAILOVER_STRATEGY
            ),
            transport_options=dict(
                conf.BROKER_TRANSPORT_OPTIONS, **transport_options or {}
            ),
            connect_timeout=self.either(
                'BROKER_CONNECTION_TIMEOUT', connect_timeout
            ),
        )
    broker_connection = connection

    def _acquire_connection(self, pool=True):
        """Helper for :meth:`connection_or_acquire`."""
        if pool:
            return self.pool.acquire(block=True)
        return self.connection()

    def connection_or_acquire(self, connection=None, pool=True, *_, **__):
        """For use within a with-statement to get a connection from the pool
        if one is not already provided.

        :keyword connection: If not provided, then a connection will be
                             acquired from the connection pool.
        """
        return FallbackContext(connection, self._acquire_connection, pool=pool)
    default_connection = connection_or_acquire  # XXX compat

    def producer_or_acquire(self, producer=None):
        """For use within a with-statement to get a producer from the pool
        if one is not already provided

        :keyword producer: If not provided, then a producer will be
                           acquired from the producer pool.

        """
        return FallbackContext(
            producer, self.amqp.producer_pool.acquire, block=True,
        )
    default_producer = producer_or_acquire  # XXX compat

    def prepare_config(self, c):
        """Prepare configuration before it is merged with the defaults."""
        return find_deprecated_settings(c)

    def now(self):
        """Return the current time and date as a
        :class:`~datetime.datetime` object."""
        return self.loader.now(utc=self.conf.CELERY_ENABLE_UTC)

    def mail_admins(self, subject, body, fail_silently=False):
        """Sends an email to the admins in the :setting:`ADMINS` setting."""
        conf = self.conf
        if conf.ADMINS:
            to = [admin_email for _, admin_email in conf.ADMINS]
            return self.loader.mail_admins(
                subject, body, fail_silently, to=to,
                sender=conf.SERVER_EMAIL,
                host=conf.EMAIL_HOST,
                port=conf.EMAIL_PORT,
                user=conf.EMAIL_HOST_USER,
                password=conf.EMAIL_HOST_PASSWORD,
                timeout=conf.EMAIL_TIMEOUT,
                use_ssl=conf.EMAIL_USE_SSL,
                use_tls=conf.EMAIL_USE_TLS,
                charset=conf.EMAIL_CHARSET,
            )

    def select_queues(self, queues=None):
        """Select a subset of queues, where queues must be a list of queue
        names to keep."""

        return self.amqp.queues.select(queues)

    def either(self, default_key, *values):
        """Fallback to the value of a configuration key if none of the
        `*values` are true."""
        return first(None, values) or self.conf.get(default_key)

    def bugreport(self):
        """Return a string with information useful for the Celery core
        developers when reporting a bug."""
        return bugreport(self)

    def _get_backend(self):
        from celery.backends import get_backend_by_url
        backend, url = get_backend_by_url(
            self.backend_cls or self.conf.CELERY_RESULT_BACKEND,
            self.loader)
        return backend(app=self, url=url)

    def _load_config(self):
        if isinstance(self.on_configure, Signal):
            self.on_configure.send(sender=self)
        else:
            # used to be a method pre 4.0
            self.on_configure()
        if self._config_source:
            self.loader.config_from_object(self._config_source)
        defaults = dict(deepcopy(DEFAULTS), **self._preconf)
        self.configured = True
        s = self._conf = Settings(
            {}, [self.prepare_config(self.loader.conf), defaults],
        )
        # load lazy config dict initializers.
        pending_def = self._pending_defaults
        while pending_def:
            s.add_defaults(maybe_evaluate(pending_def.popleft()()))

        # load lazy periodic tasks
        pending_beat = self._pending_periodic_tasks
        while pending_beat:
            self._add_periodic_task(*pending_beat.popleft())

        # Settings.__setitem__ method, set Settings.change
        if self._preconf:
            for key, value in items(self._preconf):
                setattr(s, key, value)
        self.on_after_configure.send(sender=self, source=s)
        return s

    def _after_fork(self, obj_):
        self._maybe_close_pool()

    def _maybe_close_pool(self):
        if self._pool:
            self._pool.force_close_all()
            self._pool = None
            amqp = self.__dict__.get('amqp')
            if amqp is not None and amqp._producer_pool is not None:
                amqp._producer_pool.force_close_all()
                amqp._producer_pool = None

    def signature(self, *args, **kwargs):
        """Return a new :class:`~celery.canvas.Signature` bound to this app.

        See :meth:`~celery.signature`

        """
        kwargs['app'] = self
        return self.canvas.signature(*args, **kwargs)

    def add_periodic_task(self, schedule, sig,
                          args=(), kwargs=(), name=None, **opts):
        key, entry = self._sig_to_periodic_task_entry(
            schedule, sig, args, kwargs, name, **opts)
        if self.configured:
            self._add_periodic_task(key, entry)
        else:
            self._pending_periodic_tasks.append((key, entry))
        return key

    def _sig_to_periodic_task_entry(self, schedule, sig,
                                    args=(), kwargs={}, name=None, **opts):
        sig = (sig.clone(args, kwargs)
               if isinstance(sig, abstract.CallableSignature)
               else self.signature(sig.name, args, kwargs))
        return name or repr(sig), {
            'schedule': schedule,
            'task': sig.name,
            'args': sig.args,
            'kwargs': sig.kwargs,
            'options': dict(sig.options, **opts),
        }

    def _add_periodic_task(self, key, entry):
        self._conf.CELERYBEAT_SCHEDULE[key] = entry

    def create_task_cls(self):
        """Creates a base task class using default configuration
        taken from this app."""
        return self.subclass_with_self(
            self.task_cls, name='Task', attribute='_app',
            keep_reduce=True, abstract=True,
        )

    def subclass_with_self(self, Class, name=None, attribute='app',
                           reverse=None, keep_reduce=False, **kw):
        """Subclass an app-compatible class by setting its app attribute
        to be this app instance.

        App-compatible means that the class has a class attribute that
        provides the default app it should use, e.g.
        ``class Foo: app = None``.

        :param Class: The app-compatible class to subclass.
        :keyword name: Custom name for the target class.
        :keyword attribute: Name of the attribute holding the app,
                            default is 'app'.

        """
        Class = symbol_by_name(Class)
        reverse = reverse if reverse else Class.__name__

        def __reduce__(self):
            return _unpickle_appattr, (reverse, self.__reduce_args__())

        attrs = dict({attribute: self}, __module__=Class.__module__,
                     __doc__=Class.__doc__, **kw)
        if not keep_reduce:
            attrs['__reduce__'] = __reduce__

        return type(name or Class.__name__, (Class,), attrs)

    def _rgetattr(self, path):
        return attrgetter(path)(self)

    def __repr__(self):
        return '<{0} {1}>'.format(type(self).__name__, appstr(self))

    def __reduce__(self):
        if self._using_v1_reduce:
            return self.__reduce_v1__()
        return (_unpickle_app_v2, (self.__class__, self.__reduce_keys__()))

    def __reduce_v1__(self):
        # Reduce only pickles the configuration changes,
        # so the default configuration doesn't have to be passed
        # between processes.
        return (
            _unpickle_app,
            (self.__class__, self.Pickler) + self.__reduce_args__(),
        )

    def __reduce_keys__(self):
        """Return keyword arguments used to reconstruct the object
        when unpickling."""
        return {
            'main': self.main,
            'changes': self._conf.changes if self._conf else self._preconf,
            'loader': self.loader_cls,
            'backend': self.backend_cls,
            'amqp': self.amqp_cls,
            'events': self.events_cls,
            'log': self.log_cls,
            'control': self.control_cls,
            'fixups': self.fixups,
            'config_source': self._config_source,
            'task_cls': self.task_cls,
        }

    def __reduce_args__(self):
        """Deprecated method, please use :meth:`__reduce_keys__` instead."""
        return (self.main, self._conf.changes if self._conf else {},
                self.loader_cls, self.backend_cls, self.amqp_cls,
                self.events_cls, self.log_cls, self.control_cls,
                False, self._config_source)

    @cached_property
    def Worker(self):
        """Worker application. See :class:`~@Worker`."""
        return self.subclass_with_self('celery.apps.worker:Worker')

    @cached_property
    def WorkController(self, **kwargs):
        """Embeddable worker. See :class:`~@WorkController`."""
        return self.subclass_with_self('celery.worker:WorkController')

    @cached_property
    def Beat(self, **kwargs):
        """Celerybeat scheduler application.

        See :class:`~@Beat`.

        """
        return self.subclass_with_self('celery.apps.beat:Beat')

    @cached_property
    def Task(self):
        """Base task class for this app."""
        return self.create_task_cls()

    @cached_property
    def annotations(self):
        return prepare_annotations(self.conf.CELERY_ANNOTATIONS)

    @cached_property
    def AsyncResult(self):
        """Create new result instance.

        See :class:`celery.result.AsyncResult`.

        """
        return self.subclass_with_self('celery.result:AsyncResult')

    @cached_property
    def ResultSet(self):
        return self.subclass_with_self('celery.result:ResultSet')

    @cached_property
    def GroupResult(self):
        """Create new group result instance.

        See :class:`celery.result.GroupResult`.

        """
        return self.subclass_with_self('celery.result:GroupResult')

    @cached_property
    def TaskSet(self):  # XXX compat
        """Deprecated! Please use :class:`celery.group` instead."""
        return self.subclass_with_self('celery.task.sets:TaskSet')

    @cached_property
    def TaskSetResult(self):  # XXX compat
        """Deprecated! Please use :attr:`GroupResult` instead."""
        return self.subclass_with_self('celery.result:TaskSetResult')

    @property
    def pool(self):
        """Broker connection pool: :class:`~@pool`.

        This attribute is not related to the workers concurrency pool.

        """
        if self._pool is None:
            _ensure_after_fork()
            limit = self.conf.BROKER_POOL_LIMIT
            self._pool = self.connection().Pool(limit=limit)
        return self._pool

    @property
    def current_task(self):
        """The instance of the task that is being executed, or
        :const:`None`."""
        return _task_stack.top

    @cached_property
    def oid(self):
        return oid_from(self)

    @cached_property
    def amqp(self):
        """AMQP related functionality: :class:`~@amqp`."""
        return instantiate(self.amqp_cls, app=self)

    @cached_property
    def backend(self):
        """Current backend instance."""
        return self._get_backend()

    @property
    def conf(self):
        """Current configuration."""
        if self._conf is None:
            self._load_config()
        return self._conf

    @conf.setter
    def conf(self, d):  # noqa
        self._conf = d

    @cached_property
    def control(self):
        """Remote control: :class:`~@control`."""
        return instantiate(self.control_cls, app=self)

    @cached_property
    def events(self):
        """Consuming and sending events: :class:`~@events`."""
        return instantiate(self.events_cls, app=self)

    @cached_property
    def loader(self):
        """Current loader instance."""
        return get_loader_cls(self.loader_cls)(app=self)

    @cached_property
    def log(self):
        """Logging: :class:`~@log`."""
        return instantiate(self.log_cls, app=self)

    @cached_property
    def canvas(self):
        from celery import canvas
        return canvas

    @cached_property
    def tasks(self):
        """Task registry.

        Accessing this attribute will also finalize the app.

        """
        self.finalize(auto=True)
        return self._tasks

    @cached_property
    def timezone(self):
        """Current timezone for this app.

        This is a cached property taking the time zone from the
        :setting:`CELERY_TIMEZONE` setting.

        """
        from celery.utils.timeutils import timezone
        conf = self.conf
        tz = conf.CELERY_TIMEZONE
        if not tz:
            return (timezone.get_timezone('UTC') if conf.CELERY_ENABLE_UTC
                    else timezone.local)
        return timezone.get_timezone(conf.CELERY_TIMEZONE)
App = Celery  # compat
