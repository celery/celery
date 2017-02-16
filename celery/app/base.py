# -*- coding: utf-8 -*-
"""Actual App instance implementation."""
import os
import threading
import warnings

from collections import UserDict, defaultdict, deque
from datetime import datetime
from operator import attrgetter
from types import ModuleType
from typing import (
    Any, Callable, ContextManager, Dict, List,
    Mapping, MutableMapping, Optional, Set, Sequence, Tuple, Union,
)

from amqp.types import SSLArg
from kombu import pools
from kombu.clocks import Clock, LamportClock
from kombu.common import oid_from
from kombu.utils.compat import register_after_fork
from kombu.utils.objects import cached_property
from kombu.utils.uuid import uuid
from kombu.types import ConnectionT, ProducerT, ResourceT
from vine import starpromise
from vine.utils import wraps

from celery import platforms
from celery import signals
from celery._state import (
    _task_stack, get_current_app, _set_current_app, set_default_app,
    _register_app, _deregister_app,
    get_current_worker_task, connect_on_app_finalize,
    _announce_app_finalized,
)
from celery.exceptions import AlwaysEagerIgnored, ImproperlyConfigured
from celery.loaders import get_loader_cls
from celery.local import PromiseProxy, maybe_evaluate
from celery.types import (
    AppT, AppAMQPT, AppControlT, AppEventsT, AppLogT,
    BackendT, BeatT, LoaderT, ResultT, RouterT, ScheduleT,
    SignalT, SignatureT, TaskT, TaskRegistryT, WorkerT,
)
from celery.utils import abstract
from celery.utils.collections import AttributeDictMixin
from celery.utils.dispatch import Signal
from celery.utils.functional import first, maybe_list, head_from_fun
from celery.utils.time import timezone
from celery.utils.imports import gen_task_name, instantiate, symbol_by_name
from celery.utils.log import get_logger
from celery.utils.objects import FallbackContext, mro_lookup

from .annotations import prepare as prepare_annotations
from . import backends
from .defaults import find_deprecated_settings
from .registry import TaskRegistry
from .utils import (
    AppPickler, Settings,
    bugreport, _unpickle_app, _unpickle_app_v2,
    _old_key_to_new, _new_key_to_old,
    appstr, detect_settings,
)

# Load all builtin tasks
from . import builtins  # noqa

__all__ = ['Celery']

logger = get_logger(__name__)

BUILTIN_FIXUPS = {
    'celery.fixups.django:fixup',
}
USING_EXECV = os.environ.get('FORKED_BY_MULTIPROCESSING')

ERR_ENVVAR_NOT_SET = """
The environment variable {0!r} is not set,
and as such the configuration could not be loaded.

Please set this variable and make sure it points to
a valid configuration module.

Example:
    {0}="proj.celeryconfig"
"""


def app_has_custom(app: AppT, attr: str) -> bool:
    """Return true if app has customized method `attr`.

    Note:
        This is used for optimizations in cases where we know
        how the default behavior works, but need to account
        for someone using inheritance to override a method/property.
    """
    return mro_lookup(app.__class__, attr, stop={Celery, object},
                      monkey_patched=[__name__])


def _unpickle_appattr(reverse_name: str, args: Tuple) -> Any:
    """Unpickle app."""
    # Given an attribute name and a list of args, gets
    # the attribute from the current app and calls it.
    return get_current_app()._rgetattr(reverse_name)(*args)


def _after_fork_cleanup_app(app: AppT) -> None:
    # This is used with multiprocessing.register_after_fork,
    # so need to be at module level.
    try:
        app._after_fork()
    except Exception as exc:  # pylint: disable=broad-except
        logger.info('after forker raised exception: %r', exc, exc_info=1)


class PendingConfiguration(UserDict, AttributeDictMixin):
    # `app.conf` will be of this type before being explicitly configured,
    # meaning the app can keep any configuration set directly
    # on `app.conf` before the `app.config_from_object` call.
    #
    # accessing any key will finalize the configuration,
    # replacing `app.conf` with a concrete settings object.

    callback: Callable[[], MutableMapping] = None
    _data: MutableMapping = None

    def __init__(self,
                 conf: MutableMapping,
                 callback: Callable[[], MutableMapping]) -> None:
        object.__setattr__(self, '_data', conf)
        object.__setattr__(self, 'callback', callback)

    def __setitem__(self, key: str, value: Any) -> None:
        self._data[key] = value

    def clear(self) -> None:
        self._data.clear()

    def update(self, *args, **kwargs) -> None:
        self._data.update(*args, **kwargs)

    def setdefault(self, key: str, value: Any) -> Any:
        return self._data.setdefault(key, value)

    def __contains__(self, key: str) -> bool:
        # XXX will not show finalized configuration
        # setdefault will cause `key in d` to happen,
        # so for setdefault to be lazy, so does contains.
        return key in self._data

    def __len__(self) -> int:
        return len(self.data)

    def __repr__(self) -> str:
        return repr(self.data)

    @cached_property
    def data(self) -> MutableMapping:
        return self.callback()


@abstract.AbstractApp.register
class Celery:
    """Celery application.

    Arguments:
        main (str): Name of the main module if running as `__main__`.
            This is used as the prefix for auto-generated task names.

    Keyword Arguments:
        broker (str): URL of the default broker used.
        backend (Union[str, type]): The result store backend class,
            or the name of the backend class to use.

            Default is the value of the :setting:`result_backend` setting.
        autofinalize (bool): If set to False a :exc:`RuntimeError`
            will be raised if the task registry or tasks are used before
            the app is finalized.
        set_as_current (bool):  Make this the global current app.
        include (List[str]): List of modules every worker should import.

        amqp (Union[str, type]): AMQP object or class name.
        events (Union[str, type]): Events object or class name.
        log (Union[str, type]): Log object or class name.
        control (Union[str, type]): Control object or class name.
        tasks (Union[str, type]): A task registry, or the name of
            a registry class.
        fixups (List[str]): List of fix-up plug-ins (e.g., see
            :mod:`celery.fixups.django`).
        config_source (Union[str, type]): Take configuration from a class,
            or object.  Attributes may include any setings described in
            the documentation.
    """

    #: This is deprecated, use :meth:`reduce_keys` instead
    Pickler = AppPickler

    SYSTEM = platforms.SYSTEM
    IS_macOS, IS_WINDOWS = platforms.IS_macOS, platforms.IS_WINDOWS

    clock: Clock = None

    #: Name of the `__main__` module.  Required for standalone scripts.
    #:
    #: If set this will be used instead of `__main__` when automatically
    #: generating task names.
    main: str = None

    #: Custom options for command-line programs.
    #: See :ref:`extending-commandoptions`
    user_options: MutableMapping[str, Set] = None

    #: Custom bootsteps to extend and modify the worker.
    #: See :ref:`extending-bootsteps`.
    steps: MutableMapping[str, Set] = None

    builtin_fixups = BUILTIN_FIXUPS

    amqp_cls: Union[str, type] = 'celery.app.amqp:AMQP'
    backend_cls: Union[str, type] = None
    events_cls: Union[str, type] = 'celery.app.events:Events'
    loader_cls: Union[str, type] = None
    log_cls: Union[str, type] = 'celery.app.log:Logging'
    control_cls: Union[str, type] = 'celery.app.control:Control'
    task_cls: Union[str, type] = 'celery.app.task:Task'
    registry_cls: Union[str, type] = TaskRegistry

    _fixups: List = None
    _pool: ResourceT = None
    _conf: MutableMapping = None
    _after_fork_registered: bool = False

    #: Signal sent when app is loading configuration.
    on_configure: SignatureT = None

    #: Signal sent after app has prepared the configuration.
    on_after_configure: SignalT = None

    #: Signal sent after app has been finalized.
    on_after_finalize: SignalT = None

    #: Signal sent by every new process after fork.
    on_after_fork: SignalT = None

    def __init__(self,
                 main: str = None,
                 *,
                 loader: Union[str, type] = None,
                 backend: Union[str, type] = None,
                 amqp: Union[str, type] = None,
                 events: Union[str, type] = None,
                 log: Union[str, type] = None,
                 control: Union[str, type] = None,
                 set_as_current: bool = True,
                 tasks: Union[str, type] = None,
                 broker: str = None,
                 include: Sequence[str] = None,
                 changes: MutableMapping = None,
                 config_source: str = None,
                 fixups: Sequence[Union[str, type]] = None,
                 task_cls: Union[str, type] = None,
                 autofinalize: bool = True,
                 namespace: str = None,
                 strict_typing: bool = True,
                 **kwargs) -> None:
        self.clock = LamportClock()
        self.main = main
        self.amqp_cls = amqp or self.amqp_cls
        self.events_cls = events or self.events_cls
        self.loader_cls = loader or self._get_default_loader()
        self.log_cls = log or self.log_cls
        self.control_cls = control or self.control_cls
        self.task_cls = task_cls or self.task_cls
        self.set_as_current = set_as_current
        self.registry_cls = symbol_by_name(self.registry_cls)
        self.user_options = defaultdict(set)
        self.steps = defaultdict(set)
        self.autofinalize = autofinalize
        self.namespace = namespace
        self.strict_typing = strict_typing

        self.configured = False
        self._config_source = config_source
        self._pending_defaults = deque()
        self._pending_periodic_tasks = deque()

        self.finalized = False
        self._finalize_mutex = threading.Lock()
        self._pending = deque()
        self._tasks = tasks
        if not isinstance(self._tasks, TaskRegistry):
            self._tasks = self.registry_cls(self._tasks or {})

        # If the class defines a custom __reduce_args__ we need to use
        # the old way of pickling apps: pickling a list of
        # args instead of the new way that pickles a dict of keywords.
        self._using_v1_reduce = app_has_custom(self, '__reduce_args__')

        # these options are moved to the config to
        # simplify pickling of the app object.
        self._preconf = changes or {}
        self._preconf_set_by_auto = set()
        self.__autoset('broker_url', broker)
        self.__autoset('result_backend', backend)
        self.__autoset('include', include)
        self._conf = Settings(
            PendingConfiguration(
                self._preconf, self._finalize_pending_conf),
            prefix=self.namespace,
            keys=(_old_key_to_new, _new_key_to_old),
        )

        # - Apply fix-ups.
        self.fixups = set(self.builtin_fixups) if fixups is None else fixups
        # ...store fixup instances in _fixups to keep weakrefs alive.
        self._fixups = [symbol_by_name(fixup)(self) for fixup in self.fixups]

        if self.set_as_current:
            self.set_current()

        # Signals
        if self.on_configure is None:
            # used to be a method pre 4.0
            self.on_configure = Signal(name='app.on_configure')
        self.on_after_configure = Signal(
            name='app.on_after_configure',
            providing_args={'source'},
        )
        self.on_after_finalize = Signal(name='app.on_after_finalize')
        self.on_after_fork = Signal(name='app.on_after_fork')

        self.on_init()
        _register_app(self)

    def _get_default_loader(self) -> Union[str, type]:
        # the --loader command-line argument sets the environment variable.
        return (
            os.environ.get('CELERY_LOADER') or
            self.loader_cls or
            'celery.loaders.app:AppLoader'
        )

    def on_init(self) -> None:
        """Optional callback called at init."""
        ...

    def __autoset(self, key: str, value: Any) -> None:
        if value:
            self._preconf[key] = value
            self._preconf_set_by_auto.add(key)

    def set_current(self) -> None:
        """Make this the current app for this thread."""
        _set_current_app(self)

    def set_default(self) -> None:
        """Make this the default app for all threads."""
        set_default_app(self)

    def _ensure_after_fork(self) -> None:
        if not self._after_fork_registered:
            self._after_fork_registered = True
            if register_after_fork is not None:
                register_after_fork(self, _after_fork_cleanup_app)

    def close(self) -> None:
        """Clean up after the application.

        Only necessary for dynamically created apps, and you should
        probably use the :keyword:`with` statement instead.

        Example:
            >>> with Celery(set_as_current=False) as app:
            ...     with app.connection_for_write() as conn:
            ...         pass
        """
        self._pool = None
        _deregister_app(self)

    def start(self, argv: List[str] = None) -> None:
        """Run :program:`celery` using `argv`.

        Uses :data:`sys.argv` if `argv` is not specified.
        """
        instantiate(
            'celery.bin.celery:CeleryCommand', app=self
        ).execute_from_commandline(argv)

    def worker_main(self, argv: List[str] = None) -> None:
        """Run :program:`celery worker` using `argv`.

        Uses :data:`sys.argv` if `argv` is not specified.
        """
        instantiate(
            'celery.bin.worker:worker', app=self
        ).execute_from_commandline(argv)

    def task(self, *args, **opts) -> Union[Callable, TaskT]:
        """Decorator to create a task class out of any callable.

        Examples:
            .. code-block:: python

                @app.task
                def refresh_feed(url):
                    store_feed(feedparser.parse(url))

            with setting extra options:

            .. code-block:: python

                @app.task(exchange='feeds')
                def refresh_feed(url):
                    return store_feed(feedparser.parse(url))

        Note:
            App Binding: For custom apps the task decorator will return
            a proxy object, so that the act of creating the task is not
            performed until the task is used or the task registry is accessed.

            If you're depending on binding to be deferred, then you must
            not access any attributes on the returned object until the
            application is fully set up (finalized).
        """
        if USING_EXECV and opts.get('lazy', True):
            # When using execv the task in the original module will point to a
            # different app, so doing things like 'add.request' will point to
            # a different task instance.  This makes sure it will always use
            # the task instance from the current app.
            # Really need a better solution for this :(
            from . import shared_task
            return shared_task(*args, lazy=False, **opts)

        def inner_create_task_cls(
                *,
                shared: bool = True,
                filter: Callable = None,
                lazy: bool = True,
                **opts) -> Callable:
            _filt = filter  # stupid 2to3

            def _create_task_cls(fun: Callable) -> TaskT:
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

    def _task_from_fun(self, fun: Callable,
                       *,
                       name: str = None,
                       base: type = None,
                       bind: bool = False,
                       **options) -> TaskT:
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
            # for some reason __qualname__ cannot be set in type()
            # so we have to set it here.
            try:
                task.__qualname__ = fun.__qualname__
            except AttributeError:
                pass
            self._tasks[task.name] = task
            task.bind(self)  # connects task to this app

            autoretry_for = tuple(options.get('autoretry_for', ()))
            retry_kwargs = options.get('retry_kwargs', {})

            if autoretry_for and not hasattr(task, '_orig_run'):

                @wraps(task.run)
                def run(*args, **kwargs) -> Any:
                    try:
                        return task._orig_run(*args, **kwargs)
                    except autoretry_for as exc:
                        raise task.retry(exc=exc, **retry_kwargs)

                task._orig_run, task.run = task.run, run
        else:
            task = self._tasks[name]
        return task

    def register_task(self, task: TaskT) -> TaskT:
        """Utility for registering a task-based class.

        Note:
            This is here for compatibility with old Celery 1.0
            style task classes, you should not need to use this for
            new projects.
        """
        if not task.name:
            task_cls = type(task)
            task.name = self.gen_task_name(
                task_cls.__name__, task_cls.__module__)
        self.tasks[task.name] = task
        task._app = self
        task.bind(self)
        return task

    def gen_task_name(self, name: str, module: str) -> str:
        return gen_task_name(self, name, module)

    def finalize(self, auto: bool = False) -> None:
        """Finalize the app.

        This loads built-in tasks, evaluates pending task decorators,
        reads configuration, etc.
        """
        with self._finalize_mutex:
            if not self.finalized:
                if auto and not self.autofinalize:
                    raise RuntimeError('Contract breach: app not finalized')
                self.finalized = True
                _announce_app_finalized(self)

                pending = self._pending
                while pending:
                    maybe_evaluate(pending.popleft())

                for task in self._tasks.values():
                    task.bind(self)

                self.on_after_finalize.send(sender=self)

    def add_defaults(self, fun: Union[Callable, Mapping]) -> None:
        """Add default configuration from dict ``d``.

        If the argument is a callable function then it will be regarded
        as a promise, and it won't be loaded until the configuration is
        actually needed.

        This method can be compared to:

        .. code-block:: pycon

            >>> celery.conf.update(d)

        with a difference that 1) no copy will be made and 2) the dict will
        not be transferred when the worker spawns child processes, so
        it's important that the same configuration happens at import time
        when pickle restores the object on the other side.
        """
        if not callable(fun):
            d, fun = fun, lambda: d
        if self.configured:
            self._conf.add_defaults(fun())
        else:
            self._pending_defaults.append(fun)

    def config_from_object(self, obj: Any,
                           *,
                           silent: bool = False,
                           force: bool = False,
                           namespace: str = None) -> None:
        """Read configuration from object.

        Object is either an actual object or the name of a module to import.

        Example:
            >>> celery.config_from_object('myapp.celeryconfig')

            >>> from myapp import celeryconfig
            >>> celery.config_from_object(celeryconfig)

        Arguments:
            silent (bool): If true then import errors will be ignored.
            force (bool): Force reading configuration immediately.
                By default the configuration will be read only when required.
        """
        self._config_source = obj
        self.namespace = namespace or self.namespace
        if force or self.configured:
            self._conf = None
            if self.loader.config_from_object(obj, silent=silent):
                conf = self.conf  # noqa

    def config_from_envvar(self, variable_name: str,
                           *,
                           silent: bool = False,
                           force: bool = False) -> None:
        """Read configuration from environment variable.

        The value of the environment variable must be the name
        of a module to import.

        Example:
            >>> os.environ['CELERY_CONFIG_MODULE'] = 'myapp.celeryconfig'
            >>> celery.config_from_envvar('CELERY_CONFIG_MODULE')
        """
        module_name = os.environ.get(variable_name)
        if module_name:
            self.config_from_object(module_name, silent=silent, force=force)
        elif not silent:
            raise ImproperlyConfigured(
                ERR_ENVVAR_NOT_SET.strip().format(variable_name))

    def config_from_cmdline(self, argv: List[str],
                            namespace: str = 'celery') -> None:
        self._conf.update(
            self.loader.cmdline_config_parser(argv, namespace)
        )

    def setup_security(self,
                       allowed_serializers: Set[str] = None,
                       *,
                       key: str = None,
                       cert: str = None,
                       store: str = None,
                       digest: str = 'sha1',
                       serializer: str = 'json') -> None:
        """Setup the message-signing serializer.

        This will affect all application instances (a global operation).

        Disables untrusted serializers and if configured to use the ``auth``
        serializer will register the ``auth`` serializer with the provided
        settings into the Kombu serializer registry.

        Arguments:
            allowed_serializers (Set[str]): List of serializer names, or
                content_types that should be exempt from being disabled.
            key (str): Name of private key file to use.
                Defaults to the :setting:`security_key` setting.
            cert (str): Name of certificate file to use.
                Defaults to the :setting:`security_certificate` setting.
            store (str): Directory containing certificates.
                Defaults to the :setting:`security_cert_store` setting.
            digest (str): Digest algorithm used when signing messages.
                Default is ``sha1``.
            serializer (str): Serializer used to encode messages after
                they've been signed.  See :setting:`task_serializer` for
                the serializers supported.  Default is ``json``.
        """
        from celery.security import setup_security
        setup_security(allowed_serializers, key, cert,
                       store, digest, serializer, app=self)

    def autodiscover_tasks(self,
                           packages: Sequence[str] = None,
                           *,
                           related_name: str = 'tasks',
                           force: bool = False) -> None:
        """Auto-discover task modules.

        Searches a list of packages for a "tasks.py" module (or use
        related_name argument).

        If the name is empty, this will be delegated to fix-ups (e.g., Django).

        For example if you have a directory layout like this:

        .. code-block:: text

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

        Arguments:
            packages (List[str]): List of packages to search.
                This argument may also be a callable, in which case the
                value returned is used (for lazy evaluation).
            related_name (str): The name of the module to find.  Defaults
                to "tasks": meaning "look for 'module.tasks' for every
                module in ``packages``."
            force (bool): By default this call is lazy so that the actual
                auto-discovery won't happen until an application imports
                the default modules.  Forcing will cause the auto-discovery
                to happen immediately.
        """
        if force:
            self._autodiscover_tasks(packages, related_name)
        else:
            signals.import_modules.connect(starpromise(
                self._autodiscover_tasks, packages, related_name,
            ), weak=False, sender=self)

    def _autodiscover_tasks(
            self, packages: Sequence[str], related_name: str,
            **kwargs) -> None:
        if packages:
            self._autodiscover_tasks_from_names(packages, related_name)
        else:
            self._autodiscover_tasks_from_fixups(related_name)

    def _autodiscover_tasks_from_names(
            self, packages: Sequence[str], related_name: str) -> None:
        # packages argument can be lazy
        self.loader.autodiscover_tasks(
            packages() if callable(packages) else packages, related_name,
        )

    def _autodiscover_tasks_from_fixups(self, related_name: str) -> None:
        self._autodiscover_tasks_from_names([
            pkg for fixup in self._fixups
            for pkg in fixup.autodiscover_tasks()
            if hasattr(fixup, 'autodiscover_tasks')
        ], related_name=related_name)

    def send_task(self, name: str,
                  args: Sequence = None,
                  kwargs: Mapping = None,
                  countdown: float = None,
                  eta: datetime = None,
                  task_id: str = None,
                  producer: ProducerT = None,
                  connection: ConnectionT = None,
                  router: RouterT = None,
                  result_cls: type = None,
                  expires: Union[float, datetime] = None,
                  link: Union[SignatureT, Sequence[SignatureT]]=None,
                  link_error: Union[SignatureT, Sequence[SignatureT]] = None,
                  add_to_parent: bool = True,
                  group_id: str = None,
                  retries: int = 0,
                  chord: SignatureT = None,
                  reply_to: str = None,
                  time_limit: float = None,
                  soft_time_limit: float = None,
                  root_id: str = None,
                  parent_id: str = None,
                  route_name: str = None,
                  shadow: str = None,
                  chain: List[SignatureT] = None,
                  task_type: TaskT = None,
                  **options) -> ResultT:
        """Send task by name.

        Supports the same arguments as :meth:`@-Task.apply_async`.

        Arguments:
            name (str): Name of task to call (e.g., `"tasks.add"`).
            result_cls (~@AsyncResult): Specify custom result class.
        """
        parent = have_parent = None
        amqp = self.amqp
        task_id = task_id or uuid()
        router = router or amqp.router
        conf = self.conf
        if conf.task_always_eager:  # pragma: no cover
            warnings.warn(AlwaysEagerIgnored(
                'task_always_eager has no effect on send_task',
            ), stacklevel=2)
        options = router.route(
            options, route_name or name, args, kwargs, task_type)

        if not root_id or not parent_id:
            parent = self.current_worker_task
            if parent:
                if not root_id:
                    root_id = parent.request.root_id or parent.request.id
                if not parent_id:
                    parent_id = parent.request.id

        message = amqp.create_task_message(
            task_id, name, args, kwargs, countdown, eta, group_id,
            expires, retries, chord,
            maybe_list(link), maybe_list(link_error),
            reply_to or self.oid, time_limit, soft_time_limit,
            self.conf.task_send_sent_event,
            root_id, parent_id, shadow, chain,
        )

        if connection:
            producer = amqp.Producer(connection, auto_declare=False)
        with self.producer_or_acquire(producer) as P:
            with P.connection._reraise_as_library_errors():
                self.backend.on_task_call(P, task_id)
                amqp.send_task_message(P, name, message, **options)
        result = (result_cls or self.AsyncResult)(task_id)
        if add_to_parent:
            if not have_parent:
                parent, have_parent = self.current_worker_task, True
            if parent:
                parent.add_trail(result)
        return result

    def connection_for_read(self, url: str = None, **kwargs) -> ConnectionT:
        """Establish connection used for consuming.

        See Also:
            :meth:`connection` for supported arguments.
        """
        return self._connection(url or self.conf.broker_read_url, **kwargs)

    def connection_for_write(self, url: str = None, **kwargs) -> ConnectionT:
        """Establish connection used for producing.

        See Also:
            :meth:`connection` for supported arguments.
        """
        return self._connection(url or self.conf.broker_write_url, **kwargs)

    def connection(self,
                   hostname: str = None,
                   userid: str = None,
                   password: str = None,
                   virtual_host: str = None,
                   port: int = None,
                   ssl: SSLArg = None,
                   connect_timeout: float = None,
                   transport: Any = None,
                   transport_options: Mapping = None,
                   heartbeat: float = None,
                   login_method: str = None,
                   failover_strategy: str = None,
                   **kwargs) -> ConnectionT:
        """Establish a connection to the message broker.

        Please use :meth:`connection_for_read` and
        :meth:`connection_for_write` instead, to convey the intent
        of use for this connection.

        Arguments:
            url: Either the URL or the hostname of the broker to use.
            hostname (str): URL, Hostname/IP-address of the broker.
                If a URL is used, then the other argument below will
                be taken from the URL instead.
            userid (str): Username to authenticate as.
            password (str): Password to authenticate with
            virtual_host (str): Virtual host to use (domain).
            port (int): Port to connect to.
            ssl (bool, Dict): Defaults to the :setting:`broker_use_ssl`
                setting.
            transport (str): defaults to the :setting:`broker_transport`
                setting.
            transport_options (Dict): Dictionary of transport specific options.
            heartbeat (int): AMQP Heartbeat in seconds (``pyamqp`` only).
            login_method (str): Custom login method to use (AMQP only).
            failover_strategy (str, Callable): Custom failover strategy.
            **kwargs: Additional arguments to :class:`kombu.Connection`.

        Returns:
            kombu.Connection: the lazy connection instance.
        """
        return self.connection_for_write(
            hostname or self.conf.broker_write_url,
            userid=userid, password=password,
            virtual_host=virtual_host, port=port, ssl=ssl,
            connect_timeout=connect_timeout, transport=transport,
            transport_options=transport_options, heartbeat=heartbeat,
            login_method=login_method, failover_strategy=failover_strategy,
            **kwargs
        )

    def _connection(self, url: str,
                    userid: str = None,
                    password: str = None,
                    virtual_host: str = None,
                    port: int = None,
                    ssl: SSLArg = None,
                    connect_timeout: float = None,
                    transport: Any = None,
                    transport_options: Mapping = None,
                    heartbeat: float = None,
                    login_method: str = None,
                    failover_strategy: str = None,
                    **kwargs) -> ConnectionT:
        conf = self.conf
        return self.amqp.Connection(
            url,
            userid or conf.broker_user,
            password or conf.broker_password,
            virtual_host or conf.broker_vhost,
            port or conf.broker_port,
            transport=transport or conf.broker_transport,
            ssl=self.either('broker_use_ssl', ssl),
            heartbeat=heartbeat,
            login_method=login_method or conf.broker_login_method,
            failover_strategy=(
                failover_strategy or conf.broker_failover_strategy
            ),
            transport_options=dict(
                conf.broker_transport_options, **transport_options or {}
            ),
            connect_timeout=self.either(
                'broker_connection_timeout', connect_timeout
            ),
        )
    broker_connection = connection

    def _acquire_connection(self, pool: bool = True) -> ConnectionT:
        """Helper for :meth:`connection_or_acquire`."""
        if pool:
            return self.pool.acquire(block=True)
        return self.connection_for_write()

    def connection_or_acquire(self, connection: ConnectionT = None,
                              pool: bool = True, *_, **__) -> ContextManager:
        """Context used to acquire a connection from the pool.

        For use within a :keyword:`with` statement to get a connection
        from the pool if one is not already provided.

        Arguments:
            connection (kombu.Connection): If not provided, a connection
                will be acquired from the connection pool.
        """
        return FallbackContext(connection, self._acquire_connection, pool=pool)

    def producer_or_acquire(self,
                            producer: ProducerT = None) -> ContextManager:
        """Context used to acquire a producer from the pool.

        For use within a :keyword:`with` statement to get a producer
        from the pool if one is not already provided

        Arguments:
            producer (kombu.Producer): If not provided, a producer
                will be acquired from the producer pool.
        """
        return FallbackContext(
            producer, self.producer_pool.acquire, block=True,
        )

    def prepare_config(self, c: Mapping) -> Mapping:
        """Prepare configuration before it is merged with the defaults."""
        return find_deprecated_settings(c)

    def now(self) -> datetime:
        """Return the current time and date as a datetime."""
        return self.loader.now(utc=self.conf.enable_utc)

    def select_queues(self, queues: Sequence[str] = None) -> None:
        """Select subset of queues.

        Arguments:
            queues (Sequence[str]): a list of queue names to keep.
        """
        self.amqp.queues.select(queues)

    def either(self, default_key: str, *defaults) -> Any:
        """Get key from configuration or use default values.

        Fallback to the value of a configuration key if none of the
        `*values` are true.
        """
        return first(None, [
            first(None, defaults), starpromise(self.conf.get, default_key),
        ])

    def bugreport(self) -> str:
        """Return information useful in bug reports."""
        return bugreport(self)

    def _get_backend(self) -> BackendT:
        backend, url = backends.by_url(
            self.backend_cls or self.conf.result_backend,
            self.loader)
        return backend(app=self, url=url)

    def _finalize_pending_conf(self) -> MutableMapping:
        """Get config value by key and finalize loading the configuration.

        Note:
            This is used by PendingConfiguration:
                as soon as you access a key the configuration is read.
        """
        conf = self._conf = self._load_config()
        return conf

    def _load_config(self) -> MutableMapping:
        if isinstance(self.on_configure, Signal):
            self.on_configure.send(sender=self)
        else:
            # used to be a method pre 4.0
            self.on_configure()
        if self._config_source:
            self.loader.config_from_object(self._config_source)
        self.configured = True
        settings = detect_settings(
            self.prepare_config(self.loader.conf), self._preconf,
            ignore_keys=self._preconf_set_by_auto, prefix=self.namespace,
        )
        if self._conf is not None:
            # replace in place, as someone may have referenced app.conf,
            # done some changes, accessed a key, and then try to make more
            # changes to the reference and not the finalized value.
            self._conf.swap_with(settings)
        else:
            self._conf = settings

        # load lazy config dict initializers.
        pending_def = self._pending_defaults
        while pending_def:
            self._conf.add_defaults(maybe_evaluate(pending_def.popleft()()))

        # load lazy periodic tasks
        pending_beat = self._pending_periodic_tasks
        while pending_beat:
            self._add_periodic_task(*pending_beat.popleft())

        self.on_after_configure.send(sender=self, source=self._conf)
        return self._conf

    def _after_fork(self) -> None:
        self._pool = None
        try:
            self.__dict__['amqp']._producer_pool = None
        except (AttributeError, KeyError):
            pass
        self.on_after_fork.send(sender=self)

    def signature(self, *args, **kwargs) -> SignatureT:
        """Return a new :class:`~celery.Signature` bound to this app."""
        kwargs['app'] = self
        return self._canvas.signature(*args, **kwargs)

    def add_periodic_task(self, schedule: ScheduleT, sig: SignatureT,
                          args: Tuple = (), kwargs: Dict = (),
                          name: str = None, **opts) -> str:
        key, entry = self._sig_to_periodic_task_entry(
            schedule, sig, args, kwargs, name, **opts)
        if self.configured:
            self._add_periodic_task(key, entry)
        else:
            self._pending_periodic_tasks.append((key, entry))
        return key

    def _sig_to_periodic_task_entry(
            self, schedule: ScheduleT, sig: SignatureT,
            args: Tuple = (), kwargs: Dict = {},
            name: str = None, **opts) -> Tuple[str, Mapping]:
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

    def _add_periodic_task(self, key: str, entry: Mapping) -> None:
        self._conf.beat_schedule[key] = entry

    def create_task_cls(self) -> type:
        """Create a base task class bound to this app."""
        return self.subclass_with_self(
            self.task_cls, name='Task', attribute='_app',
            keep_reduce=True, abstract=True,
        )

    def subclass_with_self(self, Class: type,
                           name: str = None,
                           attribute: str = 'app',
                           reverse: str = None,
                           keep_reduce: bool = False,
                           **kw) -> type:
        """Subclass an app-compatible class.

        App-compatible means that the class has a class attribute that
        provides the default app it should use, for example:
        ``class Foo: app = None``.

        Arguments:
            Class (type): The app-compatible class to subclass.
            name (str): Custom name for the target class.
            attribute (str): Name of the attribute holding the app,
                Default is 'app'.
            reverse (str): Reverse path to this object used for pickling
                purposes. For example, to get ``app.AsyncResult``,
                use ``"AsyncResult"``.
            keep_reduce (bool): If enabled a custom ``__reduce__``
                implementation won't be provided.
        """
        Class = symbol_by_name(Class)
        reverse = reverse if reverse else Class.__name__

        def __reduce__(self):
            return _unpickle_appattr, (reverse, self.__reduce_args__())

        attrs = dict(
            {attribute: self},
            __module__=Class.__module__,
            __doc__=Class.__doc__,
            **kw)
        if not keep_reduce:
            attrs['__reduce__'] = __reduce__

        return type(name or Class.__name__, (Class,), attrs)

    def _rgetattr(self, path: str) -> Any:
        return attrgetter(path)(self)

    def __enter__(self) -> AppT:
        return self

    def __exit__(self, *exc_info) -> None:
        self.close()

    def __repr__(self) -> str:
        return '<{0} {1}>'.format(type(self).__name__, appstr(self))

    def __reduce__(self) -> Tuple:
        if self._using_v1_reduce:
            return self.__reduce_v1__()
        return (_unpickle_app_v2, (self.__class__, self.__reduce_keys__()))

    def __reduce_v1__(self) -> Tuple:
        # Reduce only pickles the configuration changes,
        # so the default configuration doesn't have to be passed
        # between processes.
        return (
            _unpickle_app,
            (self.__class__, self.Pickler) + self.__reduce_args__(),
        )

    def __reduce_keys__(self) -> Mapping[str, Any]:
        """Keyword arguments used to reconstruct the object when unpickling."""
        return {
            'main': self.main,
            'changes':
                self._conf.changes if self.configured else self._preconf,
            'loader': self.loader_cls,
            'backend': self.backend_cls,
            'amqp': self.amqp_cls,
            'events': self.events_cls,
            'log': self.log_cls,
            'control': self.control_cls,
            'fixups': self.fixups,
            'config_source': self._config_source,
            'task_cls': self.task_cls,
            'namespace': self.namespace,
        }

    def __reduce_args__(self) -> Tuple:
        """Deprecated method, please use :meth:`__reduce_keys__` instead."""
        return (self.main, self._conf.changes if self.configured else {},
                self.loader_cls, self.backend_cls, self.amqp_cls,
                self.events_cls, self.log_cls, self.control_cls,
                False, self._config_source)

    @cached_property
    def Worker(self) -> WorkerT:
        """Worker application.

        See Also:
            :class:`~@Worker`.
        """
        return self.subclass_with_self('celery.apps.worker:Worker')

    @cached_property
    def WorkController(self, **kwargs) -> WorkerT:
        """Embeddable worker.

        See Also:
            :class:`~@WorkController`.
        """
        return self.subclass_with_self('celery.worker:WorkController')

    @cached_property
    def Beat(self, **kwargs) -> BeatT:
        """:program:`celery beat` scheduler application.

        See Also:
            :class:`~@Beat`.
        """
        return self.subclass_with_self('celery.apps.beat:Beat')

    @cached_property
    def Task(self) -> type:
        """Base task class for this app."""
        return self.create_task_cls()

    @cached_property
    def annotations(self):
        return prepare_annotations(self.conf.task_annotations)

    @cached_property
    def AsyncResult(self) -> type:
        """Create new result instance.

        See Also:
            :class:`celery.result.AsyncResult`.
        """
        return self.subclass_with_self('celery.result:AsyncResult')

    @cached_property
    def ResultSet(self) -> type:
        return self.subclass_with_self('celery.result:ResultSet')

    @cached_property
    def GroupResult(self) -> type:
        """Create new group result instance.

        See Also:
            :class:`celery.result.GroupResult`.
        """
        return self.subclass_with_self('celery.result:GroupResult')

    @property
    def pool(self) -> ResourceT:
        """Broker connection pool: :class:`~@pool`.

        Note:
            This attribute is not related to the workers concurrency pool.
        """
        if self._pool is None:
            self._ensure_after_fork()
            limit = self.conf.broker_pool_limit
            pools.set_limit(limit)
            self._pool = pools.connections[self.connection_for_write()]
        return self._pool

    @property
    def current_task(self) -> Optional[TaskT]:
        """Instance of task being executed, or :const:`None`."""
        return _task_stack.top

    @property
    def current_worker_task(self) -> Optional[TaskT]:
        """The task currently being executed by a worker or :const:`None`.

        Differs from :data:`current_task` in that it's not affected
        by tasks calling other tasks directly, or eagerly.
        """
        return get_current_worker_task()

    @cached_property
    def oid(self) -> str:
        """Universally unique identifier for this app."""
        # since 4.0: thread.get_ident() is not included when
        # generating the process id.  This is due to how the RPC
        # backend now dedicates a single thread to receive results,
        # which would not work if each thread has a separate id.
        return oid_from(self, threads=False)

    @cached_property
    def amqp(self) -> AppAMQPT:
        """AMQP related functionality: :class:`~@amqp`."""
        return instantiate(self.amqp_cls, app=self)

    @cached_property
    def backend(self) -> BackendT:
        """Current backend instance."""
        return self._get_backend()

    @property
    def conf(self) -> MutableMapping:
        """Current configuration."""
        if self._conf is None:
            self._conf = self._load_config()
        return self._conf

    @conf.setter
    def conf(self, d: MutableMapping) -> None:  # noqa
        self._conf = d

    @cached_property
    def control(self) -> AppControlT:
        """Remote control: :class:`~@control`."""
        return instantiate(self.control_cls, app=self)

    @cached_property
    def events(self) -> AppEventsT:
        """Consuming and sending events: :class:`~@events`."""
        return instantiate(self.events_cls, app=self)

    @cached_property
    def loader(self) -> LoaderT:
        """Current loader instance."""
        return get_loader_cls(self.loader_cls)(app=self)

    @cached_property
    def log(self) -> AppLogT:
        """Logging: :class:`~@log`."""
        return instantiate(self.log_cls, app=self)

    @cached_property
    def _canvas(self) -> ModuleType:
        from celery import canvas
        return canvas

    @cached_property
    def tasks(self) -> TaskRegistryT:
        """Task registry.

        Warning:
            Accessing this attribute will also auto-finalize the app.
        """
        self.finalize(auto=True)
        return self._tasks

    @property
    def producer_pool(self):
        return self.amqp.producer_pool

    @cached_property
    def timezone(self):
        """Current timezone for this app.

        This is a cached property taking the time zone from the
        :setting:`timezone` setting.
        """
        conf = self.conf
        tz = conf.timezone
        if not tz:
            return (timezone.get_timezone('UTC') if conf.enable_utc
                    else timezone.local)
        return timezone.get_timezone(conf.timezone)
