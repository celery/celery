# -*- coding: utf-8 -*-
"""
    celery.bootsteps
    ~~~~~~~~~~~~~~~~

    The boot-steps!

"""
from __future__ import absolute_import

from collections import defaultdict
from importlib import import_module
from threading import Event

from kombu.common import ignore_errors
from kombu.utils import symbol_by_name

from .datastructures import DependencyGraph
from .utils.imports import instantiate
from .utils.log import get_logger
from .utils.threads import default_socket_timeout

try:
    from greenlet import GreenletExit
    IGNORE_ERRORS = (GreenletExit, )
except ImportError:  # pragma: no cover
    IGNORE_ERRORS = ()

#: Default socket timeout at shutdown.
SHUTDOWN_SOCKET_TIMEOUT = 5.0

#: States
RUN = 0x1
CLOSE = 0x2
TERMINATE = 0x3

logger = get_logger(__name__)
debug = logger.debug


def _pre(ns, fmt):
    return '| {0}: {1}'.format(ns.name, fmt)


class Namespace(object):
    """A namespace containing bootsteps.

    Every step must belong to a namespace.

    When step classes are created they are added to the
    mapping of unclaimed steps.  The steps will be
    claimed when the namespace they belong to is created.

    :keyword name: Set the name of this namespace.
    :keyword app: Set the Celery app for this namespace.

    """
    name = None
    state = None
    started = 0
    default_steps = set()

    def __init__(self, name=None, app=None, on_start=None,
            on_close=None, on_stopped=None):
        self.app = app
        self.name = name or self.name
        self.on_start = on_start
        self.on_close = on_close
        self.on_stopped = on_stopped
        self.services = []
        self.shutdown_complete = Event()

    def start(self, parent):
        self.state = RUN
        if self.on_start:
            self.on_start()
        for i, step in enumerate(filter(None, parent.steps)):
            self._debug('Starting %s', step.name)
            self.started = i + 1
            step.start(parent)
            debug('^-- substep ok')

    def close(self, parent):
        if self.on_close:
            self.on_close()
        for step in parent.steps:
            close = getattr(step, 'close', None)
            if close:
                close(parent)

    def restart(self, parent, description='Restarting', attr='stop'):
        with default_socket_timeout(SHUTDOWN_SOCKET_TIMEOUT):  # Issue 975
            for step in reversed(parent.steps):
                if step:
                    self._debug('%s %s...', description, step.name)
                    fun = getattr(step, attr, None)
                    if fun:
                        fun(parent)

    def stop(self, parent, close=True, terminate=False):
        what = 'Terminating' if terminate else 'Stopping'
        if self.state in (CLOSE, TERMINATE):
            return

        self.close(parent)

        if self.state != RUN or self.started != len(parent.steps):
            # Not fully started, can safely exit.
            self.state = TERMINATE
            self.shutdown_complete.set()
            return
        self.state = CLOSE
        self.restart(parent, what, 'terminate' if terminate else 'stop')

        if self.on_stopped:
            self.on_stopped()
        self.state = TERMINATE
        self.shutdown_complete.set()

    def join(self, timeout=None):
        try:
            # Will only get here if running green,
            # makes sure all greenthreads have exited.
            self.shutdown_complete.wait(timeout=timeout)
        except IGNORE_ERRORS:
            pass

    def apply(self, parent, **kwargs):
        """Apply the steps in this namespace to an object.

        This will apply the ``__init__`` and ``include`` methods
        of each steps with the object as argument.

        For :class:`StartStopStep` the services created
        will also be added the the objects ``steps`` attribute.

        """
        self._debug('Loading boot-steps.')
        self.steps = self.claim_steps()
        self._debug('Building graph.')
        self.boot_steps = [self.bind_step(name, parent, **kwargs)
                                for name in self._finalize_boot_steps()]
        self._debug('New boot order: {%s}',
                ', '.join(c.name for c in self.boot_steps))

        for step in self.boot_steps:
            step.include(parent)
        return self

    def bind_step(self, name, parent, **kwargs):
        """Bind step to parent object and this namespace."""
        comp = self[name](parent, **kwargs)
        comp.namespace = self
        return comp

    def import_module(self, module):
        return import_module(module)

    def __getitem__(self, name):
        return self.steps[name]

    def _find_last(self):
        for C in self.steps.itervalues():
            if C.last:
                return C

    def _finalize_boot_steps(self):
        G = self.graph = DependencyGraph((C.name, C.requires)
                            for C in self.steps.itervalues())
        last = self._find_last()
        if last:
            for obj in G:
                if obj != last.name:
                    G.add_edge(last.name, obj)
        try:
            return G.topsort()
        except KeyError as exc:
            raise KeyError('unknown boot-step: %s' % exc)

    def claim_steps(self):
        return dict(self.load_step(step) for step in self._unclaimed_steps())

    def _unclaimed_steps(self):
        return set(self.default_steps) | self.app.steps[self.name]

    def load_step(self, step):
        step = symbol_by_name(step)
        return step.name, step

    def _debug(self, msg, *args):
        return debug(_pre(self, msg), *args)


def _prepare_requires(req):
    if not isinstance(req, basestring):
        req = req.name
    return req


class StepType(type):
    """Metaclass for steps."""

    def __new__(cls, name, bases, attrs):
        module = attrs.get('__module__')
        qname = '.'.join([module, name]) if module else name
        attrs['name'] = attrs.get('name') or qname
        attrs['requires'] = tuple(_prepare_requires(req)
                                    for req in attrs.get('requires', ()))
        return super(StepType, cls).__new__(cls, name, bases, attrs)


class Step(object):
    """A Bootstep.

    The :meth:`__init__` method is called when the step
    is bound to a parent object, and can as such be used
    to initialize attributes in the parent object at
    parent instantiation-time.

    """
    __metaclass__ = StepType

    #: The name of the step, or the namespace
    #: and the name of the step separated by dot.
    name = None

    #: List of other steps that that must be started before this step.
    #: Note that all dependencies must be in the same namespace.
    requires = ()

    #: can be used to specify the namespace,
    #: if the name does not include it.
    namespace = None

    #: if set the step will not be registered,
    #: but can be used as a base class.
    abstract = True

    #: Optional obj created by the :meth:`create` method.
    #: This is used by :class:`StartStopStep` to keep the
    #: original service object.
    obj = None

    #: This flag is reserved for the workers Consumer,
    #: since it is required to always be started last.
    #: There can only be one object marked with lsat
    #: in every namespace.
    last = False

    #: This provides the default for :meth:`include_if`.
    enabled = True

    def __init__(self, parent, **kwargs):
        pass

    def create(self, parent):
        """Create the step."""
        pass

    def include_if(self, parent):
        """An optional predicate that decided whether this
        step should be created."""
        return self.enabled

    def instantiate(self, name, *args, **kwargs):
        return instantiate(name, *args, **kwargs)

    def include(self, parent):
        if self.include_if(parent):
            self.obj = self.create(parent)
            return True


class StartStopStep(Step):
    abstract = True

    def start(self, parent):
        if self.obj:
            return self.obj.start()

    def stop(self, parent):
        if self.obj:
            return self.obj.stop()

    def close(self, parent):
        pass

    def terminate(self, parent):
        self.stop(parent)

    def include(self, parent):
        if super(StartStopStep, self).include(parent):
            parent.steps.append(self)


class ConsumerStep(StartStopStep):
    abstract = True
    requires = ('Connection', )
    consumers = None

    def get_consumers(self, channel):
        raise NotImplementedError('missing get_consumers')

    def start(self, c):
        self.consumers = self.get_consumers(c.connection)
        for consumer in self.consumers or []:
            consumer.consume()

    def stop(self, c):
        for consumer in self.consumers or []:
            ignore_errors(c.connection, consumer.cancel)

    def shutdown(self, c):
        self.stop(c)
        for consumer in self.consumers or []:
            if consumer.channel:
                ignore_errors(c.connection, consumer.channel.close)
