# -*- coding: utf-8 -*-
"""
    celery.bootsteps
    ~~~~~~~~~~~~~~~~

    The bootsteps!

"""
from __future__ import absolute_import, unicode_literals

from collections import deque
from importlib import import_module
from threading import Event

from kombu.common import ignore_errors
from kombu.utils import symbol_by_name

from .datastructures import DependencyGraph, GraphFormatter
from .five import values, with_metaclass
from .utils.imports import instantiate, qualname
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
    return '| {0}: {1}'.format(ns.alias, fmt)


def _label(s):
    return s.name.rsplit('.', 1)[-1]


class StepFormatter(GraphFormatter):

    namespace_prefix = '⧉'
    conditional_prefix = '∘'
    namespace_scheme = {
        'shape': 'parallelogram',
        'color': 'slategray4',
        'fillcolor': 'slategray3',
    }

    def label(self, step):
        return step and '{0}{1}'.format(
            self._get_prefix(step),
            (step.label or _label(step)).encode('utf-8', 'ignore'),
        )

    def _get_prefix(self, step):
        if step.last:
            return self.namespace_prefix
        if step.conditional:
            return self.conditional_prefix
        return ''

    def node(self, obj, **attrs):
        scheme = self.namespace_scheme if obj.last else self.node_scheme
        return self.draw_node(obj, scheme, attrs)

    def edge(self, a, b, **attrs):
        if a.last:
            attrs.update(arrowhead='none', color='darkseagreen3')
        return self.draw_edge(a, b, self.edge_scheme, attrs)


class Namespace(object):
    """A namespace containing bootsteps.

    :keyword steps: List of steps.
    :keyword name: Set explicit name for this namespace.
    :keyword app: Set the Celery app for this namespace.
    :keyword on_start: Optional callback applied after namespace start.
    :keyword on_close: Optional callback applied before namespace close.
    :keyword on_stopped: Optional callback applied after namespace stopped.

    """
    GraphFormatter = StepFormatter

    name = None
    state = None
    started = 0
    default_steps = set()

    def __init__(self, steps=None, name=None, app=None,
                 on_start=None, on_close=None, on_stopped=None):
        self.app = app
        self.name = name or self.name or qualname(type(self))
        self.types = set(steps or []) | set(self.default_steps)
        self.on_start = on_start
        self.on_close = on_close
        self.on_stopped = on_stopped
        self.shutdown_complete = Event()
        self.steps = {}

    def start(self, parent):
        self.state = RUN
        if self.on_start:
            self.on_start()
        for i, step in enumerate(s for s in parent.steps if s is not None):
            self._debug('Starting %s', step.alias)
            self.started = i + 1
            step.start(parent)
            debug('^-- substep ok')

    def info(self, parent):
        info = {}
        for step in parent.steps:
            info.update(step.info(parent) or {})
        return info

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
                    self._debug('%s %s...', description, step.alias)
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
        self._debug('Preparing bootsteps.')
        order = self.order = []
        steps = self.steps = self.claim_steps()

        self._debug('Building graph...')
        for S in self._finalize_steps(steps):
            step = S(parent, **kwargs)
            steps[step.name] = step
            order.append(step)
        self._debug('New boot order: {%s}',
                    ', '.join(s.alias for s in self.order))
        for step in order:
            step.include(parent)
        return self

    def connect_with(self, other):
        self.graph.adjacent.update(other.graph.adjacent)
        self.graph.add_edge(type(other.order[0]), type(self.order[-1]))

    def import_module(self, module):
        return import_module(module)

    def __getitem__(self, name):
        return self.steps[name]

    def _find_last(self):
        for C in values(self.steps):
            if C.last:
                return C

    def _firstpass(self, steps):
        stream = deque(step.requires for step in values(steps))
        while stream:
            for node in stream.popleft():
                node = symbol_by_name(node)
                if node.name not in self.steps:
                    steps[node.name] = node
                stream.append(node.requires)

    def _finalize_steps(self, steps):
        last = self._find_last()
        self._firstpass(steps)
        it = ((C, C.requires) for C in values(steps))
        G = self.graph = DependencyGraph(
            it, formatter=self.GraphFormatter(root=last),
        )
        if last:
            for obj in G:
                if obj != last:
                    G.add_edge(last, obj)
        try:
            return G.topsort()
        except KeyError as exc:
            raise KeyError('unknown bootstep: %s' % exc)

    def claim_steps(self):
        return dict(self.load_step(step) for step in self._all_steps())

    def _all_steps(self):
        return self.types | self.app.steps[self.name.lower()]

    def load_step(self, step):
        step = symbol_by_name(step)
        return step.name, step

    def _debug(self, msg, *args):
        return debug(_pre(self, msg), *args)

    @property
    def alias(self):
        return _label(self)


class StepType(type):
    """Metaclass for steps."""

    def __new__(cls, name, bases, attrs):
        module = attrs.get('__module__')
        qname = '{0}.{1}'.format(module, name) if module else name
        attrs.update(
            __qualname__=qname,
            name=attrs.get('name') or qname,
            requires=attrs.get('requires', ()),
        )
        return super(StepType, cls).__new__(cls, name, bases, attrs)

    def __str__(self):
        return self.name

    def __repr__(self):
        return 'step:{0.name}{{{0.requires!r}}}'.format(self)


@with_metaclass(StepType)
class Step(object):
    """A Bootstep.

    The :meth:`__init__` method is called when the step
    is bound to a parent object, and can as such be used
    to initialize attributes in the parent object at
    parent instantiation-time.

    """

    #: Optional step name, will use qualname if not specified.
    name = None

    #: Optional short name used for graph outputs and in logs.
    label = None

    #: Set this to true if the step is enabled based on some condition.
    conditional = False

    #: List of other steps that that must be started before this step.
    #: Note that all dependencies must be in the same namespace.
    requires = ()

    #: This flag is reserved for the workers Consumer,
    #: since it is required to always be started last.
    #: There can only be one object marked with lsat
    #: in every namespace.
    last = False

    #: This provides the default for :meth:`include_if`.
    enabled = True

    def __init__(self, parent, **kwargs):
        pass

    def include_if(self, parent):
        """An optional predicate that decided whether this
        step should be created."""
        return self.enabled

    def instantiate(self, name, *args, **kwargs):
        return instantiate(name, *args, **kwargs)

    def _should_include(self, parent):
        if self.include_if(parent):
            return True, self.create(parent)
        return False, None

    def include(self, parent):
        return self._should_include(parent)[0]

    def create(self, parent):
        """Create the step."""
        pass

    def __repr__(self):
        return '<step: {0.alias}>'.format(self)

    @property
    def alias(self):
        return self.label or _label(self)

    def info(self, obj):
        pass


class StartStopStep(Step):

    #: Optional obj created by the :meth:`create` method.
    #: This is used by :class:`StartStopStep` to keep the
    #: original service object.
    obj = None

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
        inc, ret = self._should_include(parent)
        if inc:
            self.obj = ret
            parent.steps.append(self)
        return inc


class ConsumerStep(StartStopStep):
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
