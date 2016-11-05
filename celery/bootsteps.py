# -*- coding: utf-8 -*-
"""A directed acyclic graph of reusable components."""
from collections import deque
from threading import Event
from typing import Any, Callable, Mapping, Optional, Sequence, Tuple

from kombu.common import ignore_errors
from kombu.utils.imports import symbol_by_name

from .utils.graph import DependencyGraph, GraphFormatter
from .utils.imports import instantiate, qualname
from .utils.log import get_logger
from .utils.typing import Timeout

try:
    from greenlet import GreenletExit
except ImportError:  # pragma: no cover
    IGNORE_ERRORS = ()
else:
    IGNORE_ERRORS = (GreenletExit,)

__all__ = ['Blueprint', 'Step', 'StartStopStep', 'ConsumerStep']

#: States
RUN = 0x1
CLOSE = 0x2
TERMINATE = 0x3

logger = get_logger(__name__)


class StepType(type):
    """Meta-class for steps."""

    def __new__(cls, name, bases, attrs):
        module = attrs.get('__module__')
        qname = '{0}.{1}'.format(module, name) if module else name
        attrs.update(
            __qualname__=qname,
            name=attrs.get('name') or qname,
        )
        return super().__new__(cls, name, bases, attrs)

    def __str__(self):
        return self.name

    def __repr__(self):
        return 'step:{0.name}{{{0.requires!r}}}'.format(self)


class Step(metaclass=StepType):
    """A Bootstep.

    The :meth:`__init__` method is called when the step
    is bound to a parent object, and can as such be used
    to initialize attributes in the parent object at
    parent instantiation-time.
    """

    #: Optional step name, will use ``qualname`` if not specified.
    name = None

    #: Optional short name used for graph outputs and in logs.
    label = None

    #: Set this to true if the step is enabled based on some condition.
    conditional = False

    #: List of other steps that that must be started before this step.
    #: Note that all dependencies must be in the same blueprint.
    requires = ()

    #: This flag is reserved for the workers Consumer,
    #: since it is required to always be started last.
    #: There can only be one object marked last
    #: in every blueprint.
    last = False

    #: This provides the default for :meth:`include_if`.
    enabled = True

    def __init__(self, parent, **kwargs):
        pass

    def include_if(self, parent):
        """Return True if bootstep should be included."""
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
    """Bootstep that is started and stopped."""

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
        if self.obj:
            return getattr(self.obj, 'terminate', self.obj.stop)()

    def include(self, parent):
        inc, ret = self._should_include(parent)
        if inc:
            self.obj = ret
            parent.steps.append(self)
        return inc


class ConsumerStep(StartStopStep):
    """Bootstep that starts message consumer."""

    requires = ('celery.worker.consumer:Connection',)
    consumers = None

    def get_consumers(self, channel):
        raise NotImplementedError('missing get_consumers')

    def start(self, c):
        channel = c.connection.channel()
        self.consumers = self.get_consumers(channel)
        for consumer in self.consumers or []:
            consumer.consume()

    def stop(self, c):
        self._close(c, True)

    def shutdown(self, c):
        self._close(c, False)

    def _close(self, c, cancel_consumers=True):
        channels = set()
        for consumer in self.consumers or []:
            if cancel_consumers:
                ignore_errors(c.connection, consumer.cancel)
            if consumer.channel:
                channels.add(consumer.channel)
        for channel in channels:
            ignore_errors(c.connection, channel.close)


def _pre(ns: Step, fmt: str) -> str:
    return '| {0}: {1}'.format(ns.alias, fmt)


def _label(s: Step) -> str:
    return s.name.rsplit('.', 1)[-1]


class StepFormatter(GraphFormatter):
    """Graph formatter for :class:`Blueprint`."""

    blueprint_prefix = '⧉'
    conditional_prefix = '∘'
    blueprint_scheme = {
        'shape': 'parallelogram',
        'color': 'slategray4',
        'fillcolor': 'slategray3',
    }

    def label(self, step: Step) -> str:
        return step and '{0}{1}'.format(
            self._get_prefix(step),
            (step.label or _label(step)).encode('utf-8', 'ignore').decode(),
        )

    def _get_prefix(self, step: Step) -> str:
        if step.last:
            return self.blueprint_prefix
        if step.conditional:
            return self.conditional_prefix
        return ''

    def node(self, obj: Any, **attrs) -> str:
        scheme = self.blueprint_scheme if obj.last else self.node_scheme
        return self.draw_node(obj, scheme, attrs)

    def edge(self, a: Any, b: Any, **attrs) -> str:
        if a.last:
            attrs.update(arrowhead='none', color='darkseagreen3')
        return self.draw_edge(a, b, self.edge_scheme, attrs)


class Blueprint:
    """Blueprint containing bootsteps that can be applied to objects.

    Arguments:
        steps Sequence[Union[str, Step]]: List of steps.
        name (str): Set explicit name for this blueprint.
        on_start (Callable): Optional callback applied after blueprint start.
        on_close (Callable): Optional callback applied before blueprint close.
        on_stopped (Callable): Optional callback applied after
            blueprint stopped.
    """

    GraphFormatter = StepFormatter

    name = None                        # type: Optional[str]
    state = None                       # type: Optional[int]
    started = 0                        # type: int
    default_steps = set()              # type: Set[Union[str, Step]]
    state_to_name = {                  # type: Mapping[int, str]
        0: 'initializing',
        RUN: 'running',
        CLOSE: 'closing',
        TERMINATE: 'terminating',
    }

    def __init__(self, steps: Optional[Sequence]=None,
                 name: Optional[str]=None,
                 on_start: Optional[Callable[[], None]]=None,
                 on_close: Optional[Callable[[], None]]=None,
                 on_stopped: Optional[Callable[[], None]]=None) -> None:
        self.name = name or self.name or qualname(type(self))
        self.types = set(steps or []) | set(self.default_steps)
        self.on_start = on_start
        self.on_close = on_close
        self.on_stopped = on_stopped
        self.shutdown_complete = Event()
        self.steps = {}  # type: Mapping[str, Step]

    def start(self, parent: Any) -> None:
        self.state = RUN
        if self.on_start:
            self.on_start()
        for i, step in enumerate(s for s in parent.steps if s is not None):
            self._debug('Starting %s', step.alias)
            self.started = i + 1
            step.start(parent)
            logger.debug('^-- substep ok')

    def human_state(self) -> str:
        return self.state_to_name[self.state or 0]

    def info(self, parent: Any) -> Mapping[str, Any]:
        info = {}
        for step in parent.steps:
            info.update(step.info(parent) or {})
        return info

    def close(self, parent: Any) -> None:
        if self.on_close:
            self.on_close()
        self.send_all(parent, 'close', 'closing', reverse=False)

    def restart(self, parent: Any, method: str='stop',
                description: str='restarting', propagate: bool=False) -> None:
        self.send_all(parent, method, description, propagate=propagate)

    def send_all(self, parent: Any, method: str,
                 description: Optional[str]=None,
                 reverse: bool=True, propagate: bool=True,
                 args: Sequence=()) -> None:
        description = description or method.replace('_', ' ')
        steps = reversed(parent.steps) if reverse else parent.steps
        for step in steps:
            if step:
                fun = getattr(step, method, None)
                if fun is not None:
                    self._debug('%s %s...',
                                description.capitalize(), step.alias)
                    try:
                        fun(parent, *args)
                    except Exception as exc:  # pylint: disable=broad-except
                        if propagate:
                            raise
                        logger.exception(
                            'Error on %s %s: %r', description, step.alias, exc)

    def stop(self, parent: Any,
             close: bool=True, terminate: bool=False) -> None:
        what = 'terminating' if terminate else 'stopping'
        if self.state in (CLOSE, TERMINATE):
            return

        if self.state != RUN or self.started != len(parent.steps):
            # Not fully started, can safely exit.
            self.state = TERMINATE
            self.shutdown_complete.set()
            return
        self.close(parent)
        self.state = CLOSE

        self.restart(
            parent, 'terminate' if terminate else 'stop',
            description=what, propagate=False,
        )

        if self.on_stopped:
            self.on_stopped()
        self.state = TERMINATE
        self.shutdown_complete.set()

    def join(self, timeout: Timeout=None) -> None:
        try:
            # Will only get here if running green,
            # makes sure all greenthreads have exited.
            self.shutdown_complete.wait(timeout=timeout)
        except IGNORE_ERRORS:
            pass

    def apply(self, parent: Any, **kwargs) -> 'Blueprint':
        """Apply the steps in this blueprint to an object.

        This will apply the ``__init__`` and ``include`` methods
        of each step, with the object as argument::

            step = Step(obj)
            ...
            step.include(obj)

        For :class:`StartStopStep` the services created
        will also be added to the objects ``steps`` attribute.
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

    def connect_with(self, other: 'Blueprint') -> None:
        self.graph.adjacent.update(other.graph.adjacent)
        self.graph.add_edge(type(other.order[0]), type(self.order[-1]))

    def __getitem__(self, name: str) -> Step:
        return self.steps[name]

    def _find_last(self) -> Optional[Step]:
        return next((C for C in self.steps.values() if C.last), None)

    def _firstpass(self, steps: Mapping[str, Step]) -> None:
        for step in steps.values():
            step.requires = [symbol_by_name(dep) for dep in step.requires]
        stream = deque(step.requires for step in steps.values())
        while stream:
            for node in stream.popleft():
                node = symbol_by_name(node)
                if node.name not in self.steps:
                    steps[node.name] = node
                stream.append(node.requires)

    def _finalize_steps(self, steps: Mapping[str, Step]) -> Sequence[Step]:
        last = self._find_last()
        self._firstpass(steps)
        it = ((C, C.requires) for C in steps.values())
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

    def claim_steps(self) -> Mapping[str, Step]:
        return dict(self.load_step(step) for step in self.types)

    def load_step(self, step: Step) -> Tuple[str, Step]:
        step = symbol_by_name(step)
        return step.name, step

    def _debug(self, msg: str, *args) -> None:
        logger.debug(_pre(self, msg), *args)

    @property
    def alias(self) -> str:
        return _label(self)
