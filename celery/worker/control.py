# -*- coding: utf-8 -*-
"""Worker remote control command implementations."""
import io
import tempfile
<<<<<<< HEAD
from collections import namedtuple

=======
from collections import UserDict
from typing import (
    Any, Callable, Iterable, Mapping, NamedTuple, Sequence, Tuple, Union,
)
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
from billiard.common import TERM_SIGNAME
from kombu.utils.encoding import safe_repr
from celery.exceptions import WorkerShutdown
from celery.platforms import signals as _signals
from celery.types import ControlStateT as StateT, RequestT, TimerT
from celery.utils.functional import maybe_list
from celery.utils.log import get_logger
from celery.utils.serialization import jsonify, strtobool
from celery.utils.time import rate
from . import state as worker_state
from .request import Request

__all__ = ('Panel',)

DEFAULT_TASK_INFO_ITEMS = ('exchange', 'routing_key', 'rate_limit')
logger = get_logger(__name__)

class controller_info_t(NamedTuple):
    """Metadata about control command."""

    alias: str
    type: str
    visible: bool
    default_timeout: float
    help: str
    signature: str
    args: Sequence[Tuple[str, type]]
    variadic: str


def ok(value: Any) -> Mapping:
    return {'ok': value}


def nok(value: Any) -> Mapping:
    return {'error': value}


class Panel(UserDict):
    """Global registry of remote control commands."""

    data = {}      # global dict.
    meta = {}      # -"-

    @classmethod
    def register(cls, *args, **kwargs) -> Callable:
        if args:
            return cls._register(**kwargs)(*args)
        return cls._register(**kwargs)

    @classmethod
    def _register(cls,
                  name: str = None,
                  alias: str = None,
                  type: str = 'control',
                  visible: bool = True,
                  default_timeout: float = 1.0,
                  help: str = None,
                  signature: str = None,
                  args: Sequence[Tuple[str, type]] = None,
                  variadic: str = None) -> Callable:

        def _inner(fun: Callable) -> Callable:
            control_name = name or fun.__name__
            _help = help or (fun.__doc__ or '').strip().split('\n')[0]
            cls.data[control_name] = fun
            cls.meta[control_name] = controller_info_t(
                alias, type, visible, default_timeout,
                _help, signature, args, variadic)
            if alias:
                cls.data[alias] = fun
            return fun
        return _inner


def control_command(**kwargs) -> Callable:
    return Panel.register(type='control', **kwargs)


def inspect_command(**kwargs) -> Callable:
    return Panel.register(type='inspect', **kwargs)

# -- App


@inspect_command()
def report(state: StateT) -> Mapping:
    """Information about Celery installation for bug reports."""
    return ok(state.app.bugreport())


@inspect_command(
    alias='dump_conf',  # XXX < backwards compatible
    signature='[include_defaults=False]',
    args=[('with_defaults', strtobool)],
)
def conf(state: StateT, with_defaults: bool = False, **kwargs) -> Mapping:
    """List configuration."""
    return jsonify(state.app.conf.table(with_defaults=with_defaults),
                   keyfilter=_wanted_config_key,
                   unknown_type_filter=safe_repr)


def _wanted_config_key(key: Any) -> bool:
    return isinstance(key, str) and not key.startswith('__')


# -- Task

@inspect_command(
    variadic='ids',
    signature='[id1 [id2 [... [idN]]]]',
)
def query_task(state: StateT,
               ids: Union[str, Sequence[str]],
               **kwargs) -> Mapping[str, Tuple[str, Mapping]]:
    """Query for task information by id."""
    return {
        req.id: (_state_of_task(req), req.info())
        for req in _find_requests_by_id(maybe_list(ids))
    }


def _find_requests_by_id(
        ids: Sequence[str],
        *,
        get_request=worker_state.requests.__getitem__) -> Iterable[RequestT]:
    for task_id in ids:
        try:
            yield get_request(task_id)
        except KeyError:
            pass


def _state_of_task(
        request: RequestT,
        *,
        is_active=worker_state.active_requests.__contains__,
        is_reserved=worker_state.reserved_requests.__contains__) -> str:
    if is_active(request):
        return 'active'
    elif is_reserved(request):
        return 'reserved'
    return 'ready'


@control_command(
    variadic='task_id',
    signature='[id1 [id2 [... [idN]]]]',
)
def revoke(state: StateT, task_id: str,
           terminate: bool = False,
           signal: Union[str, int] = None, **kwargs) -> Mapping:
    """Revoke task by task id (or list of ids).

    Keyword Arguments:
        terminate (bool): Also terminate the process if the task is active.
        signal (str): Name of signal to use for terminate (e.g., ``KILL``).
    """
    # pylint: disable=redefined-outer-name
    # XXX Note that this redefines `terminate`:
    #     Outside of this scope that is a function.
    # supports list argument since 3.1
    task_ids, task_id = set(maybe_list(task_id) or []), None
    size = len(task_ids)
    terminated = set()

    worker_state.revoked.update(task_ids)
    if terminate:
        signum = _signals.signum(signal or TERM_SIGNAME)
        for request in _find_requests_by_id(task_ids):
            if request.id not in terminated:
                terminated.add(request.id)
                logger.info('Terminating %s (%s)', request.id, signum)
                request.terminate(state.consumer.pool, signal=signum)
                if len(terminated) >= size:
                    break

        if not terminated:
            return ok('terminate: tasks unknown')
        return ok('terminate: {0}'.format(', '.join(terminated)))

    idstr = ', '.join(task_ids)
    logger.info('Tasks flagged as revoked: %s', idstr)
    return ok('tasks {0} flagged as revoked'.format(idstr))


@control_command(
    variadic='task_id',
    args=[('signal', str)],
    signature='<signal> [id1 [id2 [... [idN]]]]'
)
def terminate(state: StateT, signal: str, task_id: str, **kwargs) -> Mapping:
    """Terminate task by task id (or list of ids)."""
    return revoke(state, task_id, terminate=True, signal=signal)


@control_command(
    args=[('task_name', str), ('rate_limit', str)],
    signature='<task_name> <rate_limit (e.g., 5/s | 5/m | 5/h)>',
)
def rate_limit(state: StateT, task_name: str, rate_limit: Union[str, int],
               **kwargs) -> Mapping:
    """Tell worker(s) to modify the rate limit for a task by type.

    See Also:
        :attr:`celery.task.base.Task.rate_limit`.

    Arguments:
        task_name (str): Type of task to set rate limit for.
        rate_limit (int, str): New rate limit.
    """
    # pylint: disable=redefined-outer-name
    # XXX Note that this redefines `terminate`:
    #     Outside of this scope that is a function.
    try:
        rate(rate_limit)
    except ValueError as exc:
        return nok('Invalid rate limit string: {0!r}'.format(exc))

    try:
        state.app.tasks[task_name].rate_limit = rate_limit
    except KeyError:
        logger.error('Rate limit attempt for unknown task %s',
                     task_name, exc_info=True)
        return nok('unknown task')

    state.consumer.reset_rate_limits()

    if not rate_limit:
        logger.info('Rate limits disabled for tasks of type %s', task_name)
        return ok('rate limit disabled successfully')

    logger.info('New rate limit for tasks of type %s: %s.',
                task_name, rate_limit)
    return ok('new rate limit set successfully')


@control_command(
    args=[('task_name', str), ('soft', float), ('hard', float)],
    signature='<task_name> <soft_secs> [hard_secs]',
)
def time_limit(state: StateT,
               task_name: str = None,
               hard: float = None,
               soft: float = None, **kwargs) -> Mapping:
    """Tell worker(s) to modify the time limit for task by type.

    Arguments:
        task_name (str): Name of task to change.
        hard (float): Hard time limit.
        soft (float): Soft time limit.
    """
    try:
        task = state.app.tasks[task_name]
    except KeyError:
        logger.error('Change time limit attempt for unknown task %s',
                     task_name, exc_info=True)
        return nok('unknown task')

    task.soft_time_limit = soft
    task.time_limit = hard

    logger.info('New time limits for tasks of type %s: soft=%s hard=%s',
                task_name, soft, hard)
    return ok('time limits set successfully')


# -- Events


@inspect_command()
def clock(state: StateT, **kwargs) -> Mapping:
    """Get current logical clock value."""
    return {'clock': state.app.clock.value}


@control_command()
def election(state: StateT, id: str, topic: str, action: str = None,
             **kwargs) -> None:
    """Hold election.

    Arguments:
        id (str): Unique election id.
        topic (str): Election topic.
        action (str): Action to take for elected actor.
    """
    if state.consumer.gossip:
        state.consumer.gossip.election(id, topic, action)


@control_command()
def enable_events(state: StateT) -> Mapping:
    """Tell worker(s) to send task-related events."""
    dispatcher = state.consumer.event_dispatcher
    if dispatcher.groups and 'task' not in dispatcher.groups:
        dispatcher.groups.add('task')
        logger.info('Events of group {task} enabled by remote.')
        return ok('task events enabled')
    return ok('task events already enabled')


@control_command()
def disable_events(state: StateT) -> Mapping:
    """Tell worker(s) to stop sending task-related events."""
    dispatcher = state.consumer.event_dispatcher
    if 'task' in dispatcher.groups:
        dispatcher.groups.discard('task')
        logger.info('Events of group {task} disabled by remote.')
        return ok('task events disabled')
    return ok('task events already disabled')


@control_command()
def heartbeat(state: StateT) -> None:
    """Tell worker(s) to send event heartbeat immediately."""
    logger.debug('Heartbeat requested by remote.')
    dispatcher = state.consumer.event_dispatcher
    dispatcher.send('worker-heartbeat', freq=5, **worker_state.SOFTWARE_INFO)


# -- Worker

@inspect_command(visible=False)
def hello(state: StateT, from_node: str,
          revoked: Mapping = None, **kwargs) -> Mapping:
    """Request mingle sync-data."""
    # pylint: disable=redefined-outer-name
    # XXX Note that this redefines `revoked`:
    #     Outside of this scope that is a function.
    if from_node != state.hostname:
        logger.info('sync with %s', from_node)
        if revoked:
            worker_state.revoked.update(revoked)
        return {
            'revoked': worker_state.revoked._data,
            'clock': state.app.clock.forward(),
        }


@inspect_command(default_timeout=0.2)
def ping(state: StateT, **kwargs) -> Mapping:
    """Ping worker(s)."""
    return ok('pong')


@inspect_command()
def stats(state: StateT, **kwargs) -> Mapping:
    """Request worker statistics/information."""
    return state.consumer.controller.stats()


@inspect_command(alias='dump_schedule')
def scheduled(state: StateT, **kwargs) -> Sequence[Mapping]:
    """List of currently scheduled ETA/countdown tasks."""
    return list(_iter_schedule_requests(state.consumer.timer))


def _iter_schedule_requests(timer: TimerT) -> Iterable[Mapping]:
    for waiting in timer.schedule.queue:
        try:
            arg0 = waiting.entry.args[0]
        except (IndexError, TypeError):
            continue
        else:
            if isinstance(arg0, Request):
                yield {
                    'eta': arg0.eta.isoformat() if arg0.eta else None,
                    'priority': waiting.priority,
                    'request': arg0.info(),
                }


@inspect_command(alias='dump_reserved')
def reserved(state: StateT, **kwargs) -> Sequence[Mapping]:
    """List of currently reserved tasks, not including scheduled/active."""
    reserved_tasks = (
        state.tset(worker_state.reserved_requests) -
        state.tset(worker_state.active_requests)
    )
    if not reserved_tasks:
        return []
    return [request.info() for request in reserved_tasks]


@inspect_command(alias='dump_active')
def active(state: StateT, **kwargs) -> Sequence[Mapping]:
    """List of tasks currently being executed."""
    return [request.info()
            for request in state.tset(worker_state.active_requests)]


@inspect_command(alias='dump_revoked')
def revoked(state: StateT, **kwargs) -> Sequence[str]:
    """List of revoked task-ids."""
    return list(worker_state.revoked)


@inspect_command(
    alias='dump_tasks',
    variadic='taskinfoitems',
    signature='[attr1 [attr2 [... [attrN]]]]',
)
def registered(state: StateT,
               taskinfoitems: Sequence[str] = None,
               builtins: bool = False, **kwargs) -> Sequence[Mapping]:
    """List of registered tasks.

    Arguments:
        taskinfoitems (Sequence[str]): List of task attributes to include.
            Defaults to ``exchange,routing_key,rate_limit``.
        builtins (bool): Also include built-in tasks.
    """
    reg = state.app.tasks
    taskinfoitems = taskinfoitems or DEFAULT_TASK_INFO_ITEMS

    tasks = reg if builtins else (
        task for task in reg if not task.startswith('celery.'))

    def _extract_info(task):
        fields = {
            field: str(getattr(task, field, None)) for field in taskinfoitems
            if getattr(task, field, None) is not None
        }
        if fields:
            info = ['='.join(f) for f in fields.items()]
            return '{0} [{1}]'.format(task.name, ' '.join(info))
        return task.name

    return [_extract_info(reg[task]) for task in sorted(tasks)]


# -- Debugging

@inspect_command(
    default_timeout=60.0,
    args=[('type', str), ('num', int), ('max_depth', int)],
    signature='[object_type=Request] [num=200 [max_depth=10]]',
)
def objgraph(state: StateT,  # pragma: no cover
             num: int = 200,
             max_depth: int = 10,
             type: str = 'Request') -> Mapping:
    """Create graph of uncollected objects (memory-leak debugging).

    Arguments:
        num (int): Max number of objects to graph.
        max_depth (int): Traverse at most n levels deep.
        type (str): Name of object to graph.  Default is ``"Request"``.
    """
    try:
        import objgraph as _objgraph
    except ImportError:
        raise ImportError('Requires the objgraph library')
    logger.info('Dumping graph for type %r', type)
    with tempfile.NamedTemporaryFile(prefix='cobjg',
                                     suffix='.png', delete=False) as fh:
        objects = _objgraph.by_type(type)[:num]
        _objgraph.show_backrefs(
            objects,
            max_depth=max_depth, highlight=lambda v: v in objects,
            filename=fh.name,
        )
        return {'filename': fh.name}


@inspect_command()
def memsample(state: StateT, **kwargs) -> str:
    """Sample current RSS memory usage."""
    from celery.utils.debug import sample_mem
    return sample_mem()


@inspect_command(
    args=[('samples', int)],
    signature='[n_samples=10]',
)
def memdump(state: StateT,  # pragma: no cover
            samples: int = 10, **kwargs) -> str:
    """Dump statistics of previous memsample requests."""
    from celery.utils import debug
    out = io.StringIO()
    debug.memdump(file=out)
    return out.getvalue()

# -- Pool


@control_command(
    args=[('n', int)],
    signature='[N=1]',
)
def pool_grow(state: StateT, n: int = 1, **kwargs) -> Mapping:
    """Grow pool by n processes/threads."""
    if state.consumer.controller.autoscaler:
        state.consumer.controller.autoscaler.force_scale_up(n)
    else:
        state.consumer.pool.grow(n)
        state.consumer._update_prefetch_count(n)
    return ok('pool will grow')


@control_command(
    args=[('n', int)],
    signature='[N=1]',
)
def pool_shrink(state: StateT, n: int = 1, **kwargs) -> Mapping:
    """Shrink pool by n processes/threads."""
    if state.consumer.controller.autoscaler:
        state.consumer.controller.autoscaler.force_scale_down(n)
    else:
        state.consumer.pool.shrink(n)
        state.consumer._update_prefetch_count(-n)
    return ok('pool will shrink')


@control_command()
def pool_restart(state: StateT,
                 modules: Sequence[str] = None,
                 reload: bool = False,
                 reloader: Callable = None,
                 **kwargs) -> Mapping:
    """Restart execution pool."""
    if state.app.conf.worker_pool_restarts:
        state.consumer.controller.reload(modules, reload, reloader=reloader)
        return ok('reload started')
    else:
        raise ValueError('Pool restarts not enabled')


@control_command(
    args=[('max', int), ('min', int)],
    signature='[max [min]]',
)
def autoscale(state: StateT, max: int = None, min: int = None) -> Mapping:
    """Modify autoscale settings."""
    autoscaler = state.consumer.controller.autoscaler
    if autoscaler:
        max_, min_ = autoscaler.update(max, min)
        return ok('autoscale now max={0} min={1}'.format(max_, min_))
    raise ValueError('Autoscale not enabled')


@control_command()
def shutdown(state: StateT, msg: str = 'Got shutdown from remote',
             **kwargs) -> None:
    """Shutdown worker(s)."""
    logger.warning(msg)
    raise WorkerShutdown(msg)


# -- Queues

@control_command(
    args=[
        ('queue', str),
        ('exchange', str),
        ('exchange_type', str),
        ('routing_key', str),
    ],
    signature='<queue> [exchange [type [routing_key]]]',
)
def add_consumer(state: StateT, queue: str,
                 exchange: str = None,
                 exchange_type: str = None,
                 routing_key: str = None,
                 **options) -> Mapping:
    """Tell worker(s) to consume from task queue by name."""
    state.consumer.call_soon(
        state.consumer.add_task_queue,
        queue, exchange, exchange_type or 'direct', routing_key, **options)
    return ok('add consumer {0}'.format(queue))


@control_command(
    args=[('queue', str)],
    signature='<queue>',
)
def cancel_consumer(state: StateT, queue: str, **_) -> Mapping:
    """Tell worker(s) to stop consuming from task queue by name."""
    state.consumer.call_soon(
        state.consumer.cancel_task_queue, queue,
    )
    return ok('no longer consuming from {0}'.format(queue))


@inspect_command()
<<<<<<< HEAD
def active_queues(state):
    """List the task queues a worker is currently consuming from."""
=======
async def active_queues(state: StateT) -> Sequence[Mapping]:
    """List the task queues a worker are currently consuming from."""
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
    if state.consumer.task_consumer:
        return [dict(queue.as_dict(recurse=True))
                for queue in state.consumer.task_consumer.queues]
    return []
