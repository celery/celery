# -*- coding: utf-8 -*-
"""
    celery.worker.control
    ~~~~~~~~~~~~~~~~~~~~~

    Remote control commands.

"""
from __future__ import absolute_import

import io
import tempfile

from kombu.utils.encoding import safe_repr

from celery.exceptions import WorkerShutdown
from celery.five import items, string_t
from celery.platforms import signals as _signals
from celery.utils import timeutils
from celery.utils.functional import maybe_list
from celery.utils.log import get_logger
from celery.utils import jsonify
from celery.app.control import shared_controller

from . import state as worker_state
from .request import Request
from .state import revoked

__all__ = ['Panel']
DEFAULT_TASK_INFO_ITEMS = ('exchange', 'routing_key', 'rate_limit')
logger = get_logger(__name__)


class Panel(object):  # XXX compat

    @classmethod
    def register(cls, method, name=None):
        return shared_controller(method, name=name)


def _find_requests_by_id(ids, requests):
    found, total = 0, len(ids)
    for request in requests:
        if request.id in ids:
            yield request
            found += 1
            if found >= total:
                break


@shared_controller
def query_task(state, ids, **kwargs):
    ids = maybe_list(ids)

    reserved = 'reserved', worker_state.reserved_requests
    active = 'active', worker_state.active_requests

    return {req.id: (status, req.info()) for status, req in (reserved, active)}


@shared_controller
def revoke(state, task_id, terminate=False, signal=None, **kwargs):
    """Revoke task by task id."""
    # supports list argument since 3.1
    task_ids, task_id = set(maybe_list(task_id) or []), None
    size = len(task_ids)
    terminated = set()

    revoked.update(task_ids)
    if terminate:
        signum = _signals.signum(signal or 'TERM')
        # reserved_requests changes size during iteration
        # so need to consume the items first, then terminate after.
        requests = set(_find_requests_by_id(
            task_ids,
            worker_state.reserved_requests,
        ))
        for request in requests:
            if request.id not in terminated:
                terminated.add(request.id)
                logger.info('Terminating %s (%s)', request.id, signum)
                request.terminate(state.consumer.pool, signal=signum)
                if len(terminated) >= size:
                    break

        if not terminated:
            return {'ok': 'terminate: tasks unknown'}
        return {'ok': 'terminate: {0}'.format(', '.join(terminated))}

    idstr = ', '.join(task_ids)
    logger.info('Tasks flagged as revoked: %s', idstr)
    return {'ok': 'tasks {0} flagged as revoked'.format(idstr)}


@shared_controller
def report(state):
    return {'ok': state.app.bugreport()}


@shared_controller
def enable_events(state):
    dispatcher = state.consumer.event_dispatcher
    if 'task' not in dispatcher.groups:
        dispatcher.groups.add('task')
        logger.info('Events of group {task} enabled by remote.')
        return {'ok': 'task events enabled'}
    return {'ok': 'task events already enabled'}


@shared_controller
def disable_events(state):
    dispatcher = state.consumer.event_dispatcher
    if 'task' in dispatcher.groups:
        dispatcher.groups.discard('task')
        logger.info('Events of group {task} disabled by remote.')
        return {'ok': 'task events disabled'}
    return {'ok': 'task events already disabled'}


@shared_controller
def heartbeat(state):
    logger.debug('Heartbeat requested by remote.')
    dispatcher = state.consumer.event_dispatcher
    dispatcher.send('worker-heartbeat', freq=5, **worker_state.SOFTWARE_INFO)


@shared_controller
def rate_limit(state, task_name, rate_limit, **kwargs):
    """Set new rate limit for a task type.

    See :attr:`celery.task.base.Task.rate_limit`.

    :param task_name: Type of task.
    :param rate_limit: New rate limit.

    """

    try:
        timeutils.rate(rate_limit)
    except ValueError as exc:
        return {'error': 'Invalid rate limit string: {0!r}'.format(exc)}

    try:
        state.app.tasks[task_name].rate_limit = rate_limit
    except KeyError:
        logger.error('Rate limit attempt for unknown task %s',
                     task_name, exc_info=True)
        return {'error': 'unknown task'}

    state.consumer.reset_rate_limits()

    if not rate_limit:
        logger.info('Rate limits disabled for tasks of type %s', task_name)
        return {'ok': 'rate limit disabled successfully'}

    logger.info('New rate limit for tasks of type %s: %s.',
                task_name, rate_limit)
    return {'ok': 'new rate limit set successfully'}


@shared_controller
def time_limit(state, task_name=None, hard=None, soft=None, **kwargs):
    try:
        task = state.app.tasks[task_name]
    except KeyError:
        logger.error('Change time limit attempt for unknown task %s',
                     task_name, exc_info=True)
        return {'error': 'unknown task'}

    task.soft_time_limit = soft
    task.time_limit = hard

    logger.info('New time limits for tasks of type %s: soft=%s hard=%s',
                task_name, soft, hard)
    return {'ok': 'time limits set successfully'}


@shared_controller
def dump_schedule(state, safe=False, **kwargs):

    def prepare_entries():
        for waiting in state.consumer.timer.schedule.queue:
            try:
                arg0 = waiting.entry.args[0]
            except (IndexError, TypeError):
                continue
            else:
                if isinstance(arg0, Request):
                    yield {'eta': arg0.eta.isoformat() if arg0.eta else None,
                           'priority': waiting.priority,
                           'request': arg0.info(safe=safe)}
    return list(prepare_entries())


@shared_controller
def dump_reserved(state, safe=False, **kwargs):
    reserved = worker_state.reserved_requests - worker_state.active_requests
    if not reserved:
        return []
    return [request.info(safe=safe) for request in reserved]


@shared_controller
def dump_active(state, safe=False, **kwargs):
    return [request.info(safe=safe)
            for request in worker_state.active_requests]


@shared_controller
def stats(state, **kwargs):
    return state.consumer.controller.stats()


@shared_controller
def objgraph(state, num=200, max_depth=10, type='Request'):  # pragma: no cover
    try:
        import objgraph
    except ImportError:
        raise ImportError('Requires the objgraph library')
    logger.info('Dumping graph for type %r', type)
    with tempfile.NamedTemporaryFile(prefix='cobjg',
                                     suffix='.png', delete=False) as fh:
        objects = objgraph.by_type(type)[:num]
        objgraph.show_backrefs(
            objects,
            max_depth=max_depth, highlight=lambda v: v in objects,
            filename=fh.name,
        )
        return {'filename': fh.name}


@shared_controller
def memsample(state, **kwargs):  # pragma: no cover
    from celery.utils.debug import sample_mem
    return sample_mem()


@shared_controller
def memdump(state, samples=10, **kwargs):  # pragma: no cover
    from celery.utils.debug import memdump
    out = io.StringIO()
    memdump(file=out)
    return out.getvalue()


@shared_controller
def clock(state, **kwargs):
    return {'clock': state.app.clock.value}


@shared_controller
def dump_revoked(state, **kwargs):
    return list(worker_state.revoked)


@shared_controller
def hello(state, from_node, revoked=None, **kwargs):
    if from_node != state.hostname:
        logger.info('sync with %s', from_node)
        if revoked:
            worker_state.revoked.update(revoked)
        return {'revoked': worker_state.revoked._data,
                'clock': state.app.clock.forward()}


@shared_controller
def dump_tasks(state, taskinfoitems=None, builtins=False, **kwargs):
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
            info = ['='.join(f) for f in items(fields)]
            return '{0} [{1}]'.format(task.name, ' '.join(info))
        return task.name

    return [_extract_info(reg[task]) for task in sorted(tasks)]


@shared_controller
def ping(state, **kwargs):
    return {'ok': 'pong'}


@shared_controller
def pool_grow(state, n=1, **kwargs):
    if state.consumer.controller.autoscaler:
        state.consumer.controller.autoscaler.force_scale_up(n)
    else:
        state.consumer.pool.grow(n)
        state.consumer._update_prefetch_count(n)
    return {'ok': 'pool will grow'}


@shared_controller
def pool_shrink(state, n=1, **kwargs):
    if state.consumer.controller.autoscaler:
        state.consumer.controller.autoscaler.force_scale_down(n)
    else:
        state.consumer.pool.shrink(n)
        state.consumer._update_prefetch_count(-n)
    return {'ok': 'pool will shrink'}


@shared_controller
def pool_restart(state, modules=None, reload=False, reloader=None, **kwargs):
    if state.app.conf.CELERYD_POOL_RESTARTS:
        state.consumer.controller.reload(modules, reload, reloader=reloader)
        return {'ok': 'reload started'}
    else:
        raise ValueError('Pool restarts not enabled')


@shared_controller
def autoscale(state, max=None, min=None):
    autoscaler = state.consumer.controller.autoscaler
    if autoscaler:
        max_, min_ = autoscaler.update(max, min)
        return {'ok': 'autoscale now min={0} max={1}'.format(max_, min_)}
    raise ValueError('Autoscale not enabled')


@shared_controller
def shutdown(state, msg='Got shutdown from remote', **kwargs):
    logger.warning(msg)
    raise WorkerShutdown(msg)


@shared_controller
def add_consumer(state, queue, exchange=None, exchange_type=None,
                 routing_key=None, **options):
    state.consumer.add_task_queue(queue, exchange, exchange_type,
                                  routing_key, **options)
    return {'ok': 'add consumer {0}'.format(queue)}


@shared_controller
def cancel_consumer(state, queue=None, **_):
    state.consumer.cancel_task_queue(queue)
    return {'ok': 'no longer consuming from {0}'.format(queue)}


@shared_controller
def active_queues(state):
    """Return information about the queues a worker consumes from."""
    if state.consumer.task_consumer:
        return [dict(queue.as_dict(recurse=True))
                for queue in state.consumer.task_consumer.queues]
    return []


def _wanted_config_key(key):
    return (isinstance(key, string_t) and
            key.isupper() and
            not key.startswith('__'))


@shared_controller
def dump_conf(state, with_defaults=False, **kwargs):
    return jsonify(state.app.conf.table(with_defaults=with_defaults),
                   keyfilter=_wanted_config_key,
                   unknown_type_filter=safe_repr)


@shared_controller
def election(state, id, topic, action=None, **kwargs):
    if state.consumer.gossip:
        state.consumer.gossip.election(id, topic, action)
