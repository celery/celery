# -*- coding: utf-8 -*-
"""
    celery.worker.control
    ~~~~~~~~~~~~~~~~~~~~~

    Remote control commands.

"""
from __future__ import absolute_import

import tempfile

from celery.five import UserDict, items, StringIO
from celery.platforms import signals as _signals
from celery.utils import timeutils
from celery.utils.log import get_logger
from celery.utils import jsonify

from . import state as worker_state
from .state import revoked

DEFAULT_TASK_INFO_ITEMS = ('exchange', 'routing_key', 'rate_limit')
logger = get_logger(__name__)


class Panel(UserDict):
    data = dict()  # Global registry.

    @classmethod
    def register(cls, method, name=None):
        cls.data[name or method.__name__] = method
        return method


@Panel.register
def revoke(state, task_id, terminate=False, signal=None, **kwargs):
    """Revoke task by task id."""
    revoked.add(task_id)
    if terminate:
        signum = _signals.signum(signal or 'TERM')
        for request in worker_state.reserved_requests:
            if request.id == task_id:
                logger.info('Terminating %s (%s)', task_id, signum)
                request.terminate(state.consumer.pool, signal=signum)
                break
        else:
            return {'ok': 'terminate: task {0} not found'.format(task_id)}
        return {'ok': 'terminating {0} ({1})'.format(task_id, signal)}

    logger.info('Revoking task %s', task_id)
    return {'ok': 'revoking task {0}'.format(task_id)}


@Panel.register
def report(state):
    return {'ok': state.app.bugreport()}


@Panel.register
def enable_events(state):
    dispatcher = state.consumer.event_dispatcher
    if 'task' not in dispatcher.groups:
        dispatcher.groups.add('task')
        logger.info('Events of group {task} enabled by remote.')
        return {'ok': 'task events enabled'}
    return {'ok': 'task events already enabled'}


@Panel.register
def disable_events(state):
    dispatcher = state.consumer.event_dispatcher
    if 'task' in dispatcher.groups:
        dispatcher.groups.discard('task')
        logger.info('Events of group {task} disabled by remote.')
        return {'ok': 'task events disabled'}
    return {'ok': 'task events already disabled'}


@Panel.register
def heartbeat(state):
    logger.debug('Heartbeat requested by remote.')
    dispatcher = state.consumer.event_dispatcher
    dispatcher.send('worker-heartbeat', freq=5, **worker_state.SOFTWARE_INFO)


@Panel.register
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


@Panel.register
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


@Panel.register
def dump_schedule(state, safe=False, **kwargs):
    from celery.worker.job import Request
    schedule = state.consumer.timer.schedule
    if not schedule.queue:
        return []

    def prepare_entries():
        for entry in schedule.info():
            item = entry['item']
            if item.args and isinstance(item.args[0], Request):
                yield {'eta': entry['eta'],
                       'priority': entry['priority'],
                       'request': item.args[0].info(safe=safe)}
    return list(prepare_entries())


@Panel.register
def dump_reserved(state, safe=False, **kwargs):
    reserved = worker_state.reserved_requests - worker_state.active_requests
    if not reserved:
        return []
    return [request.info(safe=safe) for request in reserved]


@Panel.register
def dump_active(state, safe=False, **kwargs):
    return [request.info(safe=safe)
            for request in worker_state.active_requests]


@Panel.register
def stats(state, **kwargs):
    return state.consumer.controller.stats()


@Panel.register
def objgraph(state, num=200, max_depth=10, type='Request'):  # pragma: no cover
    try:
        import objgraph
    except ImportError:
        raise ImportError('Requires the objgraph library')
    print('Dumping graph for type %r' % (type, ))
    with tempfile.NamedTemporaryFile(prefix='cobjg',
                                     suffix='.png', delete=False) as fh:
        objects = objgraph.by_type(type)[:num]
        objgraph.show_backrefs(
            objects,
            max_depth=max_depth, highlight=lambda v: v in objects,
            filename=fh.name,
        )
        return {'filename': fh.name}


@Panel.register
def memsample(state, **kwargs):  # pragma: no cover
    from celery.utils.debug import sample_mem
    return sample_mem()


@Panel.register
def memdump(state, samples=10, **kwargs):  # pragma: no cover
    from celery.utils.debug import memdump
    out = StringIO()
    memdump(file=out)
    return out.getvalue()


@Panel.register
def clock(state, **kwargs):
    return {'clock': state.app.clock.value}


@Panel.register
def dump_revoked(state, **kwargs):
    return list(worker_state.revoked)


@Panel.register
def hello(state, **kwargs):
    return {'revoked': worker_state.revoked._data,
            'clock': state.app.clock.forward()}


@Panel.register
def dump_tasks(state, taskinfoitems=None, **kwargs):
    tasks = state.app.tasks
    taskinfoitems = taskinfoitems or DEFAULT_TASK_INFO_ITEMS

    def _extract_info(task):
        fields = dict((field, str(getattr(task, field, None)))
                      for field in taskinfoitems
                      if getattr(task, field, None) is not None)
        if fields:
            info = ['='.join(f) for f in items(fields)]
            return '{0} [{1}]'.format(task.name, ' '.join(info))
        return task.name

    return [_extract_info(tasks[task]) for task in sorted(tasks)]


@Panel.register
def ping(state, **kwargs):
    return {'ok': 'pong'}


@Panel.register
def pool_grow(state, n=1, **kwargs):
    if state.consumer.controller.autoscaler:
        state.consumer.controller.autoscaler.force_scale_up(n)
    else:
        state.consumer.pool.grow(n)
    return {'ok': 'spawned worker processes'}


@Panel.register
def pool_shrink(state, n=1, **kwargs):
    if state.consumer.controller.autoscaler:
        state.consumer.controller.autoscaler.force_scale_down(n)
    else:
        state.consumer.pool.shrink(n)
    return {'ok': 'terminated worker processes'}


@Panel.register
def pool_restart(state, modules=None, reload=False, reloader=None, **kwargs):
    if state.app.conf.CELERYD_POOL_RESTARTS:
        state.consumer.controller.reload(modules, reload, reloader=reloader)
        return {'ok': 'reload started'}
    else:
        raise ValueError('Pool restarts not enabled')


@Panel.register
def autoscale(state, max=None, min=None):
    autoscaler = state.consumer.controller.autoscaler
    if autoscaler:
        max_, min_ = autoscaler.update(max, min)
        return {'ok': 'autoscale now min={0} max={1}'.format(max_, min_)}
    raise ValueError('Autoscale not enabled')


@Panel.register
def shutdown(state, msg='Got shutdown from remote', **kwargs):
    logger.warning(msg)
    raise SystemExit(msg)


@Panel.register
def add_consumer(state, queue, exchange=None, exchange_type=None,
                 routing_key=None, **options):
    state.consumer.add_task_queue(queue, exchange, exchange_type,
                                  routing_key, **options)
    return {'ok': 'add consumer {0}'.format(queue)}


@Panel.register
def cancel_consumer(state, queue=None, **_):
    state.consumer.cancel_task_queue(queue)
    return {'ok': 'no longer consuming from {0}'.format(queue)}


@Panel.register
def active_queues(state):
    """Returns the queues associated with each worker."""
    return [dict(queue.as_dict(recurse=True))
            for queue in state.consumer.task_consumer.queues]


def _wanted_config_key(key):
    return key.isupper() and not key.startswith('__')


@Panel.register
def dump_conf(state, **kwargs):
    return jsonify(dict(state.app.conf), keyfilter=_wanted_config_key)


@Panel.register
def election(state, id, topic, action=None, **kwargs):
    state.consumer.gossip.election(id, topic, action)
