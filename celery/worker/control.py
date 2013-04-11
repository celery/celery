# -*- coding: utf-8 -*-
"""
    celery.worker.control
    ~~~~~~~~~~~~~~~~~~~~~

    Remote control commands.

"""
from __future__ import absolute_import

import logging
import os

from datetime import datetime

from kombu.utils.encoding import safe_repr

from celery.platforms import signals as _signals
from celery.utils import timeutils
from celery.utils.compat import UserDict
from celery.utils.log import get_logger
from celery.utils import jsonify

from . import state
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
def revoke(panel, task_id, terminate=False, signal=None, **kwargs):
    """Revoke task by task id."""
    revoked.add(task_id)
    if terminate:
        signum = _signals.signum(signal or 'TERM')
        for request in state.reserved_requests:
            if request.id == task_id:
                logger.info('Terminating %s (%s)', task_id, signum)
                request.terminate(panel.consumer.pool, signal=signum)
                break
        else:
            return {'ok': 'terminate: task %s not found' % (task_id, )}
        return {'ok': 'terminating %s (%s)' % (task_id, signal)}

    logger.info('Revoking task %s', task_id)
    return {'ok': 'revoking task %s' % (task_id, )}


@Panel.register
def report(panel):
    return {'ok': panel.app.bugreport()}


@Panel.register
def enable_events(panel):
    dispatcher = panel.consumer.event_dispatcher
    if not dispatcher.enabled:
        dispatcher.enable()
        dispatcher.send('worker-online')
        logger.info('Events enabled by remote.')
        return {'ok': 'events enabled'}
    return {'ok': 'events already enabled'}


@Panel.register
def disable_events(panel):
    dispatcher = panel.consumer.event_dispatcher
    if dispatcher.enabled:
        dispatcher.send('worker-offline')
        dispatcher.disable()
        logger.info('Events disabled by remote.')
        return {'ok': 'events disabled'}
    return {'ok': 'events already disabled'}


@Panel.register
def heartbeat(panel):
    logger.debug('Heartbeat requested by remote.')
    dispatcher = panel.consumer.event_dispatcher
    dispatcher.send('worker-heartbeat', freq=5, **state.SOFTWARE_INFO)


@Panel.register
def rate_limit(panel, task_name, rate_limit, **kwargs):
    """Set new rate limit for a task type.

    See :attr:`celery.task.base.Task.rate_limit`.

    :param task_name: Type of task.
    :param rate_limit: New rate limit.

    """

    try:
        timeutils.rate(rate_limit)
    except ValueError, exc:
        return {'error': 'Invalid rate limit string: %s' % exc}

    try:
        panel.app.tasks[task_name].rate_limit = rate_limit
    except KeyError:
        logger.error('Rate limit attempt for unknown task %s',
                     task_name, exc_info=True)
        return {'error': 'unknown task'}

    if not hasattr(panel.consumer.ready_queue, 'refresh'):
        logger.error('Rate limit attempt, but rate limits disabled.')
        return {'error': 'rate limits disabled'}

    panel.consumer.ready_queue.refresh()

    if not rate_limit:
        logger.info('Rate limits disabled for tasks of type %s', task_name)
        return {'ok': 'rate limit disabled successfully'}

    logger.info('New rate limit for tasks of type %s: %s.',
                task_name, rate_limit)
    return {'ok': 'new rate limit set successfully'}


@Panel.register
def time_limit(panel, task_name=None, hard=None, soft=None, **kwargs):
    try:
        task = panel.app.tasks[task_name]
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
def dump_schedule(panel, safe=False, **kwargs):
    from celery.worker.job import Request
    schedule = panel.consumer.timer.schedule
    if not schedule.queue:
        logger.debug('--Empty schedule--')
        return []

    formatitem = lambda i, item: '%s. %s pri%s %r' % (
        i, datetime.utcfromtimestamp(item['eta']),
        item['priority'], item['item'],
    )
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug('* Dump of current schedule:\n%s', '\n'.join(
            formatitem(i, item) for i, item in enumerate(schedule.info())
        ))
    scheduled_tasks = []
    for info in schedule.info():
        item = info['item']
        if item.args and isinstance(item.args[0], Request):
            scheduled_tasks.append({
                'eta': info['eta'],
                'priority': info['priority'],
                'request': item.args[0].info(safe=safe),
            })
    return scheduled_tasks


@Panel.register
def dump_reserved(panel, safe=False, **kwargs):
    reserved = state.reserved_requests - state.active_requests
    if not reserved:
        logger.debug('--Empty queue--')
        return []
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug('* Dump of currently reserved tasks:\n%s',
                     '\n'.join(safe_repr(r) for r in reserved))
    return [request.info(safe=safe) for request in reserved]


@Panel.register
def dump_active(panel, safe=False, **kwargs):
    return [request.info(safe=safe) for request in state.active_requests]


@Panel.register
def stats(panel, **kwargs):
    asinfo = {}
    if panel.consumer.controller.autoscaler:
        asinfo = panel.consumer.controller.autoscaler.info()
    return {'total': state.total_count,
            'consumer': panel.consumer.info,
            'pool': panel.consumer.pool.info,
            'autoscaler': asinfo,
            'pid': os.getpid()}


@Panel.register
def dump_revoked(panel, **kwargs):
    return list(state.revoked)


@Panel.register
def dump_tasks(panel, taskinfoitems=None, **kwargs):
    tasks = panel.app.tasks
    taskinfoitems = taskinfoitems or DEFAULT_TASK_INFO_ITEMS

    def _extract_info(task):
        fields = dict(
            (field, str(getattr(task, field, None)))
            for field in taskinfoitems
            if getattr(task, field, None) is not None)
        info = ['='.join(f) for f in fields.items()]
        if not info:
            return task.name
        return '%s [%s]' % (task.name, ' '.join(info))

    info = [_extract_info(tasks[task]) for task in sorted(tasks)]
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug('* Dump of currently registered tasks:\n%s',
                     '\n'.join(info))
    return info


@Panel.register
def ping(panel, **kwargs):
    return 'pong'


@Panel.register
def pool_grow(panel, n=1, **kwargs):
    if panel.consumer.controller.autoscaler:
        panel.consumer.controller.autoscaler.force_scale_up(n)
    else:
        panel.consumer.pool.grow(n)
    return {'ok': 'spawned worker processes'}


@Panel.register
def pool_shrink(panel, n=1, **kwargs):
    if panel.consumer.controller.autoscaler:
        panel.consumer.controller.autoscaler.force_scale_down(n)
    else:
        panel.consumer.pool.shrink(n)
    return {'ok': 'terminated worker processes'}


@Panel.register
def pool_restart(panel, modules=None, reload=False, reloader=None, **kwargs):
    if panel.app.conf.CELERYD_POOL_RESTARTS:
        panel.consumer.controller.reload(modules, reload, reloader=reloader)
        return {'ok': 'reload started'}
    else:
        raise ValueError('Pool restarts not enabled')


@Panel.register
def autoscale(panel, max=None, min=None):
    autoscaler = panel.consumer.controller.autoscaler
    if autoscaler:
        max_, min_ = autoscaler.update(max, min)
        return {'ok': 'autoscale now min=%r max=%r' % (max_, min_)}
    raise ValueError('Autoscale not enabled')


@Panel.register
def shutdown(panel, msg='Got shutdown from remote', **kwargs):
    logger.warning(msg)
    raise SystemExit(msg)


@Panel.register
def add_consumer(panel, queue, exchange=None, exchange_type=None,
                 routing_key=None, **options):
    panel.consumer.add_task_queue(queue, exchange, exchange_type,
                                  routing_key, **options)
    return {'ok': 'add consumer %r' % (queue, )}


@Panel.register
def cancel_consumer(panel, queue=None, **_):
    panel.consumer.cancel_task_queue(queue)
    return {'ok': 'no longer consuming from %s' % (queue, )}


@Panel.register
def active_queues(panel):
    """Returns the queues associated with each worker."""
    return [dict(queue.as_dict(recurse=True))
            for queue in panel.consumer.task_consumer.queues]


@Panel.register
def dump_conf(panel, **kwargs):
    return jsonify(dict(panel.app.conf))
