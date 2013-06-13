# -*- coding: utf-8 -*-
"""
    celery.worker.strategy
    ~~~~~~~~~~~~~~~~~~~~~~

    Task execution strategy (optimization).

"""
from __future__ import absolute_import

import logging

from kombu.utils.encoding import safe_repr

from celery.utils.log import get_logger
from celery.utils.timer2 import to_timestamp
from celery.utils.timeutils import timezone

logger = get_logger(__name__)

from .job import Request
from .state import task_reserved


def default(task, app, consumer,
            info=logger.info, error=logger.error, task_reserved=task_reserved,
            to_system_tz=timezone.to_system):
    hostname = consumer.hostname
    eventer = consumer.event_dispatcher
    Req = Request
    connection_errors = consumer.connection_errors
    _does_info = logger.isEnabledFor(logging.INFO)
    events = eventer and eventer.enabled
    send_event = eventer.send
    timer_apply_at = consumer.timer.apply_at
    apply_eta_task = consumer.apply_eta_task
    rate_limits_enabled = not consumer.disable_rate_limits
    bucket = consumer.task_buckets[task.name]
    handle = consumer.on_task
    limit_task = consumer._limit_task

    def task_message_handler(message, body, ack, to_timestamp=to_timestamp):
        req = Req(body, on_ack=ack, app=app, hostname=hostname,
                  eventer=eventer, task=task,
                  connection_errors=connection_errors,
                  delivery_info=message.delivery_info)
        if req.revoked():
            return

        if _does_info:
            info('Got task from broker: %s', req)

        if events:
            send_event(
                'task-received',
                uuid=req.id, name=req.name,
                args=safe_repr(req.args), kwargs=safe_repr(req.kwargs),
                retries=req.request_dict.get('retries', 0),
                eta=req.eta and req.eta.isoformat(),
                expires=req.expires and req.expires.isoformat(),
            )

        if req.eta:
            try:
                if req.utc:
                    eta = to_timestamp(to_system_tz(req.eta))
                else:
                    eta = to_timestamp(req.eta, timezone.local)
            except OverflowError as exc:
                error("Couldn't convert eta %s to timestamp: %r. Task: %r",
                      req.eta, exc, req.info(safe=True), exc_info=True)
                req.acknowledge()
            else:
                consumer.qos.increment_eventually()
                timer_apply_at(
                    eta, apply_eta_task, (req, ), priority=6,
                )
        else:
            if rate_limits_enabled:
                if bucket:
                    return limit_task(req, bucket, 1)
            task_reserved(req)
            handle(req)

    return task_message_handler
