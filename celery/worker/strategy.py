# -*- coding: utf-8 -*-
"""Task execution strategy (optimization)."""
import logging
from typing import (
    Awaitable, Callable, Dict, List, Mapping, NamedTuple, Sequence, Tuple,
)
from kombu.async.timer import to_timestamp
from kombu.types import MessageT
from celery.exceptions import InvalidTaskError
from celery.types import AppT, WorkerConsumerT
from celery.utils.log import get_logger
from celery.utils.saferepr import saferepr
from celery.utils.time import timezone
from .request import Request, create_request_cls
from .state import task_reserved

__all__ = ['default']

logger = get_logger(__name__)

# pylint: disable=redefined-outer-name
# We cache globals and attribute lookups, so disable this warning.


class converted_message_t(NamedTuple):
    """Describes a converted message."""

    body: Tuple[List, Dict, Mapping]
    headers: Mapping
    decoded: bool
    utc: bool


def proto1_to_proto2(message: MessageT, body: Mapping) -> converted_message_t:
    """Convert Task message protocol 1 arguments to protocol 2.

    Returns:
        Tuple: of ``(body, headers, already_decoded_status, utc)``
    """
    try:
        args, kwargs = body['args'], body['kwargs']
        kwargs.items  # pylint: disable=pointless-statement
    except KeyError:
        raise InvalidTaskError('Message does not have args/kwargs')
    except AttributeError:
        raise InvalidTaskError(
            'Task keyword arguments must be a mapping',
        )
    body.update(
        argsrepr=saferepr(args),
        kwargsrepr=saferepr(kwargs),
        headers=message.headers,
    )
    try:
        body['group'] = body['taskset']
    except KeyError:
        pass
    embed = {
        'callbacks': body.get('callbacks'),
        'errbacks': body.get('errbacks'),
        'chord': body.get('chord'),
        'chain': None,
    }
    return converted_message_t(
        body=(args, kwargs, embed),
        headers=body,
        decoded=True,
        utc=body.get('utc', True),
    )


StrategyT = Callable[
    [MessageT, Mapping, Callable, Callable, Sequence[Callable]],
    Awaitable,
]


def default(task: str, app: AppT, consumer: WorkerConsumerT,
            *,
            info: Callable = logger.info,
            error: Callable = logger.error,
            task_reserved: Callable = task_reserved,
            to_system_tz: Callable = timezone.to_system,
            proto1_to_proto2: Callable = proto1_to_proto2,
            bytes: Callable = bytes) -> StrategyT:
    """Default task execution strategy.

    Note:
        Strategies are here as an optimization, so sadly
        it's not very easy to override.
    """
    hostname = consumer.hostname
    connection_errors = consumer.connection_errors
    _does_info = logger.isEnabledFor(logging.INFO)

    # task event related
    # (optimized to avoid calling request.send_event)
    eventer = consumer.event_dispatcher
    events = eventer and eventer.enabled
    send_event = eventer.send
    task_sends_events = events and task.send_events

    call_at = consumer.timer.call_at
    apply_eta_task = consumer.apply_eta_task
    rate_limits_enabled = not consumer.disable_rate_limits
    get_bucket = consumer.task_buckets.__getitem__
    handle = consumer.on_task_request
    limit_task = consumer._limit_task
    Req = create_request_cls(Request, task, consumer.pool, hostname, eventer)

    revoked_tasks = consumer.controller.state.revoked
    convert_to_timestamp = to_timestamp

    async def task_message_handler(
            message: MessageT,
            body: Mapping,
            ack: Callable,
            reject: Callable,
            callbacks: Sequence[Callable]) -> Awaitable:
        if body is None:
            body, headers, decoded, utc = (
                message.body, message.headers, False, True,
            )
        else:
            body, headers, decoded, utc = proto1_to_proto2(message, body)

        req = Req(
            message,
            on_ack=ack, on_reject=reject, app=app, hostname=hostname,
            eventer=eventer, task=task, connection_errors=connection_errors,
            body=body, headers=headers, decoded=decoded, utc=utc,
        )
        if _does_info:
            info('Received task: %s', req)
        if (req.expires or req.id in revoked_tasks) and req.revoked():
            return

        if task_sends_events:
            send_event(
                'task-received',
                uuid=req.id, name=req.name,
                args=req.argsrepr, kwargs=req.kwargsrepr,
                root_id=req.root_id, parent_id=req.parent_id,
                retries=req.request_dict.get('retries', 0),
                eta=req.eta and req.eta.isoformat(),
                expires=req.expires and req.expires.isoformat(),
            )

        if req.eta:
            try:
                if req.utc:
                    eta = convert_to_timestamp(to_system_tz(req.eta))
                else:
                    eta = convert_to_timestamp(req.eta, timezone.local)
            except (OverflowError, ValueError) as exc:
                error("Couldn't convert ETA %r to timestamp: %r. Task: %r",
                      req.eta, exc, req.info(safe=True), exc_info=True)
                req.reject(requeue=False)
            else:
                consumer.qos.increment_eventually()
                call_at(eta, apply_eta_task, (req,), priority=6)
        else:
            if rate_limits_enabled:
                bucket = get_bucket(task.name)
                if bucket:
                    return limit_task(req, bucket, 1)
            task_reserved(req)
            if callbacks:
                [callback(req) for callback in callbacks]
            handle(req)

    return task_message_handler
