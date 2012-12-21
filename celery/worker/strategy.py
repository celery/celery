# -*- coding: utf-8 -*-
"""
    celery.worker.strategy
    ~~~~~~~~~~~~~~~~~~~~~~

    Task execution strategy (optimization).

"""
from __future__ import absolute_import

from .job import Request


def default(task, app, consumer):
    hostname = consumer.hostname
    eventer = consumer.event_dispatcher
    Req = Request
    handle = consumer.on_task
    connection_errors = consumer.connection_errors

    def task_message_handler(message, body, ack):
        handle(Req(body, on_ack=ack, app=app, hostname=hostname,
                   eventer=eventer, task=task,
                   connection_errors=connection_errors,
                   delivery_info=message.delivery_info))
    return task_message_handler
