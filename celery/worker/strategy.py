from __future__ import absolute_import

from .job import TaskRequest


def default(task, app, consumer):
    logger = consumer.logger
    hostname = consumer.hostname
    eventer = consumer.event_dispatcher
    Request = TaskRequest.from_message
    handle = consumer.on_task

    def task_message_handler(M, B, A):
        handle(Request(M, B, A, app=app, logger=logger,
                                hostname=hostname, eventer=eventer))

    return task_message_handler
