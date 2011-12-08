from __future__ import absolute_import

from .job import TaskRequest


def default(task, app, consumer):
    logger = consumer.logger
    hostname = consumer.hostname
    eventer = consumer.event_dispatcher
    Request = TaskRequest.from_message
    handle = consumer.on_task
    connection_errors = consumer.connection_errors

    def task_message_handler(M, B, A):
        handle(Request(M, B, A, app=app, logger=logger,
                                hostname=hostname, eventer=eventer,
                                connection_errors=connection_errors))

    return task_message_handler
