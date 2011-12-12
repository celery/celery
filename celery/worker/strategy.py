from __future__ import absolute_import

from .job import Request


def default(task, app, consumer):
    logger = consumer.logger
    hostname = consumer.hostname
    eventer = consumer.event_dispatcher
    Req = Request
    handle = consumer.on_task
    connection_errors = consumer.connection_errors

    def task_message_handler(M, B, A):
        handle(Req(B, on_ack=A, app=app, hostname=hostname,
                         eventer=eventer, logger=logger,
                         connection_errors=connection_errors,
                         delivery_info=M.delivery_info,
                         task=task))

    return task_message_handler
