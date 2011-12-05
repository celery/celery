from .job import TaskRequest

from ..utils.coroutine import coroutine


def default(task, app, consumer):

    @coroutine
    def task_message_handler(self):
        logger = consumer.logger
        hostname = consumer.hostname
        eventer = consumer.event_dispatcher
        Request = TaskRequest.from_message
        handle = consumer.on_task

        while 1:
            M, B, A = (yield)
            handle(Request(M, B, A, app=app, logger=logger,
                                    hostname=hostname, eventer=eventer))

    return task_message_handler()
