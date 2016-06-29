# -*- coding: utf-8 -*-
"""
    ``celery.backends.amqp``
    ~~~~~~~~~~~~~~~~~~~~~~~~

    The AMQP result backend.

    This backend publishes results as messages.

"""
from __future__ import absolute_import, unicode_literals

from .rpc import BaseRPCBackend

from celery.utils import deprecated

__all__ = ['AMQPBackend']


def repair_uuid(s):
    # Historically the dashes in UUIDS are removed from AMQ entity names,
    # but there is no known reason to.  Hopefully we'll be able to fix
    # this in v4.0.
    return '%s-%s-%s-%s-%s' % (s[:8], s[8:12], s[12:16], s[16:20], s[20:])


class AMQPBackend(BaseRPCBackend):
    """Publishes results by sending messages."""

    def __init__(self, *args, **kwargs):
        deprecated.warn(
            'The AMQP backend', deprecation='4.0', removal='5.0',
            alternative='Please use RPC backend or a persistent backend.')
        super(AMQPBackend, self).__init__(*args, **kwargs)

    def _create_exchange(self, name, type='direct', delivery_mode=2):
        return self.Exchange(name=name,
                             type=type,
                             delivery_mode=delivery_mode,
                             durable=self.persistent,
                             auto_delete=False)

    def _create_binding(self, task_id):
        name = self.rkey(task_id)
        return self.Queue(name=name,
                          exchange=self.exchange,
                          routing_key=name,
                          durable=self.persistent,
                          auto_delete=self.auto_delete,
                          queue_arguments=self.queue_arguments)

    def on_task_call(self, producer, task_id):
        pass

    def rkey(self, task_id):
        return task_id.replace('-', '')

    def destination_for(self, task_id, request):
        if request:
            return self.rkey(task_id), request.correlation_id or task_id
        return self.rkey(task_id), task_id

    def on_reply_declare(self, task_id):
        return [self._create_binding(task_id)]

    def on_result_fulfilled(self, result):
        self.result_consumer.cancel_for(result.id)

    def as_uri(self, include_password=True):
        return 'amqp://'
