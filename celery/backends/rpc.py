# -*- coding: utf-8 -*-
"""
    celery.backends.rpc
    ~~~~~~~~~~~~~~~~~~~

    RPC-style result backend, using reply-to and one queue per client.

"""
from __future__ import absolute_import

import kombu

from threading import local

from kombu.common import maybe_declare, oid_from

from celery import current_task
from celery.backends import amqp


class RPCBackend(amqp.AMQPBackend):
    _tls = local()

    class Consumer(kombu.Consumer):
        auto_declare = False

    def _create_exchange(self, name, type='direct', persistent=False):
        return self.Exchange('c.rep', type=type, delivery_mode=1,
                             durable=False, auto_delete=False)

    def on_task_call(self, producer, task_id):
        maybe_declare(self.binding(producer.channel), retry=True)
        return self.extra_properties

    @property
    def extra_properties(self):
        return {'reply_to': self.oid}

    def _create_binding(self, task_id):
        return self.binding

    def _many_bindings(self, ids):
        return [self.binding]

    def _routing_key(self, task_id):
        return current_task.request.reply_to

    def on_reply_declare(self, task_id):
        pass

    @property
    def binding(self):
        return self.Queue(self.oid, self.exchange, self.oid,
                          durable=False, auto_delete=False)

    @property
    def oid(self):
        try:
            return self._tls.OID
        except AttributeError:
            oid = self._tls.OID = oid_from(self)
            return oid
