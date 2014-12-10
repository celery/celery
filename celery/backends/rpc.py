# -*- coding: utf-8 -*-
"""
    celery.backends.rpc
    ~~~~~~~~~~~~~~~~~~~

    RPC-style result backend, using reply-to and one queue per client.

"""
from __future__ import absolute_import

from kombu import Consumer, Exchange
from kombu.common import maybe_declare
from kombu.utils import cached_property

from celery import current_task
from celery.backends import amqp

__all__ = ['RPCBackend']


class RPCBackend(amqp.AMQPBackend):
    persistent = False

    class Consumer(Consumer):
        auto_declare = False

    def _create_exchange(self, name, type='direct', delivery_mode=2):
        # uses direct to queue routing (anon exchange).
        return Exchange(None)

    def on_task_call(self, producer, task_id):
        maybe_declare(self.binding(producer.channel), retry=True)

    def _create_binding(self, task_id):
        return self.binding

    def _many_bindings(self, ids):
        return [self.binding]

    def rkey(self, task_id):
        return task_id

    def destination_for(self, task_id, request):
        # Request is a new argument for backends, so must still support
        # old code that rely on current_task
        try:
            request = request or current_task.request
        except AttributeError:
            raise RuntimeError(
                'RPC backend missing task request for {0!r}'.format(task_id),
            )
        return request.reply_to, request.correlation_id or task_id

    def on_reply_declare(self, task_id):
        pass

    @property
    def binding(self):
        return self.Queue(self.oid, self.exchange, self.oid,
                          durable=False, auto_delete=True)

    @cached_property
    def oid(self):
        return self.app.oid

    def get_task_meta(self, task_id, backlog_limit=1000):
        # Polling and using basic_get
        with self.app.pool.acquire_channel(block=True) as (_, channel):
            binding = self._create_binding(task_id)(channel)
            binding.declare()

            # Discard all but the latest message per task id
            latest_by_id = dict()
            prev = acc = None
            for i in range(backlog_limit):  # spool ffwd
                acc = binding.get(
                    accept=self.accept, no_ack=False,
                )
                if not acc:  # no more messages
                    break
                _id = acc.payload.get('task_id')
                if _id:
                    prev, latest_by_id[_id] = latest_by_id.get(_id), acc
                if prev:
                    # backends are not expected to keep history,
                    # so we delete everything except the most recent state.
                    prev.ack()
                    prev = None
            else:
                raise self.BacklogLimitExceeded(task_id)

            for id, msg in latest_by_id.items():
                self._cache[id] = msg.payload
                msg.requeue()
                
            # If we found updated state for our task_id it's in the cache now
            try:
                return self._cache[task_id]
            except KeyError:
                # result probably pending.
                return {'status': states.PENDING, 'result': None}
    poll = get_task_meta  # XXX compat
