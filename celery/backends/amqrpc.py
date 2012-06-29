from __future__ import absolute_import

import os
import uuid

from threading import local

from celery.backends import amqp

try:
    from thread import get_ident            # noqa
except ImportError:                         # pragma: no cover
    try:
        from dummy_thread import get_ident  # noqa
    except ImportError:                     # pragma: no cover
        from _thread import get_ident       # noqa

_nodeid = uuid.getnode()


class AMQRPCBackend(amqp.AMQPBackend):
    _tls = local()

    def _create_exchange(self, name, type='direct', persistent=False):
        return self.Exchange('c.amqrpc', type=type, delivery_mode=1,
                durable=False, auto_delete=False)

    def on_task_apply(self, task_id):
        with self.app.pool.acquire_channel(block=True) as (conn, channel):
            self.binding(channel).declare()
            return {'reply_to': self.oid}

    def _create_binding(self, task_id):
        print("BINDING: %r" % (self.binding, ))
        return self.binding

    def _routing_key(self, task_id):
        from celery import current_task
        return current_task.request.reply_to

    @property
    def binding(self):
        return self.Queue(self.oid, self.exchange, self.oid,
                          durable=False, auto_delete=True)

    @property
    def oid(self):
        try:
            return self._tls.OID
        except AttributeError:
            ent = '%x-%x-%x' % (_nodeid, os.getpid(), get_ident())
            oid = self._tls.OID = str(uuid.uuid3(uuid.NAMESPACE_OID, ent))
            return oid
