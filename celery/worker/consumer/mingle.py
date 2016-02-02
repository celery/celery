from __future__ import absolute_import, unicode_literals

from operator import itemgetter

from celery import bootsteps
from celery.five import items, values
from celery.utils.log import get_logger

from celery.worker.state import revoked

from .events import Events

__all__ = ['Mingle']

MINGLE_GET_FIELDS = itemgetter('clock', 'revoked')

logger = get_logger(__name__)
info = logger.info


class Mingle(bootsteps.StartStopStep):

    label = 'Mingle'
    requires = (Events,)
    compatible_transports = {'amqp', 'redis'}

    def __init__(self, c, without_mingle=False, **kwargs):
        self.enabled = not without_mingle and self.compatible_transport(c.app)

    def compatible_transport(self, app):
        with app.connection_for_read() as conn:
            return conn.transport.driver_type in self.compatible_transports

    def start(self, c):
        info('mingle: searching for neighbors')
        I = c.app.control.inspect(timeout=1.0, connection=c.connection)
        replies = I.hello(c.hostname, revoked._data) or {}
        replies.pop(c.hostname, None)
        if replies:
            info('mingle: sync with %s nodes',
                 len([reply for reply, value in items(replies) if value]))
            for reply in values(replies):
                if reply:
                    try:
                        other_clock, other_revoked = MINGLE_GET_FIELDS(reply)
                    except KeyError:  # reply from pre-3.1 worker
                        pass
                    else:
                        c.app.clock.adjust(other_clock)
                        revoked.update(other_revoked)
            info('mingle: sync complete')
        else:
            info('mingle: all alone')
