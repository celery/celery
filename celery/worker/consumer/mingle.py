"""Worker <-> Worker Sync at startup (Bootstep)."""
<<<<<<< HEAD
from __future__ import absolute_import, unicode_literals

=======
from typing import Mapping
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
from celery import bootsteps
from celery.types import AppT, WorkerConsumerT
from celery.utils.log import get_logger

from .events import Events

__all__ = ('Mingle',)

logger = get_logger(__name__)
debug, info, exception = logger.debug, logger.info, logger.exception


class Mingle(bootsteps.StartStopStep):
    """Bootstep syncing state with neighbor workers.

    At startup, or upon consumer restart, this will:

    - Sync logical clocks.
    - Sync revoked tasks.

    """

    label = 'Mingle'
    requires = (Events,)
    compatible_transports = {'amqp', 'redis'}

    def __init__(self, c: WorkerConsumerT,
                 without_mingle: bool = False,
                 **kwargs) -> None:
        self.enabled = not without_mingle and self.compatible_transport(c.app)
        super(Mingle, self).__init__(
            c, without_mingle=without_mingle, **kwargs)

    def compatible_transport(self, app: AppT) -> bool:
        with app.connection_for_read() as conn:
            return conn.transport.driver_type in self.compatible_transports

    async def start(self, c: WorkerConsumerT) -> None:
        await self.sync(c)

    async def sync(self, c: WorkerConsumerT) -> None:
        info('mingle: searching for neighbors')
        replies = await self.send_hello(c)
        if replies:
            info('mingle: sync with %s nodes',
                 len([reply for reply, value in replies.items() if value]))
            [await self.on_node_reply(c, nodename, reply)
             for nodename, reply in replies.items() if reply]
            info('mingle: sync complete')
        else:
            info('mingle: all alone')

    async def send_hello(self, c: WorkerConsumerT) -> Mapping:
        inspect = c.app.control.inspect(timeout=1.0, connection=c.connection)
        our_revoked = c.controller.state.revoked
        replies = inspect.hello(c.hostname, our_revoked._data) or {}
        replies.pop(c.hostname, None)  # delete my own response
        return replies

    async def on_node_reply(self, c: WorkerConsumerT,
                            nodename: str, reply: Mapping) -> None:
        debug('mingle: processing reply from %s', nodename)
        try:
            await self.sync_with_node(c, **reply)
        except MemoryError:
            raise
        except Exception as exc:  # pylint: disable=broad-except
            exception('mingle: sync with %s failed: %r', nodename, exc)

    async def sync_with_node(self, c: WorkerConsumerT,
                             clock: int = None,
                             revoked: Mapping = None,
                             **kwargs) -> None:
        self.on_clock_event(c, clock)
        self.on_revoked_received(c, revoked)

    def on_clock_event(self, c: WorkerConsumerT, clock: int):
        c.app.clock.adjust(clock) if clock else c.app.clock.forward()

    def on_revoked_received(self,
                            c: WorkerConsumerT, revoked: Mapping) -> None:
        if revoked:
            c.controller.state.revoked.update(revoked)
