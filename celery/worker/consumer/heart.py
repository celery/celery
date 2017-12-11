"""Worker Event Heartbeat Bootstep."""
<<<<<<< HEAD
from __future__ import absolute_import, unicode_literals

=======
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
from celery import bootsteps
from celery.types import WorkerConsumerT
from celery.worker import heartbeat

from .events import Events

__all__ = ('Heart',)


class Heart(bootsteps.StartStopStep):
    """Bootstep sending event heartbeats.

    This service sends a ``worker-heartbeat`` message every n seconds.

    Note:
        Not to be confused with AMQP protocol level heartbeats.
    """

    requires = (Events,)

    def __init__(self, c: WorkerConsumerT,
                 without_heartbeat: bool = False,
                 heartbeat_interval: float = None,
                 **kwargs) -> None:
        self.enabled = not without_heartbeat
        self.heartbeat_interval = heartbeat_interval
        c.heart = None
        super(Heart, self).__init__(c, **kwargs)

    async def start(self, c: WorkerConsumerT) -> None:
        c.heart = heartbeat.Heart(
            c.timer, c.event_dispatcher, self.heartbeat_interval,
        )
        await c.heart.start()

    async def stop(self, c: WorkerConsumerT) -> None:
        heart, c.heart = c.heart, None
        if heart:
            await heart.stop()
    shutdown = stop
