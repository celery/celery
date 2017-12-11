"""Implementation for the app.events shortcuts."""
<<<<<<< HEAD
from __future__ import absolute_import, unicode_literals

from contextlib import contextmanager

=======
from contextlib import contextmanager
from celery.events import EventDispatcher, EventReceiver
from celery.events.state import State
from celery.types import AppT
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
from kombu.utils.objects import cached_property


class Events:
    """Implements app.events."""

    receiver_cls = 'celery.events.receiver:EventReceiver'
    dispatcher_cls = 'celery.events.dispatcher:EventDispatcher'
    state_cls = 'celery.events.state:State'

    def __init__(self, app: AppT = None):
        self.app = app

    @cached_property
    def Receiver(self) -> EventReceiver:
        return self.app.subclass_with_self(
            self.receiver_cls, reverse='events.Receiver')

    @cached_property
    def Dispatcher(self) -> EventDispatcher:
        return self.app.subclass_with_self(
            self.dispatcher_cls, reverse='events.Dispatcher')

    @cached_property
    def State(self) -> State:
        return self.app.subclass_with_self(
            self.state_cls, reverse='events.State')

    @contextmanager
    def default_dispatcher(
            self,
            hostname: str = None,
            enabled: bool = True,
            buffer_while_offline: bool = False) -> EventDispatcher:
        with self.app.amqp.producer_pool.acquire(block=True) as prod:
            # pylint: disable=too-many-function-args
            # This is a property pylint...
            with self.Dispatcher(prod.connection, hostname, enabled,
                                 prod.channel, buffer_while_offline) as d:
                yield d
