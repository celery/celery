from __future__ import absolute_import, unicode_literals

from kombu.common import ignore_errors

from celery import bootsteps

from .connection import Connection

__all__ = ['Events']


class Events(bootsteps.StartStopStep):

    requires = (Connection,)

    def __init__(self, c, send_events=True,
                 without_heartbeat=False, without_gossip=False, **kwargs):
        self.groups = None if send_events else ['worker']
        self.send_events = (
            send_events or
            not without_gossip or
            not without_heartbeat
        )
        c.event_dispatcher = None

    def start(self, c):
        # flush events sent while connection was down.
        prev = self._close(c)
        dis = c.event_dispatcher = c.app.events.Dispatcher(
            c.connect(), hostname=c.hostname,
            enabled=self.send_events, groups=self.groups,
            buffer_group=['task'] if c.hub else None,
            on_send_buffered=c.on_send_event_buffered if c.hub else None,
        )
        if prev:
            dis.extend_buffer(prev)
            dis.flush()

    def stop(self, c):
        pass

    def _close(self, c):
        if c.event_dispatcher:
            dispatcher = c.event_dispatcher
            # remember changes from remote control commands:
            self.groups = dispatcher.groups

            # close custom connection
            if dispatcher.connection:
                ignore_errors(c, dispatcher.connection.close)
            ignore_errors(c, dispatcher.close)
            c.event_dispatcher = None
            return dispatcher

    def shutdown(self, c):
        self._close(c)
