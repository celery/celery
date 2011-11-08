# -*- coding: utf-8 -*-
"""
    celery.worker.heartbeat
    ~~~~~~~~~~~~~~~~~~~~~~~

    This is the internal thread that sends heartbeat events
    at regular intervals.

    :copyright: (c) 2009 - 2011 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from .state import SOFTWARE_INFO


class Heart(object):
    """Timer sending heartbeats at regular intervals.

    :param timer: Timer instance.
    :param eventer: Event dispatcher used to send the event.
    :keyword interval: Time in seconds between heartbeats.
                       Default is 30 seconds.

    """

    def __init__(self, timer, eventer, interval=None):
        self.timer = timer
        self.eventer = eventer
        self.interval = interval or 30
        self.tref = None

    def _send(self, event):
        return self.eventer.send(event, **SOFTWARE_INFO)

    def start(self):
        self._send("worker-online")
        self.tref = self.timer.apply_interval(self.interval * 1000.0,
                self._send, ("worker-heartbeat", ))

    def stop(self):
        if self.tref is not None:
            self.timer.cancel(self.tref)
            self.tref = None
        self._send("worker-offline")
