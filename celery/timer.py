import time


class TimeoutError(Exception):
    """The event has timed out."""


class EventTimer(object):
    """Do something at an interval."""

    def __init__(self, event, interval=None):
        self.event = event
        self.interval = interval
        self.last_triggered = None

    def tick(self):
        if not self.interval: # never trigger if no interval.
            return
        if not self.last_triggered or \
                time.time() > self.last_triggered + self.interval:
            self.event()
            self.last_triggered = time.time()


