"""celery.timer"""
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
        """Run a event timer clock tick.
       
        When the interval has run, the event will be triggered.
        If interval is not set, the event will never be triggered.

        """
        if not self.interval: # never trigger if no interval.
            return
        if not self.last_triggered or \
                time.time() > self.last_triggered + self.interval:
            self.event()
            self.last_triggered = time.time()


class TimeoutTimer(object):
    """A timer that raises ``TimeoutError`` when the time has run out."""

    def __init__(self, timeout):
        self.timeout = timeout
        self.time_start = time.time()

    def tick(self):
        """Run a timeout timer clock tick.

        When ``timeout`` seconds has passed, it will raise a
        :class:`TimeoutTimer` exception.
        If ``timeout`` is not set, it will never time out.

        """
        if not self.timeout:
            return
        if time.time() > self.time_start + self.timeout:
            raise TimeoutError("The operation timed out.")
