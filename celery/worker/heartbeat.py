import threading
from time import time, sleep


class Heart(threading.Thread):
    """Thread sending heartbeats at an interval.

    :param eventer: Event dispatcher used to send the event.
    :keyword interval: Time in seconds between heartbeats.
        Default is 2 minutes.

    .. attribute:: bpm

        Beats per minute.

    """
    bpm = 0.5

    def __init__(self, eventer, interval=None):
        super(Heart, self).__init__()
        self.eventer = eventer
        self.bpm = interval and interval / 60.0 or self.bpm
        self._shutdown = threading.Event()
        self._stopped = threading.Event()
        self.setDaemon(True)
        self._state = None

    def run(self):
        self._state = "RUN"
        bpm = self.bpm
        dispatch = self.eventer.send

        dispatch("worker-online")

        # We can't sleep all of the interval, because then
        # it takes 60 seconds (or value of interval) to shutdown
        # the thread.

        last_beat = None
        while 1:
            now = time()
            if not last_beat or now > last_beat + (60.0 / bpm):
                last_beat = now
                dispatch("worker-heartbeat")
            if self._shutdown.isSet():
                break
            sleep(1)

        try:
            dispatch("worker-offline")
        finally:
            self._stopped.set()

    def stop(self):
        """Gracefully shutdown the thread."""
        if not self._state == "RUN":
            return
        self._state = "CLOSE"
        self._shutdown.set()
        self._stopped.wait() # block until this thread is done
        if self.isAlive():
            self.join(1e100)
