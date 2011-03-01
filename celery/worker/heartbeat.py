import threading

from time import time, sleep

from celery.worker.state import SOFTWARE_INFO


class Heart(threading.Thread):
    """Thread sending heartbeats at regular intervals.

    :param eventer: Event dispatcher used to send the event.
    :keyword interval: Time in seconds between heartbeats.
                       Default is 2 minutes.

    """

    #: Beats per minute.
    bpm = 0.5

    def __init__(self, eventer, interval=None):
        super(Heart, self).__init__()
        self.eventer = eventer
        self.bpm = interval and interval / 60.0 or self.bpm
        self._shutdown = threading.Event()
        self.setDaemon(True)
        self.setName(self.__class__.__name__)
        self._state = None

    def run(self):
        self._state = "RUN"
        bpm = self.bpm
        dispatch = self.eventer.send

        dispatch("worker-online", **SOFTWARE_INFO)

        # We can't sleep all of the interval, because then
        # it takes 60 seconds (or value of interval) to shutdown
        # the thread.

        last_beat = None
        while 1:
            try:
                now = time()
            except TypeError:
                # we lost the race at interpreter shutdown,
                # so time() has been collected by gc.
                return

            if not last_beat or now > last_beat + (60.0 / bpm):
                last_beat = now
                dispatch("worker-heartbeat", **SOFTWARE_INFO)
            if self._shutdown.isSet():
                break
            sleep(1)

        dispatch("worker-offline", **SOFTWARE_INFO)

    def stop(self):
        """Gracefully shutdown the thread."""
        if not self._state == "RUN":
            return
        self._state = "CLOSE"
        self._shutdown.set()
        if self.isAlive():
            self.join(1e10)
