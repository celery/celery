import threading
from time import time, sleep


class Heart(threading.Thread):
    interval = 60

    def __init__(self, eventer, interval=None):
        super(Heart, self).__init__()
        self.eventer = eventer
        self.interval = interval or self.interval
        self._shutdown = threading.Event()
        self._stopped = threading.Event()
        self.setDaemon(True)
        self._state = None

    def run(self):
        self._state = "RUN"
        interval = self.interval
        dispatch = self.eventer.send

        dispatch("worker-online")


        # We can't sleep all of the interval, because then
        # it takes 60 seconds (or value of interval) to shutdown
        # the thread.

        last_beat = None
        while 1:
            if self._shutdown.isSet():
                break
            now = time()
            if not last_beat or now > last_beat + interval:
                last_beat = now
                dispatch("worker-heartbeat")
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
        print("SET SHUTDOWN!")
        self._shutdown.set()
        print("WAIT FOR STOPPED")
        self._stopped.wait() # block until this thread is done
        print("STOPPED")
