import socket
import threading
from time import sleep


class Heart(threading.Thread):
    interval = 60

    def __init__(self, eventer, hostname=None, interval=None):
        super(Heart, self).__init__()
        self.eventer = eventer
        self.interval = interval or self.interval
        self.hostname = hostname or socket.gethostname()
        self._shutdown = threading.Event()
        self._stopped = threading.Event()
        self.setDaemon(True)

    def run(self):
        hostname = self.hostname
        interval = self.interval
        send = self.eventer.send

        send("worker-online", hostname=hostname)

        while 1:
            if self._shutdown.isSet():
                break
            send("worker-heartbeat", hostname=hostname)
            sleep(interval)
        self._stopped.set()

        send("worker-offline", hostname=hostname)

    def stop(self):
        """Gracefully shutdown the thread."""
        self._shutdown.set()
        self._stopped.wait() # block until this thread is done
