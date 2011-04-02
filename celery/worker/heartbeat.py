from celery.worker.state import SOFTWARE_INFO


class Heart(object):
    """Timer sending heartbeats at regular intervals.

    :param timer: Timer instance.
    :param eventer: Event dispatcher used to send the event.
    :keyword interval: Time in seconds between heartbeats.
                       Default is 2 minutes.

    """

    #: Beats per minute.
    bpm = 0.5

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
            self.tref.cancel()
            self.tref = None
        self._send("worker-offline")
