from celery.utils import timer2

from celery import log
from celery.datastructures import TokenBucket
from celery.events import EventReceiver
from celery.events.state import State
from celery.messaging import establish_connection
from celery.utils import instantiate, LOG_LEVELS
from celery.utils.dispatch import Signal
from celery.utils.timeutils import rate


class Polaroid(object):
    shutter_signal = Signal(providing_args=("state", ))
    cleanup_signal = Signal()

    _tref = None

    def __init__(self, state, freq=1.0, maxrate=None,
            cleanup_freq=3600.0, logger=None):
        self.state = state
        self.freq = freq
        self.cleanup_freq = cleanup_freq
        self.logger = logger or log.get_default_logger(name="celery.cam")
        self.maxrate = maxrate and TokenBucket(rate(maxrate))

    def install(self):
        self._tref = timer2.apply_interval(self.freq * 1000.0,
                                           self.capture)
        self._ctref = timer2.apply_interval(self.cleanup_freq * 1000.0,
                                            self.cleanup)

    def on_shutter(self, state):
        pass

    def on_cleanup(self):
        pass

    def cleanup(self):
        self.debug("Cleanup: Running...")
        self.cleanup_signal.send(None)
        self.on_cleanup()

    def debug(self, msg):
        if self.logger:
            self.logger.debug(msg)

    def shutter(self):
        if self.maxrate is None or self.maxrate.can_consume():
            self.debug("Shutter: %s" % (self.state, ))
            self.shutter_signal.send(self.state)
            self.on_shutter(self.state)
            self.state.clear()

    def capture(self):
        return self.state.freeze_while(self.shutter)

    def cancel(self):
        if self._tref:
            self._tref()
            self._tref.cancel()
        if self._ctref:
            self._ctref.cancel()

    def __enter__(self):
        self.install()
        return self

    def __exit__(self, *exc_info):
        self.cancel()


def evcam(camera, freq=1.0, maxrate=None, loglevel=0,
        logfile=None):
    if not isinstance(loglevel, int):
        loglevel = LOG_LEVELS[loglevel.upper()]
    logger = log.setup_logger(loglevel=loglevel,
                              logfile=logfile,
                              name="celery.evcam")
    logger.info(
        "-> evcam: Taking snapshots with %s (every %s secs.)\n" % (
            camera, freq))
    state = State()
    cam = instantiate(camera, state,
                      freq=freq, maxrate=maxrate, logger=logger)
    cam.install()
    conn = establish_connection()
    recv = EventReceiver(conn, handlers={"*": state.event})
    try:
        try:
            recv.capture(limit=None)
        except KeyboardInterrupt:
            raise SystemExit
    finally:
        cam.cancel()
        conn.close()
