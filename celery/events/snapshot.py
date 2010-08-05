import sys
import time
import timer2

from celery.events import EventReceiver
from celery.events.state import State
from celery.messaging import establish_connection
from celery.utils import instantiate
from celery.utils.dispatch import Signal


class Polaroid(object):
    _tref = None
    shutter_signal = Signal(providing_args=("state", ))

    def __init__(self, state, freq=1.0, verbose=False):
        self.state = state
        self.freq = freq
        self.verbose = verbose

    def install(self):
        self._tref = timer2.apply_interval(self.freq * 1000.0, self.capture)

    def on_shutter(self, state):
        pass

    def shutter(self):
        if self.verbose:
            sys.stderr.write("[%s] Shutter: %s\n" % (
                time.asctime(), self.state))
        self.shutter_signal.send(self.state)
        self.on_shutter(self.state)
        self.state.clear()

    def capture(self):
        return self.state.freeze_while(self.shutter)

    def cancel(self):
        if self._tref:
            self._tref()
            self._tref.cancel()

    def __enter__(self):
        self.install()
        return self

    def __exit__(self, *exc_info):
        self.cancel()


def evcam(camera, freq, verbose=False):
    sys.stderr.write(
        "-> evcam: Taking snapshots with %s (every %s secs.)\n" % (
            camera, freq))
    state = State()
    cam = instantiate(camera, state, freq=freq, verbose=verbose)
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
