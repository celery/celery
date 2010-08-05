import sys
import time
import timer2

from celery.utils.dispatch import Signal


class Polaroid(object):
    shutter_signal = Signal(providing_args=("state", ))

    def __init__(self, state, freq=1.0, verbose=False):
        self.state = state
        self.freq = freq
        self.verbose = verbose

    def install(self):
        timer2.apply_interval(self.freq * 1000.0, self.capture)

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
