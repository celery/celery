# -*- coding: utf-8 -*-
"""
    celery.events.snapshot
    ~~~~~~~~~~~~~~~~~~~~~~

    Consuming the events as a stream is not always suitable
    so this module implements a system to take snapshots of the
    state of a cluster at regular intervals.  There is a full
    implementation of this writing the snapshots to a database
    in :mod:`djcelery.snapshots` in the `django-celery` distribution.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

import atexit

from kombu.utils.limits import TokenBucket

from .. import platforms
from ..app import app_or_default
from ..utils import timer2, instantiate, LOG_LEVELS
from ..utils.dispatch import Signal
from ..utils.timeutils import rate


class Polaroid(object):
    timer = timer2
    shutter_signal = Signal(providing_args=("state", ))
    cleanup_signal = Signal()
    clear_after = False

    _tref = None
    _ctref = None

    def __init__(self, state, freq=1.0, maxrate=None,
            cleanup_freq=3600.0, logger=None, timer=None, app=None):
        self.app = app_or_default(app)
        self.state = state
        self.freq = freq
        self.cleanup_freq = cleanup_freq
        self.timer = timer or self.timer
        self.logger = logger or \
                self.app.log.get_default_logger(name="celery.cam")
        self.maxrate = maxrate and TokenBucket(rate(maxrate))

    def install(self):
        self._tref = self.timer.apply_interval(self.freq * 1000.0,
                                               self.capture)
        self._ctref = self.timer.apply_interval(self.cleanup_freq * 1000.0,
                                                self.cleanup)

    def on_shutter(self, state):
        pass

    def on_cleanup(self):
        pass

    def cleanup(self):
        self.logger.debug("Cleanup: Running...")
        self.cleanup_signal.send(None)
        self.on_cleanup()

    def shutter(self):
        if self.maxrate is None or self.maxrate.can_consume():
            self.logger.debug("Shutter: %s", self.state)
            self.shutter_signal.send(self.state)
            self.on_shutter(self.state)

    def capture(self):
        self.state.freeze_while(self.shutter, clear_after=self.clear_after)

    def cancel(self):
        if self._tref:
            self._tref()  # flush all received events.
            self._tref.cancel()
        if self._ctref:
            self._ctref.cancel()

    def __enter__(self):
        self.install()
        return self

    def __exit__(self, *exc_info):
        self.cancel()


def evcam(camera, freq=1.0, maxrate=None, loglevel=0,
        logfile=None, pidfile=None, timer=None, app=None):
    app = app_or_default(app)

    if pidfile:
        pidlock = platforms.create_pidlock(pidfile).acquire()
        atexit.register(pidlock.release)

    if not isinstance(loglevel, int):
        loglevel = LOG_LEVELS[loglevel.upper()]
    logger = app.log.setup_logger(loglevel=loglevel,
                                  logfile=logfile,
                                  name="celery.evcam")

    logger.info(
        "-> evcam: Taking snapshots with %s (every %s secs.)\n" % (
            camera, freq))
    state = app.events.State()
    cam = instantiate(camera, state, app=app,
                      freq=freq, maxrate=maxrate, logger=logger,
                      timer=timer)
    cam.install()
    conn = app.broker_connection()
    recv = app.events.Receiver(conn, handlers={"*": state.event})
    try:
        try:
            recv.capture(limit=None)
        except KeyboardInterrupt:
            raise SystemExit
    finally:
        cam.cancel()
        conn.close()
