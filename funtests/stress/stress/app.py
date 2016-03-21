# -*- coding: utf-8 -*-
from __future__ import absolute_import, print_function, unicode_literals

import celery
import os
import sys
import signal

from time import sleep

from celery import signals
from celery.bin.base import Option
from celery.exceptions import SoftTimeLimitExceeded
from celery.utils.log import get_task_logger

from .templates import use_template, template_names

logger = get_task_logger(__name__)

IS_CELERY_4 = celery.VERSION[0] >= 4


class App(celery.Celery):
    template_selected = False

    def __init__(self, *args, **kwargs):
        self.template = kwargs.pop('template', None)
        super(App, self).__init__(*args, **kwargs)
        self.user_options['preload'].add(
            Option(
                '-Z', '--template', default='default',
                help='Configuration template to use: {0}'.format(
                    template_names(),
                ),
            )
        )
        signals.user_preload_options.connect(self.on_preload_parsed)
        if IS_CELERY_4:
            self.on_configure.connect(self._maybe_use_default_template)

    def on_preload_parsed(self, options=None, **kwargs):
        self.use_template(options['template'])

    def use_template(self, name='default'):
        if self.template_selected:
            raise RuntimeError('App already configured')
        use_template(self, name)
        self.template_selected = True

    def _maybe_use_default_template(self, **kwargs):
        if not self.template_selected:
            self.use_template('default')

    if not IS_CELERY_4:
        after_configure = None

        def _get_config(self):
            ret = super(App, self)._get_config()
            if self.after_configure:
                self.after_configure(ret)
            return ret

        def on_configure(self):
            self._maybe_use_default_template()

app = App('stress', set_as_current=False)


@app.task
def _marker(s, sep='-'):
    print('{0} {1} {2}'.format(sep * 3, s, sep * 3))


@app.task
def add(x, y):
    return x + y


@app.task(bind=True)
def ids(self, i):
    return (self.request.root_id, self.request.parent_id, i)


@app.task(bind=True)
def collect_ids(self, ids, i):
    return ids, (self.request.root_id, self.request.parent_id, i)


@app.task
def xsum(x):
    return sum(x)


@app.task
def any_(*args, **kwargs):
    wait = kwargs.get('sleep')
    if wait:
        sleep(wait)


@app.task
def any_returning(*args, **kwargs):
    any_(*args, **kwargs)
    return args, kwargs


@app.task
def exiting(status=0):
    sys.exit(status)


@app.task
def kill(sig=getattr(signal, 'SIGKILL', None) or signal.SIGTERM):
    os.kill(os.getpid(), sig)


@app.task
def sleeping(i, **_):
    sleep(i)


@app.task
def sleeping_ignore_limits(i):
    try:
        sleep(i)
    except SoftTimeLimitExceeded:
        sleep(i)


@app.task(bind=True)
def retries(self):
    if not self.request.retries:
        raise self.retry(countdown=1)
    return 10


@app.task
def print_unicode():
    logger.warning('håå®ƒ valmuefrø')
    print('hiöäüß')


@app.task
def segfault():
    import ctypes
    ctypes.memset(0, 0, 1)
    assert False, 'should not get here'


@app.task(bind=True)
def chord_adds(self, x):
    self.add_to_chord(add.s(x, x))
    return 42


@app.task(bind=True)
def chord_replace(self, x):
    return self.replace_in_chord(add.s(x, x))


@app.task
def raising(exc=KeyError()):
    raise exc


@app.task
def logs(msg, p=False):
    print(msg) if p else logger.info(msg)


def marker(s, sep='-'):
    print('{0}{1}'.format(sep, s))
    while True:
        try:
            return _marker.delay(s, sep)
        except Exception as exc:
            print('Retrying marker.delay(). It failed to start: %s' % exc)
