from __future__ import absolute_import, unicode_literals

import importlib
from functools import wraps

from case import patch, skip
from celery.bin import events


def _old_patch(module, name, mocked):
    module = importlib.import_module(module)

    def _patch(fun):

        @wraps(fun)
        def __patched(*args, **kwargs):
            prev = getattr(module, name)
            setattr(module, name, mocked)
            try:
                return fun(*args, **kwargs)
            finally:
                setattr(module, name, prev)
        return __patched
    return _patch


class MockCommand(object):
    executed = []

    def execute_from_commandline(self, **kwargs):
        self.executed.append(True)


def proctitle(prog, info=None):
    proctitle.last = (prog, info)


proctitle.last = ()  # noqa: E305


class test_events:

    def setup(self):
        self.ev = events.events(app=self.app)

    @_old_patch('celery.events.dumper', 'evdump',
                lambda **kw: 'me dumper, you?')
    @_old_patch('celery.bin.events', 'set_process_title', proctitle)
    def test_run_dump(self):
        assert self.ev.run(dump=True), 'me dumper == you?'
        assert 'celery events:dump' in proctitle.last[0]

    @skip.unless_module('curses', import_errors=(ImportError, OSError))
    def test_run_top(self):
        @_old_patch('celery.events.cursesmon', 'evtop',
                    lambda **kw: 'me top, you?')
        @_old_patch('celery.bin.events', 'set_process_title', proctitle)
        def _inner():
            assert self.ev.run(), 'me top == you?'
            assert 'celery events:top' in proctitle.last[0]
        return _inner()

    @_old_patch('celery.events.snapshot', 'evcam',
                lambda *a, **k: (a, k))
    @_old_patch('celery.bin.events', 'set_process_title', proctitle)
    def test_run_cam(self):
        a, kw = self.ev.run(camera='foo.bar.baz', logfile='logfile')
        assert a[0] == 'foo.bar.baz'
        assert kw['freq'] == 1.0
        assert kw['maxrate'] is None
        assert kw['loglevel'] == 'INFO'
        assert kw['logfile'] == 'logfile'
        assert 'celery events:cam' in proctitle.last[0]

    @patch('celery.events.snapshot.evcam')
    @patch('celery.bin.events.detached')
    def test_run_cam_detached(self, detached, evcam):
        self.ev.prog_name = 'celery events'
        self.ev.run_evcam('myapp.Camera', detach=True)
        detached.assert_called()
        evcam.assert_called()

    def test_get_options(self):
        assert not self.ev.get_options()

    @_old_patch('celery.bin.events', 'events', MockCommand)
    def test_main(self):
        MockCommand.executed = []
        events.main()
        assert MockCommand.executed
