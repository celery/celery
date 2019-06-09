from __future__ import absolute_import, unicode_literals

import logging
import sys

import pytest

from case import Mock, mock, patch
from celery import beat, platforms
from celery.apps import beat as beatapp
from celery.bin import beat as beat_bin


def MockBeat(*args, **kwargs):
    class _Beat(beatapp.Beat):
        Service = Mock(
            name='MockBeat.Service',
            return_value=Mock(name='MockBeat()', max_interval=3.3),
        )
    b = _Beat(*args, **kwargs)
    sched = b.Service.return_value.get_scheduler = Mock()
    sched.return_value.max_interval = 3.3
    return b


class test_Beat:

    def test_loglevel_string(self):
        b = beatapp.Beat(app=self.app, loglevel='DEBUG',
                         redirect_stdouts=False)
        assert b.loglevel == logging.DEBUG

        b2 = beatapp.Beat(app=self.app, loglevel=logging.DEBUG,
                          redirect_stdouts=False)
        assert b2.loglevel == logging.DEBUG

    def test_colorize(self):
        self.app.log.setup = Mock()
        b = beatapp.Beat(app=self.app, no_color=True,
                         redirect_stdouts=False)
        b.setup_logging()
        self.app.log.setup.assert_called()
        assert not self.app.log.setup.call_args[1]['colorize']

    def test_init_loader(self):
        b = beatapp.Beat(app=self.app, redirect_stdouts=False)
        b.init_loader()

    def test_process_title(self):
        b = beatapp.Beat(app=self.app, redirect_stdouts=False)
        b.set_process_title()

    def test_run(self):
        b = MockBeat(app=self.app, redirect_stdouts=False)
        b.install_sync_handler = Mock(name='beat.install_sync_handler')
        b.Service.return_value.max_interval = 3.0
        b.run()
        b.Service().start.assert_called_with()

    def psig(self, fun, *args, **kwargs):
        handlers = {}

        class Signals(platforms.Signals):

            def __setitem__(self, sig, handler):
                handlers[sig] = handler

        p, platforms.signals = platforms.signals, Signals()
        try:
            fun(*args, **kwargs)
            return handlers
        finally:
            platforms.signals = p

    def test_install_sync_handler(self):
        b = beatapp.Beat(app=self.app, redirect_stdouts=False)
        clock = beat.Service(app=self.app)
        clock.start = Mock(name='beat.Service().start')
        clock.sync = Mock(name='beat.Service().sync')
        handlers = self.psig(b.install_sync_handler, clock)
        with pytest.raises(SystemExit):
            handlers['SIGINT']('SIGINT', object())
        clock.sync.assert_called_with()

    @mock.restore_logging()
    def test_setup_logging(self):
        try:
            # py3k
            delattr(sys.stdout, 'logger')
        except AttributeError:
            pass
        b = beatapp.Beat(app=self.app, redirect_stdouts=False)
        b.redirect_stdouts = False
        b.app.log.already_setup = False
        b.setup_logging()
        with pytest.raises(AttributeError):
            sys.stdout.logger

    import sys
    orig_stdout = sys.__stdout__

    @patch('celery.apps.beat.logger')
    def test_logs_errors(self, logger):
        b = MockBeat(
            app=self.app, redirect_stdouts=False, socket_timeout=None,
        )
        b.install_sync_handler = Mock('beat.install_sync_handler')
        b.install_sync_handler.side_effect = RuntimeError('xxx')
        with mock.restore_logging():
            with pytest.raises(RuntimeError):
                b.start_scheduler()
        logger.critical.assert_called()

    @patch('celery.platforms.create_pidlock')
    def test_using_pidfile(self, create_pidlock):
        b = MockBeat(app=self.app, pidfile='pidfilelockfilepid',
                     socket_timeout=None, redirect_stdouts=False)
        b.install_sync_handler = Mock(name='beat.install_sync_handler')
        with mock.stdouts():
            b.start_scheduler()
        create_pidlock.assert_called()


class test_div:

    def setup(self):
        self.Beat = self.app.Beat = self.patching('celery.apps.beat.Beat')
        self.detached = self.patching('celery.bin.beat.detached')
        self.Beat.__name__ = 'Beat'

    def test_main(self):
        sys.argv = [sys.argv[0], '-s', 'foo']
        beat_bin.main(app=self.app)
        self.Beat().run.assert_called_with()

    def test_detach(self):
        cmd = beat_bin.beat()
        cmd.app = self.app
        cmd.run(detach=True)
        self.detached.assert_called()

    def test_parse_options(self):
        cmd = beat_bin.beat()
        cmd.app = self.app
        options, args = cmd.parse_options('celery beat', ['-s', 'foo'])
        assert options['schedule'] == 'foo'
