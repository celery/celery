from __future__ import absolute_import, unicode_literals

from celery.bin import events

from celery.tests.case import AppCase, patch, _old_patch, skip


class MockCommand(object):
    executed = []

    def execute_from_commandline(self, **kwargs):
        self.executed.append(True)


def proctitle(prog, info=None):
    proctitle.last = (prog, info)
proctitle.last = ()


class test_events(AppCase):

    def setup(self):
        self.ev = events.events(app=self.app)

    @_old_patch('celery.events.dumper', 'evdump',
                lambda **kw: 'me dumper, you?')
    @_old_patch('celery.bin.events', 'set_process_title', proctitle)
    def test_run_dump(self):
        self.assertEqual(self.ev.run(dump=True), 'me dumper, you?')
        self.assertIn('celery events:dump', proctitle.last[0])

    @skip.unless_module('curses', import_errors=(ImportError, OSError))
    def test_run_top(self):
        @_old_patch('celery.events.cursesmon', 'evtop',
                    lambda **kw: 'me top, you?')
        @_old_patch('celery.bin.events', 'set_process_title', proctitle)
        def _inner():
            self.assertEqual(self.ev.run(), 'me top, you?')
            self.assertIn('celery events:top', proctitle.last[0])
        return _inner()

    @_old_patch('celery.events.snapshot', 'evcam',
                lambda *a, **k: (a, k))
    @_old_patch('celery.bin.events', 'set_process_title', proctitle)
    def test_run_cam(self):
        a, kw = self.ev.run(camera='foo.bar.baz', logfile='logfile')
        self.assertEqual(a[0], 'foo.bar.baz')
        self.assertEqual(kw['freq'], 1.0)
        self.assertIsNone(kw['maxrate'])
        self.assertEqual(kw['loglevel'], 'INFO')
        self.assertEqual(kw['logfile'], 'logfile')
        self.assertIn('celery events:cam', proctitle.last[0])

    @patch('celery.events.snapshot.evcam')
    @patch('celery.bin.events.detached')
    def test_run_cam_detached(self, detached, evcam):
        self.ev.prog_name = 'celery events'
        self.ev.run_evcam('myapp.Camera', detach=True)
        detached.assert_called()
        evcam.assert_called()

    def test_get_options(self):
        self.assertFalse(self.ev.get_options())

    @_old_patch('celery.bin.events', 'events', MockCommand)
    def test_main(self):
        MockCommand.executed = []
        events.main()
        self.assertTrue(MockCommand.executed)
