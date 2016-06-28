from __future__ import absolute_import, unicode_literals

from celery.platforms import IS_WINDOWS
from celery.bin.celeryd_detach import (
    detach,
    detached_celeryd,
    main,
)

from celery.tests.case import AppCase, Mock, mock, patch


if not IS_WINDOWS:
    class test_detached(AppCase):

        @patch('celery.bin.celeryd_detach.detached')
        @patch('os.execv')
        @patch('celery.bin.celeryd_detach.logger')
        @patch('celery.app.log.Logging.setup_logging_subsystem')
        def test_execs(self, setup_logs, logger, execv, detached):
            context = detached.return_value = Mock()
            context.__enter__ = Mock()
            context.__exit__ = Mock()

            detach('/bin/boo', ['a', 'b', 'c'], logfile='/var/log',
                   pidfile='/var/pid', hostname='foo@example.com')
            detached.assert_called_with(
                '/var/log', '/var/pid', None, None, None, None, False,
                after_forkers=False,
            )
            execv.assert_called_with('/bin/boo', ['/bin/boo', 'a', 'b', 'c'])

            r = detach('/bin/boo', ['a', 'b', 'c'],
                       logfile='/var/log', pidfile='/var/pid',
                       executable='/bin/foo', app=self.app)
            execv.assert_called_with('/bin/foo', ['/bin/foo', 'a', 'b', 'c'])

            execv.side_effect = Exception('foo')
            r = detach(
                '/bin/boo', ['a', 'b', 'c'],
                logfile='/var/log', pidfile='/var/pid',
                hostname='foo@example.com', app=self.app)
            context.__enter__.assert_called_with()
            logger.critical.assert_called()
            setup_logs.assert_called_with(
                'ERROR', '/var/log', hostname='foo@example.com')
            self.assertEqual(r, 1)

            self.patch('celery.current_app')
            from celery import current_app
            r = detach(
                '/bin/boo', ['a', 'b', 'c'],
                logfile='/var/log', pidfile='/var/pid',
                hostname='foo@example.com', app=None)
            current_app.log.setup_logging_subsystem.assert_called_with(
                'ERROR', '/var/log', hostname='foo@example.com',
            )


class test_PartialOptionParser(AppCase):

    def test_parser(self):
        x = detached_celeryd(self.app)
        p = x.create_parser('celeryd_detach')
        options, values = p.parse_args([
            '--logfile=foo', '--fake', '--enable',
            'a', 'b', '-c1', '-d', '2',
        ])
        self.assertEqual(options.logfile, 'foo')
        self.assertEqual(values, ['a', 'b'])
        self.assertEqual(p.leftovers, ['--enable', '-c1', '-d', '2'])
        options, values = p.parse_args([
            '--fake', '--enable',
            '--pidfile=/var/pid/foo.pid',
            'a', 'b', '-c1', '-d', '2',
        ])
        self.assertEqual(options.pidfile, '/var/pid/foo.pid')

        with mock.stdouts():
            with self.assertRaises(SystemExit):
                p.parse_args(['--logfile'])
            p.get_option('--logfile').nargs = 2
            with self.assertRaises(SystemExit):
                p.parse_args(['--logfile=a'])
            with self.assertRaises(SystemExit):
                p.parse_args(['--fake=abc'])

        assert p.get_option('--logfile').nargs == 2
        p.parse_args(['--logfile=a', 'b'])
        p.get_option('--logfile').nargs = 1


class test_Command(AppCase):
    argv = ['--foobar=10,2', '-c', '1',
            '--logfile=/var/log', '-lDEBUG',
            '--', '.disable_rate_limits=1']

    def test_parse_options(self):
        x = detached_celeryd(app=self.app)
        o, v, l = x.parse_options('cd', self.argv)
        self.assertEqual(o.logfile, '/var/log')
        self.assertEqual(l, ['--foobar=10,2', '-c', '1',
                             '-lDEBUG', '--logfile=/var/log',
                             '--pidfile=celeryd.pid'])
        x.parse_options('cd', [])  # no args

    @patch('sys.exit')
    @patch('celery.bin.celeryd_detach.detach')
    def test_execute_from_commandline(self, detach, exit):
        x = detached_celeryd(app=self.app)
        x.execute_from_commandline(self.argv)
        exit.assert_called()
        detach.assert_called_with(
            path=x.execv_path, uid=None, gid=None,
            umask=None, fake=False, logfile='/var/log', pidfile='celeryd.pid',
            working_directory=None, executable=None, hostname=None,
            argv=x.execv_argv + [
                '-c', '1', '-lDEBUG',
                '--logfile=/var/log', '--pidfile=celeryd.pid',
                '--', '.disable_rate_limits=1'
            ],
            app=self.app,
        )

    @patch('celery.bin.celeryd_detach.detached_celeryd')
    def test_main(self, command):
        c = command.return_value = Mock()
        main(self.app)
        c.execute_from_commandline.assert_called_with()
