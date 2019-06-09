from __future__ import absolute_import, unicode_literals

import pytest

from case import Mock, mock, patch
from celery.bin.celeryd_detach import detach, detached_celeryd, main
from celery.platforms import IS_WINDOWS

if not IS_WINDOWS:
    class test_detached:

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
            assert r == 1

            self.patching('celery.current_app')
            from celery import current_app
            r = detach(
                '/bin/boo', ['a', 'b', 'c'],
                logfile='/var/log', pidfile='/var/pid',
                hostname='foo@example.com', app=None)
            current_app.log.setup_logging_subsystem.assert_called_with(
                'ERROR', '/var/log', hostname='foo@example.com',
            )


class test_PartialOptionParser:

    def test_parser(self):
        x = detached_celeryd(self.app)
        p = x.create_parser('celeryd_detach')
        options, leftovers = p.parse_known_args([
            '--logfile=foo', '--fake', '--enable',
            'a', 'b', '-c1', '-d', '2',
        ])
        assert options.logfile == 'foo'
        assert leftovers, ['--enable', '-c1', '-d' == '2']
        options, leftovers = p.parse_known_args([
            '--fake', '--enable',
            '--pidfile=/var/pid/foo.pid',
            'a', 'b', '-c1', '-d', '2',
        ])
        assert options.pidfile == '/var/pid/foo.pid'

        with mock.stdouts():
            with pytest.raises(SystemExit):
                p.parse_args(['--logfile'])
            p._option_string_actions['--logfile'].nargs = 2
            with pytest.raises(SystemExit):
                p.parse_args(['--logfile=a'])
            with pytest.raises(SystemExit):
                p.parse_args(['--fake=abc'])

        assert p._option_string_actions['--logfile'].nargs == 2
        p.parse_args(['--logfile', 'a', 'b'])


class test_Command:
    argv = [
        '--foobar=10,2', '-c', '1',
        '--logfile=/var/log', '-lDEBUG',
        '--', '.disable_rate_limits=1',
    ]

    def test_parse_options(self):
        x = detached_celeryd(app=self.app)
        _, argv = x._split_command_line_config(self.argv)
        o, l = x.parse_options('cd', argv)
        assert o.logfile == '/var/log'
        assert l == [
            '--foobar=10,2', '-c', '1',
            '-lDEBUG', '--logfile=/var/log',
            '--pidfile=celeryd.pid',
        ]
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
            workdir=None, executable=None, hostname=None,
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
