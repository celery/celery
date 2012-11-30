from __future__ import absolute_import

from mock import Mock, patch

from celery import current_app
from celery.bin.celeryd_detach import (
    detach,
    detached_celeryd,
    main,
)

from celery.tests.utils import Case, override_stdouts


if not current_app.IS_WINDOWS:
    class test_detached(Case):

        @patch('celery.bin.celeryd_detach.detached')
        @patch('os.execv')
        @patch('celery.bin.celeryd_detach.logger')
        @patch('celery.app.log.Logging.setup_logging_subsystem')
        def test_execs(self, setup_logs, logger, execv, detached):
            context = detached.return_value = Mock()
            context.__enter__ = Mock()
            context.__exit__ = Mock()

            detach('/bin/boo', ['a', 'b', 'c'], logfile='/var/log',
                    pidfile='/var/pid')
            detached.assert_called_with('/var/log', '/var/pid', None, None, 0,
                                        None, False)
            execv.assert_called_with('/bin/boo', ['/bin/boo', 'a', 'b', 'c'])

            execv.side_effect = Exception('foo')
            r = detach('/bin/boo', ['a', 'b', 'c'], logfile='/var/log',
                    pidfile='/var/pid')
            context.__enter__.assert_called_with()
            self.assertTrue(logger.critical.called)
            setup_logs.assert_called_with('ERROR', '/var/log')
            self.assertEqual(r, 1)


class test_PartialOptionParser(Case):

    def test_parser(self):
        x = detached_celeryd()
        p = x.Parser('celeryd_detach')
        options, values = p.parse_args(['--logfile=foo', '--fake', '--enable',
                                        'a', 'b', '-c1', '-d', '2'])
        self.assertEqual(options.logfile, 'foo')
        self.assertEqual(values, ['a', 'b'])
        self.assertEqual(p.leftovers, ['--enable', '-c1', '-d', '2'])

        with override_stdouts():
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


class test_Command(Case):
    argv = ['--autoscale=10,2', '-c', '1',
            '--logfile=/var/log', '-lDEBUG',
            '--', '.disable_rate_limits=1']

    def test_parse_options(self):
        x = detached_celeryd()
        o, v, l = x.parse_options('cd', self.argv)
        self.assertEqual(o.logfile, '/var/log')
        self.assertEqual(l, ['--autoscale=10,2', '-c', '1',
                             '-lDEBUG', '--logfile=/var/log',
                             '--pidfile=celeryd.pid'])
        x.parse_options('cd', [])  # no args

    @patch('sys.exit')
    @patch('celery.bin.celeryd_detach.detach')
    def test_execute_from_commandline(self, detach, exit):
        x = detached_celeryd()
        x.execute_from_commandline(self.argv)
        self.assertTrue(exit.called)
        detach.assert_called_with(path=x.execv_path, uid=None, gid=None,
            umask=0, fake=False, logfile='/var/log', pidfile='celeryd.pid',
            argv=['-m', 'celery', 'worker', '-c', '1', '-lDEBUG',
                  '--logfile=/var/log', '--pidfile=celeryd.pid',
                  '--', '.disable_rate_limits=1'],
        )

    @patch('celery.bin.celeryd_detach.detached_celeryd')
    def test_main(self, command):
        c = command.return_value = Mock()
        main()
        c.execute_from_commandline.assert_called_with()
