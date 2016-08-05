from __future__ import absolute_import, unicode_literals

import signal
import sys

from celery.bin.multi import (
    main,
    MultiTool,
    __doc__ as doc,
)
from celery.five import WhateverIO

from celery.tests.case import AppCase, Mock, patch


class test_MultiTool(AppCase):

    def setup(self):
        self.fh = WhateverIO()
        self.env = {}
        self.t = MultiTool(env=self.env, fh=self.fh)
        self.t.cluster_from_argv = Mock(name='cluster_from_argv')
        self.t._cluster_from_argv = Mock(name='cluster_from_argv')
        self.t.Cluster = Mock(name='Cluster')
        self.t.carp = Mock(name='.carp')
        self.t.usage = Mock(name='.usage')
        self.t.splash = Mock(name='.splash')
        self.t.say = Mock(name='.say')
        self.t.ok = Mock(name='.ok')
        self.cluster = self.t.Cluster.return_value

        def _cluster_from_argv(argv):
            p = self.t.OptionParser(argv)
            p.parse()
            return p, self.cluster
        self.t.cluster_from_argv.return_value = self.cluster
        self.t._cluster_from_argv.side_effect = _cluster_from_argv

    def test_findsig(self):
        self.assert_sig_argument(['a', 'b', 'c', '-1'], 1)
        self.assert_sig_argument(['--foo=1', '-9'], 9)
        self.assert_sig_argument(['-INT'], signal.SIGINT)
        self.assert_sig_argument([], signal.SIGTERM)
        self.assert_sig_argument(['-s'], signal.SIGTERM)
        self.assert_sig_argument(['-log'], signal.SIGTERM)

    def assert_sig_argument(self, args, expected):
        p = self.t.OptionParser(args)
        p.parse()
        self.assertEqual(self.t._find_sig_argument(p), expected)

    def test_execute_from_commandline(self):
        self.t.call_command = Mock(name='call_command')
        self.t.execute_from_commandline(
            'multi start --verbose 10 --foo'.split(),
            cmd='X',
        )
        self.assertEqual(self.t.cmd, 'X')
        self.assertEqual(self.t.prog_name, 'multi')
        self.t.call_command.assert_called_with('start', ['10', '--foo'])

    def test_execute_from_commandline__arguments(self):
        self.assertTrue(self.t.execute_from_commandline('multi'.split()))
        self.assertTrue(self.t.execute_from_commandline('multi -bar'.split()))

    def test_call_command(self):
        cmd = self.t.commands['foo'] = Mock(name='foo')
        self.t.retcode = 303
        self.assertIs(
            self.t.call_command('foo', ['1', '2', '--foo=3']),
            cmd.return_value,
        )
        cmd.assert_called_with('1', '2', '--foo=3')

    def test_call_command__error(self):
        self.assertEqual(
            self.t.call_command('asdqwewqe', ['1', '2']),
            1,
        )
        self.t.carp.assert_called()

    def test_handle_reserved_options(self):
        self.assertListEqual(
            self.t._handle_reserved_options(
                ['a', '-q', 'b', '--no-color', 'c']),
            ['a', 'b', 'c'],
        )

    def test_start(self):
        self.cluster.start.return_value = [0, 0, 1, 0]
        self.assertTrue(self.t.start('10', '-A', 'proj'))
        self.t.splash.assert_called_with()
        self.t.cluster_from_argv.assert_called_with(('10', '-A', 'proj'))
        self.cluster.start.assert_called_with()

    def test_start__exitcodes(self):
        self.cluster.start.return_value = [0, 0, 0]
        self.assertFalse(self.t.start('foo', 'bar', 'baz'))
        self.cluster.start.assert_called_with()

        self.cluster.start.return_value = [0, 1, 0]
        self.assertTrue(self.t.start('foo', 'bar', 'baz'))

    def test_stop(self):
        self.t.stop('10', '-A', 'proj', retry=3)
        self.t.splash.assert_called_with()
        self.t._cluster_from_argv.assert_called_with(('10', '-A', 'proj'))
        self.cluster.stop.assert_called_with(retry=3, sig=signal.SIGTERM)

    def test_stopwait(self):
        self.t.stopwait('10', '-A', 'proj', retry=3)
        self.t.splash.assert_called_with()
        self.t._cluster_from_argv.assert_called_with(('10', '-A', 'proj'))
        self.cluster.stopwait.assert_called_with(retry=3, sig=signal.SIGTERM)

    def test_restart(self):
        self.cluster.restart.return_value = [0, 0, 1, 0]
        self.t.restart('10', '-A', 'proj')
        self.t.splash.assert_called_with()
        self.t._cluster_from_argv.assert_called_with(('10', '-A', 'proj'))
        self.cluster.restart.assert_called_with(sig=signal.SIGTERM)

    def test_names(self):
        self.t.cluster_from_argv.return_value = [Mock(), Mock()]
        self.t.cluster_from_argv.return_value[0].name = 'x'
        self.t.cluster_from_argv.return_value[1].name = 'y'
        self.t.names('10', '-A', 'proj')
        self.t.say.assert_called()

    def test_get(self):
        node = self.cluster.find.return_value = Mock(name='node')
        node.argv = ['A', 'B', 'C']
        self.assertIs(
            self.t.get('wanted', '10', '-A', 'proj'),
            self.t.ok.return_value,
        )
        self.cluster.find.assert_called_with('wanted')
        self.t.cluster_from_argv.assert_called_with(('10', '-A', 'proj'))
        self.t.ok.assert_called_with(' '.join(node.argv))

    def test_get__KeyError(self):
        self.cluster.find.side_effect = KeyError()
        self.assertTrue(self.t.get('wanted', '10', '-A', 'proj'))

    def test_show(self):
        nodes = self.t.cluster_from_argv.return_value = [
            Mock(name='n1'),
            Mock(name='n2'),
        ]
        nodes[0].argv_with_executable = ['python', 'foo', 'bar']
        nodes[1].argv_with_executable = ['python', 'xuzzy', 'baz']

        self.assertIs(
            self.t.show('10', '-A', 'proj'),
            self.t.ok.return_value,
        )
        self.t.ok.assert_called_with(
            '\n'.join(' '.join(node.argv_with_executable) for node in nodes))

    def test_kill(self):
        self.t.kill('10', '-A', 'proj')
        self.t.splash.assert_called_with()
        self.t.cluster_from_argv.assert_called_with(('10', '-A', 'proj'))
        self.cluster.kill.assert_called_with()

    def test_expand(self):
        node1 = Mock(name='n1')
        node2 = Mock(name='n2')
        node1.expander.return_value = 'A'
        node2.expander.return_value = 'B'
        nodes = self.t.cluster_from_argv.return_value = [node1, node2]
        self.assertIs(self.t.expand('%p', '10'), self.t.ok.return_value)
        self.t.cluster_from_argv.assert_called_with(('10',))
        for node in nodes:
            node.expander.assert_called_with('%p')
        self.t.ok.assert_called_with('A\nB')

    def test_note(self):
        self.t.quiet = True
        self.t.note('foo')
        self.t.say.assert_not_called()
        self.t.quiet = False
        self.t.note('foo')
        self.t.say.assert_called_with('foo', newline=True)

    def test_splash(self):
        x = MultiTool()
        x.note = Mock()
        x.nosplash = True
        x.splash()
        x.note.assert_not_called()
        x.nosplash = False
        x.splash()
        x.note.assert_called()

    def test_Cluster(self):
        m = MultiTool()
        c = m.cluster_from_argv(['A', 'B', 'C'])
        self.assertIs(c.env, m.env)
        self.assertEqual(c.cmd, 'celery worker')
        self.assertEqual(c.on_stopping_preamble, m.on_stopping_preamble)
        self.assertEqual(c.on_send_signal, m.on_send_signal)
        self.assertEqual(c.on_still_waiting_for, m.on_still_waiting_for)
        self.assertEqual(
            c.on_still_waiting_progress,
            m.on_still_waiting_progress,
        )
        self.assertEqual(c.on_still_waiting_end, m.on_still_waiting_end)
        self.assertEqual(c.on_node_start, m.on_node_start)
        self.assertEqual(c.on_node_restart, m.on_node_restart)
        self.assertEqual(c.on_node_shutdown_ok, m.on_node_shutdown_ok)
        self.assertEqual(c.on_node_status, m.on_node_status)
        self.assertEqual(c.on_node_signal_dead, m.on_node_signal_dead)
        self.assertEqual(c.on_node_signal, m.on_node_signal)
        self.assertEqual(c.on_node_down, m.on_node_down)
        self.assertEqual(c.on_child_spawn, m.on_child_spawn)
        self.assertEqual(c.on_child_signalled, m.on_child_signalled)
        self.assertEqual(c.on_child_failure, m.on_child_failure)

    def test_on_stopping_preamble(self):
        self.t.on_stopping_preamble([])

    def test_on_send_signal(self):
        self.t.on_send_signal(Mock(), Mock())

    def test_on_still_waiting_for(self):
        self.t.on_still_waiting_for([Mock(), Mock()])

    def test_on_still_waiting_for__empty(self):
        self.t.on_still_waiting_for([])

    def test_on_still_waiting_progress(self):
        self.t.on_still_waiting_progress([])

    def test_on_still_waiting_end(self):
        self.t.on_still_waiting_end()

    def test_on_node_signal_dead(self):
        self.t.on_node_signal_dead(Mock())

    def test_on_node_start(self):
        self.t.on_node_start(Mock())

    def test_on_node_restart(self):
        self.t.on_node_restart(Mock())

    def test_on_node_down(self):
        self.t.on_node_down(Mock())

    def test_on_node_shutdown_ok(self):
        self.t.on_node_shutdown_ok(Mock())

    def test_on_node_status__FAIL(self):
        self.t.on_node_status(Mock(), 1)
        self.t.say.assert_called_with(self.t.FAILED, newline=True)

    def test_on_node_status__OK(self):
        self.t.on_node_status(Mock(), 0)
        self.t.say.assert_called_with(self.t.OK, newline=True)

    def test_on_node_signal(self):
        self.t.on_node_signal(Mock(), Mock())

    def test_on_child_spawn(self):
        self.t.on_child_spawn(Mock(), Mock(), Mock())

    def test_on_child_signalled(self):
        self.t.on_child_signalled(Mock(), Mock())

    def test_on_child_failure(self):
        self.t.on_child_failure(Mock(), Mock())

    def test_constant_strings(self):
        self.assertTrue(self.t.OK)
        self.assertTrue(self.t.DOWN)
        self.assertTrue(self.t.FAILED)


class test_MultiTool_functional(AppCase):

    def setup(self):
        self.fh = WhateverIO()
        self.env = {}
        self.t = MultiTool(env=self.env, fh=self.fh)

    def test_note(self):
        self.t.note('hello world')
        self.assertEqual(self.fh.getvalue(), 'hello world\n')

    def test_note_quiet(self):
        self.t.quiet = True
        self.t.note('hello world')
        self.assertFalse(self.fh.getvalue())

    def test_carp(self):
        self.t.say = Mock()
        self.t.carp('foo')
        self.t.say.assert_called_with('foo', True, self.t.stderr)

    def test_info(self):
        self.t.verbose = True
        self.t.info('hello info')
        self.assertEqual(self.fh.getvalue(), 'hello info\n')

    def test_info_not_verbose(self):
        self.t.verbose = False
        self.t.info('hello info')
        self.assertFalse(self.fh.getvalue())

    def test_error(self):
        self.t.carp = Mock()
        self.t.usage = Mock()
        self.assertEqual(self.t.error('foo'), 1)
        self.t.carp.assert_called_with('foo')
        self.t.usage.assert_called_with()

        self.t.carp = Mock()
        self.assertEqual(self.t.error(), 1)
        self.t.carp.assert_not_called()

    def test_nosplash(self):
        self.t.nosplash = True
        self.t.splash()
        self.assertFalse(self.fh.getvalue())

    def test_splash(self):
        self.t.nosplash = False
        self.t.splash()
        self.assertIn('celery multi', self.fh.getvalue())

    def test_usage(self):
        self.t.usage()
        self.assertTrue(self.fh.getvalue())

    def test_help(self):
        self.t.help([])
        self.assertIn(doc, self.fh.getvalue())

    def test_expand(self):
        self.t.expand('foo%n', 'ask', 'klask', 'dask')
        self.assertEqual(
            self.fh.getvalue(), 'fooask\nfooklask\nfoodask\n',
        )

    @patch('celery.apps.multi.gethostname')
    def test_get(self, gethostname):
        gethostname.return_value = 'e.com'
        self.t.get('xuzzy@e.com', 'foo', 'bar', 'baz')
        self.assertFalse(self.fh.getvalue())
        self.t.get('foo@e.com', 'foo', 'bar', 'baz')
        self.assertTrue(self.fh.getvalue())

    @patch('celery.apps.multi.gethostname')
    def test_names(self, gethostname):
        gethostname.return_value = 'e.com'
        self.t.names('foo', 'bar', 'baz')
        self.assertIn('foo@e.com\nbar@e.com\nbaz@e.com', self.fh.getvalue())

    def test_execute_from_commandline(self):
        start = self.t.commands['start'] = Mock()
        self.t.error = Mock()
        self.t.execute_from_commandline(['multi', 'start', 'foo', 'bar'])
        self.t.error.assert_not_called()
        start.assert_called_with('foo', 'bar')

        self.t.error = Mock()
        self.t.execute_from_commandline(['multi', 'frob', 'foo', 'bar'])
        self.t.error.assert_called_with('Invalid command: frob')

        self.t.error = Mock()
        self.t.execute_from_commandline(['multi'])
        self.t.error.assert_called_with()

        self.t.error = Mock()
        self.t.execute_from_commandline(['multi', '-foo'])
        self.t.error.assert_called_with()

        self.t.execute_from_commandline(
            ['multi', 'start', 'foo',
             '--nosplash', '--quiet', '-q', '--verbose', '--no-color'],
        )
        self.assertTrue(self.t.nosplash)
        self.assertTrue(self.t.quiet)
        self.assertTrue(self.t.verbose)
        self.assertTrue(self.t.no_color)

    @patch('celery.bin.multi.MultiTool')
    def test_main(self, MultiTool):
        m = MultiTool.return_value = Mock()
        with self.assertRaises(SystemExit):
            main()
        m.execute_from_commandline.assert_called_with(sys.argv)
