from __future__ import absolute_import, unicode_literals

import errno
import signal

from celery.apps.multi import (
    Cluster, MultiParser, NamespacedOptionParser, Node,
)

from celery.tests.case import AppCase, Mock, call, patch


class test_functions(AppCase):

    def test_findsig(self):
        m = Cluster([])
        self.assertEqual(m._find_sig_argument(['a', 'b', 'c', '-1']), 1)
        self.assertEqual(m._find_sig_argument(['--foo=1', '-9']), 9)
        self.assertEqual(m._find_sig_argument(['-INT']), signal.SIGINT)
        self.assertEqual(m._find_sig_argument([]), signal.SIGTERM)
        self.assertEqual(m._find_sig_argument(['-s']), signal.SIGTERM)
        self.assertEqual(m._find_sig_argument(['-log']), signal.SIGTERM)

    def test_parse_ns_range(self):
        m = MultiParser()
        self.assertEqual(m._parse_ns_range('1-3', True), ['1', '2', '3'])
        self.assertEqual(m._parse_ns_range('1-3', False), ['1-3'])
        self.assertEqual(m._parse_ns_range(
            '1-3,10,11,20', True),
            ['1', '2', '3', '10', '11', '20'],
        )

    def test_format_opt(self):
        m = MultiParser()
        self.assertEqual(m.format_opt('--foo', None), '--foo')
        self.assertEqual(m.format_opt('-c', 1), '-c 1')
        self.assertEqual(m.format_opt('--log', 'foo'), '--log=foo')


class test_NamespacedOptionParser(AppCase):

    def test_parse(self):
        x = NamespacedOptionParser(['-c:1,3', '4'])
        self.assertEqual(x.namespaces.get('1,3'), {'-c': '4'})
        x = NamespacedOptionParser(['-c:jerry,elaine', '5',
                                    '--loglevel:kramer=DEBUG',
                                    '--flag',
                                    '--logfile=foo', '-Q', 'bar', 'a', 'b',
                                    '--', '.disable_rate_limits=1'])
        self.assertEqual(x.options, {'--logfile': 'foo',
                                     '-Q': 'bar',
                                     '--flag': None})
        self.assertEqual(x.values, ['a', 'b'])
        self.assertEqual(x.namespaces.get('jerry,elaine'), {'-c': '5'})
        self.assertEqual(x.namespaces.get('kramer'), {'--loglevel': 'DEBUG'})
        self.assertEqual(x.passthrough, '-- .disable_rate_limits=1')


def multi_args(p, *args, **kwargs):
    return MultiParser(*args, **kwargs).parse(p)


class test_multi_args(AppCase):

    @patch('celery.apps.multi.gethostname')
    def test_parse(self, gethostname):
        gethostname.return_value = 'example.com'
        p = NamespacedOptionParser([
            '-c:jerry,elaine', '5',
            '--loglevel:kramer=DEBUG',
            '--flag',
            '--logfile=foo', '-Q', 'bar', 'jerry',
            'elaine', 'kramer',
            '--', '.disable_rate_limits=1',
        ])
        it = multi_args(p, cmd='COMMAND', append='*AP*',
                        prefix='*P*', suffix='*S*')
        nodes = list(it)

        def assert_line_in(name, args):
            self.assertIn(name, {n.name for n in nodes})
            argv = None
            for node in nodes:
                if node.name == name:
                    argv = node.argv
            self.assertTrue(argv)
            for arg in args:
                self.assertIn(arg, argv)

        assert_line_in(
            '*P*jerry@*S*',
            ['COMMAND', '-n *P*jerry@*S*', '-Q bar',
             '-c 5', '--flag', '--logfile=foo',
             '-- .disable_rate_limits=1', '*AP*'],
        )
        assert_line_in(
            '*P*elaine@*S*',
            ['COMMAND', '-n *P*elaine@*S*', '-Q bar',
             '-c 5', '--flag', '--logfile=foo',
             '-- .disable_rate_limits=1', '*AP*'],
        )
        assert_line_in(
            '*P*kramer@*S*',
            ['COMMAND', '--loglevel=DEBUG', '-n *P*kramer@*S*',
             '-Q bar', '--flag', '--logfile=foo',
             '-- .disable_rate_limits=1', '*AP*'],
        )
        expand = nodes[0].expander
        self.assertEqual(expand('%h'), '*P*jerry@*S*')
        self.assertEqual(expand('%n'), '*P*jerry')
        nodes2 = list(multi_args(p, cmd='COMMAND', append='',
                      prefix='*P*', suffix='*S*'))
        self.assertEqual(nodes2[0].argv[-1], '-- .disable_rate_limits=1')

        p2 = NamespacedOptionParser(['10', '-c:1', '5'])
        nodes3 = list(multi_args(p2, cmd='COMMAND'))
        self.assertEqual(len(nodes3), 10)
        self.assertEqual(nodes3[0].name, 'celery1@example.com')
        self.assertTupleEqual(
            nodes3[0].argv,
            ('COMMAND', '-n celery1@example.com', '-c 5', ''),
        )
        for i, worker in enumerate(nodes3[1:]):
            self.assertEqual(worker.name, 'celery%s@example.com' % (i + 2))
            self.assertTupleEqual(
                worker.argv,
                ('COMMAND', '-n celery%s@example.com' % (i + 2), ''),
            )

        nodes4 = list(multi_args(p2, cmd='COMMAND', suffix='""'))
        self.assertEqual(len(nodes4), 10)
        self.assertEqual(nodes4[0].name, 'celery1@')
        self.assertTupleEqual(
            nodes4[0].argv,
            ('COMMAND', '-n celery1@', '-c 5', ''),
        )

        p3 = NamespacedOptionParser(['foo@', '-c:foo', '5'])
        nodes5 = list(multi_args(p3, cmd='COMMAND', suffix='""'))
        self.assertEqual(nodes5[0].name, 'foo@')
        self.assertTupleEqual(
            nodes5[0].argv,
            ('COMMAND', '-n foo@', '-c 5', ''),
        )

        p4 = NamespacedOptionParser(['foo', '-Q:1', 'test'])
        nodes6 = list(multi_args(p4, cmd='COMMAND', suffix='""'))
        self.assertEqual(nodes6[0].name, 'foo@')
        self.assertTupleEqual(
            nodes6[0].argv,
            ('COMMAND', '-n foo@', '-Q test', ''),
        )

        p5 = NamespacedOptionParser(['foo@bar', '-Q:1', 'test'])
        nodes7 = list(multi_args(p5, cmd='COMMAND', suffix='""'))
        self.assertEqual(nodes7[0].name, 'foo@bar')
        self.assertTupleEqual(
            nodes7[0].argv,
            ('COMMAND', '-n foo@bar', '-Q test', ''),
        )

        p6 = NamespacedOptionParser(['foo@bar', '-Q:0', 'test'])
        with self.assertRaises(KeyError):
            list(multi_args(p6))

    def test_optmerge(self):
        p = NamespacedOptionParser(['foo', 'test'])
        p.options = {'x': 'y'}
        r = p.optmerge('foo')
        self.assertEqual(r['x'], 'y')


class test_Node(AppCase):

    def setup(self):
        self.p = Mock(name='p')
        self.p.options = {
            '--executable': 'python',
            '--logfile': 'foo.log',
        }
        self.p.namespaces = {}
        self.expander = Mock(name='expander')
        self.node = Node(
            'foo@bar.com', ['-A', 'proj'], self.expander, 'foo', self.p,
        )
        self.node.pid = 303

    @patch('os.kill')
    def test_send(self, kill):
        self.assertTrue(self.node.send(9))
        kill.assert_called_with(self.node.pid, 9)

    @patch('os.kill')
    def test_send__ESRCH(self, kill):
        kill.side_effect = OSError()
        kill.side_effect.errno = errno.ESRCH
        self.assertFalse(self.node.send(9))
        kill.assert_called_with(self.node.pid, 9)

    @patch('os.kill')
    def test_send__error(self, kill):
        kill.side_effect = OSError()
        kill.side_effect.errno = errno.ENOENT
        with self.assertRaises(OSError):
            self.node.send(9)
        kill.assert_called_with(self.node.pid, 9)

    def test_alive(self):
        self.node.send = Mock(name='send')
        self.assertIs(self.node.alive(), self.node.send.return_value)
        self.node.send.assert_called_with(0)

    def test_start(self):
        self.node._waitexec = Mock(name='_waitexec')
        self.node.start(env={'foo': 'bar'}, kw=2)
        self.node._waitexec.assert_called_with(
            self.node.argv, path=self.node.executable,
            env={'foo': 'bar'}, kw=2,
        )

    @patch('celery.apps.multi.Popen')
    def test_waitexec(self, Popen, argv=['A', 'B']):
        on_spawn = Mock(name='on_spawn')
        on_signalled = Mock(name='on_signalled')
        on_failure = Mock(name='on_failure')
        env = Mock(name='env')
        self.node.handle_process_exit = Mock(name='handle_process_exit')

        self.node._waitexec(
            argv,
            path='python',
            env=env,
            on_spawn=on_spawn,
            on_signalled=on_signalled,
            on_failure=on_failure,
        )

        Popen.assert_called_with(
            self.node.prepare_argv(argv, 'python'), env=env)
        self.node.handle_process_exit.assert_called_with(
            Popen().wait(),
            on_signalled=on_signalled,
            on_failure=on_failure,
        )

    def test_handle_process_exit(self):
        self.assertEqual(
            self.node.handle_process_exit(0),
            0,
        )

    def test_handle_process_exit__failure(self):
        on_failure = Mock(name='on_failure')
        self.assertEqual(
            self.node.handle_process_exit(9, on_failure=on_failure),
            9,
        )
        on_failure.assert_called_with(self.node, 9)

    def test_handle_process_exit__signalled(self):
        on_signalled = Mock(name='on_signalled')
        self.assertEqual(
            self.node.handle_process_exit(-9, on_signalled=on_signalled),
            9,
        )
        on_signalled.assert_called_with(self.node, 9)

    def test_logfile(self):
        self.assertEqual(self.node.logfile, self.expander.return_value)
        self.expander.assert_called_with('foo.log')


class test_Cluster(AppCase):

    def setup(self):
        self.Popen = self.patch('celery.apps.multi.Popen')
        self.kill = self.patch('os.kill')
        self.gethostname = self.patch('celery.apps.multi.gethostname')
        self.gethostname.return_value = 'example.com'
        self.Pidfile = self.patch('celery.apps.multi.Pidfile')
        self.cluster = Cluster(
            ['foo', 'bar', 'baz'],
            on_stopping_preamble=Mock(name='on_stopping_preamble'),
            on_send_signal=Mock(name='on_send_signal'),
            on_still_waiting_for=Mock(name='on_still_waiting_for'),
            on_still_waiting_progress=Mock(name='on_still_waiting_progress'),
            on_still_waiting_end=Mock(name='on_still_waiting_end'),
            on_node_start=Mock(name='on_node_start'),
            on_node_restart=Mock(name='on_node_restart'),
            on_node_shutdown_ok=Mock(name='on_node_shutdown_ok'),
            on_node_status=Mock(name='on_node_status'),
            on_node_signal=Mock(name='on_node_signal'),
            on_node_signal_dead=Mock(name='on_node_signal_dead'),
            on_node_down=Mock(name='on_node_down'),
            on_child_spawn=Mock(name='on_child_spawn'),
            on_child_signalled=Mock(name='on_child_signalled'),
            on_child_failure=Mock(name='on_child_failure'),
        )

    def test_len(self):
        self.assertEqual(len(self.cluster), 3)

    def test_getitem(self):
        self.assertEqual(self.cluster[0].name, 'foo@example.com')

    def test_start(self):
        self.cluster.start_node = Mock(name='start_node')
        self.cluster.start()
        self.cluster.start_node.assert_has_calls(
            call(node) for node in self.cluster
        )

    def test_start_node(self):
        self.cluster._start_node = Mock(name='_start_node')
        node = self.cluster[0]
        self.assertIs(
            self.cluster.start_node(node),
            self.cluster._start_node.return_value,
        )
        self.cluster.on_node_start.assert_called_with(node)
        self.cluster._start_node.assert_called_with(node)
        self.cluster.on_node_status.assert_called_with(
            node, self.cluster._start_node(),
        )

    def test__start_node(self):
        node = self.cluster[0]
        node.start = Mock(name='node.start')
        self.assertIs(
            self.cluster._start_node(node),
            node.start.return_value,
        )
        node.start.assert_called_with(
            self.cluster.env,
            on_spawn=self.cluster.on_child_spawn,
            on_signalled=self.cluster.on_child_signalled,
            on_failure=self.cluster.on_child_failure,
        )

    def test_send_all(self):
        nodes = [Mock(name='n1'), Mock(name='n2')]
        self.cluster.getpids = Mock(name='getpids')
        self.cluster.getpids.return_value = nodes
        self.cluster.send_all(15)
        self.cluster.on_node_signal.assert_has_calls(
            call(node, 'TERM') for node in nodes
        )
        for node in nodes:
            node.send.assert_called_with(15, self.cluster.on_node_signal_dead)

    def test_kill(self):
        self.cluster.send_all = Mock(name='.send_all')
        self.cluster.kill()
        self.cluster.send_all.assert_called_with(signal.SIGKILL)

    def test_getpids(self):
        self.gethostname.return_value = 'e.com'
        self.prepare_pidfile_for_getpids(self.Pidfile)
        callback = Mock()

        p = Cluster(['foo', 'bar', 'baz'])
        nodes = p.getpids(on_down=callback)
        node_0, node_1 = nodes
        self.assertEqual(node_0.name, 'foo@e.com')
        self.assertEqual(
            sorted(node_0.argv),
            sorted([
                '',
                '--executable={0}'.format(node_0.executable),
                '--logfile=foo%I.log',
                '--pidfile=foo.pid',
                '-m celery worker --detach',
                '-n foo@e.com',
            ]),
        )
        self.assertEqual(node_0.pid, 10)

        self.assertEqual(node_1.name, 'bar@e.com')
        self.assertEqual(
            sorted(node_1.argv),
            sorted([
                '',
                '--executable={0}'.format(node_1.executable),
                '--logfile=bar%I.log',
                '--pidfile=bar.pid',
                '-m celery worker --detach',
                '-n bar@e.com',
            ]),
        )
        self.assertEqual(node_1.pid, 11)

        # without callback, should work
        nodes = p.getpids('celery worker')

    def prepare_pidfile_for_getpids(self, Pidfile):
        class pids(object):

            def __init__(self, path):
                self.path = path

            def read_pid(self):
                try:
                    return {'foo.pid': 10,
                            'bar.pid': 11}[self.path]
                except KeyError:
                    raise ValueError()
        self.Pidfile.side_effect = pids
