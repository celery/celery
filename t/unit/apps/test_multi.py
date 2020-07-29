from __future__ import absolute_import, unicode_literals

import errno
import signal
import sys
import os

import pytest
from case import Mock, call, patch, skip

from celery.apps.multi import (Cluster, MultiParser, NamespacedOptionParser,
                               Node, format_opt)


class test_functions:

    def test_parse_ns_range(self):
        m = MultiParser()
        assert m._parse_ns_range('1-3', True), ['1', '2' == '3']
        assert m._parse_ns_range('1-3', False) == ['1-3']
        assert m._parse_ns_range('1-3,10,11,20', True) == [
            '1', '2', '3', '10', '11', '20',
        ]

    def test_format_opt(self):
        assert format_opt('--foo', None) == '--foo'
        assert format_opt('-c', 1) == '-c 1'
        assert format_opt('--log', 'foo') == '--log=foo'


class test_NamespacedOptionParser:

    def test_parse(self):
        x = NamespacedOptionParser(['-c:1,3', '4'])
        x.parse()
        assert x.namespaces.get('1,3') == {'-c': '4'}
        x = NamespacedOptionParser(['-c:jerry,elaine', '5',
                                    '--loglevel:kramer=DEBUG',
                                    '--flag',
                                    '--logfile=foo', '-Q', 'bar', 'a', 'b',
                                    '--', '.disable_rate_limits=1'])
        x.parse()
        assert x.options == {
            '--logfile': 'foo',
            '-Q': 'bar',
            '--flag': None,
        }
        assert x.values, ['a' == 'b']
        assert x.namespaces.get('jerry,elaine') == {'-c': '5'}
        assert x.namespaces.get('kramer') == {'--loglevel': 'DEBUG'}
        assert x.passthrough == '-- .disable_rate_limits=1'


def multi_args(p, *args, **kwargs):
    return MultiParser(*args, **kwargs).parse(p)


class test_multi_args:

    @patch('celery.apps.multi.os.mkdir')
    @patch('celery.apps.multi.gethostname')
    def test_parse(self, gethostname, mkdirs_mock):
        gethostname.return_value = 'example.com'
        p = NamespacedOptionParser([
            '-c:jerry,elaine', '5',
            '--loglevel:kramer=DEBUG',
            '--flag',
            '--logfile=/var/log/celery/foo', '-Q', 'bar', 'jerry',
            'elaine', 'kramer',
            '--', '.disable_rate_limits=1',
        ])
        p.parse()
        it = multi_args(p, cmd='COMMAND', append='*AP*',
                        prefix='*P*', suffix='*S*')
        nodes = list(it)

        def assert_line_in(name, args):
            assert name in {n.name for n in nodes}
            argv = None
            for node in nodes:
                if node.name == name:
                    argv = node.argv
            assert argv
            for arg in args:
                assert arg in argv

        assert_line_in(
            '*P*jerry@*S*',
            ['COMMAND', '-n *P*jerry@*S*', '-Q bar',
             '-c 5', '--flag', '--logfile=/var/log/celery/foo',
             '-- .disable_rate_limits=1', '*AP*'],
        )
        assert_line_in(
            '*P*elaine@*S*',
            ['COMMAND', '-n *P*elaine@*S*', '-Q bar',
             '-c 5', '--flag', '--logfile=/var/log/celery/foo',
             '-- .disable_rate_limits=1', '*AP*'],
        )
        assert_line_in(
            '*P*kramer@*S*',
            ['COMMAND', '--loglevel=DEBUG', '-n *P*kramer@*S*',
             '-Q bar', '--flag', '--logfile=/var/log/celery/foo',
             '-- .disable_rate_limits=1', '*AP*'],
        )
        expand = nodes[0].expander
        assert expand('%h') == '*P*jerry@*S*'
        assert expand('%n') == '*P*jerry'
        nodes2 = list(multi_args(p, cmd='COMMAND', append='',
                                 prefix='*P*', suffix='*S*'))
        assert nodes2[0].argv[-1] == '-- .disable_rate_limits=1'

        p2 = NamespacedOptionParser(['10', '-c:1', '5'])
        p2.parse()
        nodes3 = list(multi_args(p2, cmd='COMMAND'))

        def _args(name, *args):
            return args + (
                '--pidfile={}.pid'.format(os.path.join(os.path.normpath('/var/run/celery/'), name)),
                '--logfile={}%I.log'.format(os.path.join(os.path.normpath('/var/log/celery/'), name)),
                '--executable={0}'.format(sys.executable),
                '',
            )

        assert len(nodes3) == 10
        assert nodes3[0].name == 'celery1@example.com'
        assert nodes3[0].argv == (
            'COMMAND', '-c 5', '-n celery1@example.com') + _args('celery1')
        for i, worker in enumerate(nodes3[1:]):
            assert worker.name == 'celery%s@example.com' % (i + 2)
            node_i = 'celery%s' % (i + 2,)
            assert worker.argv == (
                'COMMAND',
                '-n %s@example.com' % (node_i,)) + _args(node_i)

        nodes4 = list(multi_args(p2, cmd='COMMAND', suffix='""'))
        assert len(nodes4) == 10
        assert nodes4[0].name == 'celery1@'
        assert nodes4[0].argv == (
            'COMMAND', '-c 5', '-n celery1@') + _args('celery1')

        p3 = NamespacedOptionParser(['foo@', '-c:foo', '5'])
        p3.parse()
        nodes5 = list(multi_args(p3, cmd='COMMAND', suffix='""'))
        assert nodes5[0].name == 'foo@'
        assert nodes5[0].argv == (
            'COMMAND', '-c 5', '-n foo@') + _args('foo')

        p4 = NamespacedOptionParser(['foo', '-Q:1', 'test'])
        p4.parse()
        nodes6 = list(multi_args(p4, cmd='COMMAND', suffix='""'))
        assert nodes6[0].name == 'foo@'
        assert nodes6[0].argv == (
            'COMMAND', '-Q test', '-n foo@') + _args('foo')

        p5 = NamespacedOptionParser(['foo@bar', '-Q:1', 'test'])
        p5.parse()
        nodes7 = list(multi_args(p5, cmd='COMMAND', suffix='""'))
        assert nodes7[0].name == 'foo@bar'
        assert nodes7[0].argv == (
            'COMMAND', '-Q test', '-n foo@bar') + _args('foo')

        p6 = NamespacedOptionParser(['foo@bar', '-Q:0', 'test'])
        p6.parse()
        with pytest.raises(KeyError):
            list(multi_args(p6))

    def test_optmerge(self):
        p = NamespacedOptionParser(['foo', 'test'])
        p.parse()
        p.options = {'x': 'y'}
        r = p.optmerge('foo')
        assert r['x'] == 'y'


class test_Node:

    def setup(self):
        self.p = Mock(name='p')
        self.p.options = {
            '--executable': 'python',
            '--logfile': '/var/log/celery/foo.log',
        }
        self.p.namespaces = {}
        with patch('celery.apps.multi.os.mkdir'):
            self.node = Node('foo@bar.com', options={'-A': 'proj'})
        self.expander = self.node.expander = Mock(name='expander')
        self.node.pid = 303

    def test_from_kwargs(self):
        with patch('celery.apps.multi.os.mkdir'):
            n = Node.from_kwargs(
                'foo@bar.com',
                max_tasks_per_child=30, A='foo', Q='q1,q2', O='fair',
            )
        assert sorted(n.argv) == sorted([
            '-m celery worker --detach',
            '-A foo',
            '--executable={0}'.format(n.executable),
            '-O fair',
            '-n foo@bar.com',
            '--logfile={}'.format(os.path.normpath('/var/log/celery/foo%I.log')),
            '-Q q1,q2',
            '--max-tasks-per-child=30',
            '--pidfile={}'.format(os.path.normpath('/var/run/celery/foo.pid')),
            '',
        ])

    @patch('os.kill')
    def test_send(self, kill):
        assert self.node.send(9)
        kill.assert_called_with(self.node.pid, 9)

    @patch('os.kill')
    def test_send__ESRCH(self, kill):
        kill.side_effect = OSError()
        kill.side_effect.errno = errno.ESRCH
        assert not self.node.send(9)
        kill.assert_called_with(self.node.pid, 9)

    @patch('os.kill')
    def test_send__error(self, kill):
        kill.side_effect = OSError()
        kill.side_effect.errno = errno.ENOENT
        with pytest.raises(OSError):
            self.node.send(9)
        kill.assert_called_with(self.node.pid, 9)

    def test_alive(self):
        self.node.send = Mock(name='send')
        assert self.node.alive() is self.node.send.return_value
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
        assert self.node.handle_process_exit(0) == 0

    def test_handle_process_exit__failure(self):
        on_failure = Mock(name='on_failure')
        assert self.node.handle_process_exit(9, on_failure=on_failure) == 9
        on_failure.assert_called_with(self.node, 9)

    def test_handle_process_exit__signalled(self):
        on_signalled = Mock(name='on_signalled')
        assert self.node.handle_process_exit(
            -9, on_signalled=on_signalled) == 9
        on_signalled.assert_called_with(self.node, 9)

    def test_logfile(self):
        assert self.node.logfile == self.expander.return_value
        self.expander.assert_called_with(os.path.normpath('/var/log/celery/%n%I.log'))

    @patch('celery.apps.multi.os.path.exists')
    def test_pidfile_default(self, mock_exists):
        n = Node.from_kwargs(
            'foo@bar.com',
        )
        assert n.options['--pidfile'] == os.path.normpath('/var/run/celery/%n.pid')
        mock_exists.assert_any_call(os.path.normpath('/var/run/celery'))

    @patch('celery.apps.multi.os.makedirs')
    @patch('celery.apps.multi.os.path.exists', return_value=False)
    def test_pidfile_custom(self, mock_exists, mock_dirs):
        n = Node.from_kwargs(
            'foo@bar.com',
            pidfile='/var/run/demo/celery/%n.pid'
        )
        assert n.options['--pidfile'] == '/var/run/demo/celery/%n.pid'

        try:
            mock_exists.assert_any_call('/var/run/celery')
        except AssertionError:
            pass
        else:
            raise AssertionError("Expected exists('/var/run/celery') to not have been called.")

        mock_exists.assert_any_call('/var/run/demo/celery')
        mock_dirs.assert_any_call('/var/run/demo/celery')


class test_Cluster:

    def setup(self):
        self.Popen = self.patching('celery.apps.multi.Popen')
        self.kill = self.patching('os.kill')
        self.gethostname = self.patching('celery.apps.multi.gethostname')
        self.gethostname.return_value = 'example.com'
        self.Pidfile = self.patching('celery.apps.multi.Pidfile')
        with patch('celery.apps.multi.os.mkdir'):
            self.cluster = Cluster(
                [Node('foo@example.com'),
                 Node('bar@example.com'),
                 Node('baz@example.com')],
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
        assert len(self.cluster) == 3

    def test_getitem(self):
        assert self.cluster[0].name == 'foo@example.com'

    def test_start(self):
        self.cluster.start_node = Mock(name='start_node')
        self.cluster.start()
        self.cluster.start_node.assert_has_calls(
            call(node) for node in self.cluster
        )

    def test_start_node(self):
        self.cluster._start_node = Mock(name='_start_node')
        node = self.cluster[0]
        assert (self.cluster.start_node(node) is
                self.cluster._start_node.return_value)
        self.cluster.on_node_start.assert_called_with(node)
        self.cluster._start_node.assert_called_with(node)
        self.cluster.on_node_status.assert_called_with(
            node, self.cluster._start_node(),
        )

    def test__start_node(self):
        node = self.cluster[0]
        node.start = Mock(name='node.start')
        assert self.cluster._start_node(node) is node.start.return_value
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

    @skip.if_win32()
    def test_kill(self):
        self.cluster.send_all = Mock(name='.send_all')
        self.cluster.kill()
        self.cluster.send_all.assert_called_with(signal.SIGKILL)

    def test_getpids(self):
        self.gethostname.return_value = 'e.com'
        self.prepare_pidfile_for_getpids(self.Pidfile)
        callback = Mock()

        with patch('celery.apps.multi.os.mkdir'):
            p = Cluster([
                Node('foo@e.com'),
                Node('bar@e.com'),
                Node('baz@e.com'),
            ])
        nodes = p.getpids(on_down=callback)
        node_0, node_1 = nodes
        assert node_0.name == 'foo@e.com'
        assert sorted(node_0.argv) == sorted([
            '',
            '--executable={0}'.format(node_0.executable),
            '--logfile={}'.format(os.path.normpath('/var/log/celery/foo%I.log')),
            '--pidfile={}'.format(os.path.normpath('/var/run/celery/foo.pid')),
            '-m celery worker --detach',
            '-n foo@e.com',
        ])
        assert node_0.pid == 10

        assert node_1.name == 'bar@e.com'
        assert sorted(node_1.argv) == sorted([
            '',
            '--executable={0}'.format(node_1.executable),
            '--logfile={}'.format(os.path.normpath('/var/log/celery/bar%I.log')),
            '--pidfile={}'.format(os.path.normpath('/var/run/celery/bar.pid')),
            '-m celery worker --detach',
            '-n bar@e.com',
        ])
        assert node_1.pid == 11

        # without callback, should work
        nodes = p.getpids('celery worker')

    def prepare_pidfile_for_getpids(self, Pidfile):
        class pids(object):

            def __init__(self, path):
                self.path = path

            def read_pid(self):
                try:
                    return {os.path.normpath('/var/run/celery/foo.pid'): 10,
                            os.path.normpath('/var/run/celery/bar.pid'): 11}[self.path]
                except KeyError:
                    raise ValueError()
        self.Pidfile.side_effect = pids
