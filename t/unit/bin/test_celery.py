from __future__ import absolute_import, unicode_literals

import sys

import pytest
from case import Mock, patch

from celery import __main__
from celery.bin import celery as mod
from celery.bin.base import Error
from celery.bin.celery import (CeleryCommand, Command, determine_exit_status,
                               help)
from celery.bin.celery import main as mainfun
from celery.bin.celery import multi, report
from celery.five import WhateverIO
from celery.platforms import EX_FAILURE, EX_OK, EX_USAGE


class test__main__:

    def test_main(self):
        with patch('celery.__main__.maybe_patch_concurrency') as mpc:
            with patch('celery.bin.celery.main') as main:
                __main__.main()
                mpc.assert_called_with()
                main.assert_called_with()

    def test_main__multi(self):
        with patch('celery.__main__.maybe_patch_concurrency') as mpc:
            with patch('celery.bin.celery.main') as main:
                prev, sys.argv = sys.argv, ['foo', 'multi']
                try:
                    __main__.main()
                    mpc.assert_not_called()
                    main.assert_called_with()
                finally:
                    sys.argv = prev


class test_Command:

    def test_Error_repr(self):
        x = Error('something happened')
        assert x.status is not None
        assert x.reason
        assert str(x)

    def setup(self):
        self.out = WhateverIO()
        self.err = WhateverIO()
        self.cmd = Command(self.app, stdout=self.out, stderr=self.err)

    def test_error(self):
        self.cmd.out = Mock()
        self.cmd.error('FOO')
        self.cmd.out.assert_called()

    def test_out(self):
        f = Mock()
        self.cmd.out('foo', f)

    def test_call(self):

        def ok_run():
            pass

        self.cmd.run = ok_run
        assert self.cmd() == EX_OK

        def error_run():
            raise Error('error', EX_FAILURE)
        self.cmd.run = error_run
        assert self.cmd() == EX_FAILURE

    def test_run_from_argv(self):
        with pytest.raises(NotImplementedError):
            self.cmd.run_from_argv('prog', ['foo', 'bar'])

    def test_pretty_list(self):
        assert self.cmd.pretty([])[1] == '- empty -'
        assert 'bar', self.cmd.pretty(['foo' in 'bar'][1])

    def test_pretty_dict(self, text='the quick brown fox'):
        assert 'OK' in str(self.cmd.pretty({'ok': text})[0])
        assert 'ERROR' in str(self.cmd.pretty({'error': text})[0])

    def test_pretty(self):
        assert 'OK' in str(self.cmd.pretty('the quick brown'))
        assert 'OK' in str(self.cmd.pretty(object()))
        assert 'OK' in str(self.cmd.pretty({'foo': 'bar'}))


class test_report:

    def test_run(self):
        out = WhateverIO()
        r = report(app=self.app, stdout=out)
        assert r.run() == EX_OK
        assert out.getvalue()


class test_help:

    def test_run(self):
        out = WhateverIO()
        h = help(app=self.app, stdout=out)
        h.parser = Mock()
        assert h.run() == EX_USAGE
        assert out.getvalue()
        assert h.usage('help')
        h.parser.print_help.assert_called_with()


class test_CeleryCommand:

    def test_execute_from_commandline(self):
        x = CeleryCommand(app=self.app)
        x.handle_argv = Mock()
        x.handle_argv.return_value = 1
        with pytest.raises(SystemExit):
            x.execute_from_commandline()

        x.handle_argv.return_value = True
        with pytest.raises(SystemExit):
            x.execute_from_commandline()

        x.handle_argv.side_effect = KeyboardInterrupt()
        with pytest.raises(SystemExit):
            x.execute_from_commandline()

        x.respects_app_option = True
        with pytest.raises(SystemExit):
            x.execute_from_commandline(['celery', 'multi'])
        assert not x.respects_app_option
        x.respects_app_option = True
        with pytest.raises(SystemExit):
            x.execute_from_commandline(['manage.py', 'celery', 'multi'])
        assert not x.respects_app_option

    def test_with_pool_option(self):
        x = CeleryCommand(app=self.app)
        assert x.with_pool_option(['celery', 'events']) is None
        assert x.with_pool_option(['celery', 'worker'])
        assert x.with_pool_option(['manage.py', 'celery', 'worker'])

    def test_load_extensions_no_commands(self):
        with patch('celery.bin.celery.Extensions') as Ext:
            ext = Ext.return_value = Mock(name='Extension')
            ext.load.return_value = None
            x = CeleryCommand(app=self.app)
            x.load_extension_commands()

    def test_load_extensions_commands(self):
        with patch('celery.bin.celery.Extensions') as Ext:
            prev, mod.command_classes = list(mod.command_classes), Mock()
            try:
                ext = Ext.return_value = Mock(name='Extension')
                ext.load.return_value = ['foo', 'bar']
                x = CeleryCommand(app=self.app)
                x.load_extension_commands()
                mod.command_classes.append.assert_called_with(
                    ('Extensions', ['foo', 'bar'], 'magenta'),
                )
            finally:
                mod.command_classes = prev

    def test_determine_exit_status(self):
        assert determine_exit_status('true') == EX_OK
        assert determine_exit_status('') == EX_FAILURE

    def test_relocate_args_from_start(self):
        x = CeleryCommand(app=self.app)
        assert x._relocate_args_from_start(None) == []
        relargs1 = x._relocate_args_from_start([
            '-l', 'debug', 'worker', '-c', '3', '--foo',
        ])
        assert relargs1 == ['worker', '-c', '3', '--foo', '-l', 'debug']
        relargs2 = x._relocate_args_from_start([
            '--pool=gevent', '-l', 'debug', 'worker', '--foo', '-c', '3',
        ])
        assert relargs2 == [
            'worker', '--foo', '-c', '3',
            '--pool=gevent', '-l', 'debug',
        ]
        assert x._relocate_args_from_start(['foo', '--foo=1']) == [
            'foo', '--foo=1',
        ]

    def test_register_command(self):
        prev, CeleryCommand.commands = dict(CeleryCommand.commands), {}
        try:
            fun = Mock(name='fun')
            CeleryCommand.register_command(fun, name='foo')
            assert CeleryCommand.commands['foo'] is fun
        finally:
            CeleryCommand.commands = prev

    def test_handle_argv(self):
        x = CeleryCommand(app=self.app)
        x.execute = Mock()
        x.handle_argv('celery', [])
        x.execute.assert_called_with('help', ['help'])

        x.handle_argv('celery', ['start', 'foo'])
        x.execute.assert_called_with('start', ['start', 'foo'])

    def test_execute(self):
        x = CeleryCommand(app=self.app)
        Help = x.commands['help'] = Mock()
        help = Help.return_value = Mock()
        x.execute('fooox', ['a'])
        help.run_from_argv.assert_called_with(x.prog_name, [], command='help')
        help.reset()
        x.execute('help', ['help'])
        help.run_from_argv.assert_called_with(x.prog_name, [], command='help')

        Dummy = x.commands['dummy'] = Mock()
        dummy = Dummy.return_value = Mock()
        exc = dummy.run_from_argv.side_effect = Error(
            'foo', status='EX_FAILURE',
        )
        x.on_error = Mock(name='on_error')
        help.reset()
        x.execute('dummy', ['dummy'])
        x.on_error.assert_called_with(exc)
        dummy.run_from_argv.assert_called_with(
            x.prog_name, [], command='dummy',
        )
        help.run_from_argv.assert_called_with(
            x.prog_name, [], command='help',
        )

        exc = dummy.run_from_argv.side_effect = x.UsageError('foo')
        x.on_usage_error = Mock()
        x.execute('dummy', ['dummy'])
        x.on_usage_error.assert_called_with(exc)

    def test_on_usage_error(self):
        x = CeleryCommand(app=self.app)
        x.error = Mock()
        x.on_usage_error(x.UsageError('foo'), command=None)
        x.error.assert_called()
        x.on_usage_error(x.UsageError('foo'), command='dummy')

    def test_prepare_prog_name(self):
        x = CeleryCommand(app=self.app)
        main = Mock(name='__main__')
        main.__file__ = '/opt/foo.py'
        with patch.dict(sys.modules, __main__=main):
            assert x.prepare_prog_name('__main__.py') == '/opt/foo.py'
            assert x.prepare_prog_name('celery') == 'celery'


class test_multi:

    def test_get_options(self):
        assert multi(app=self.app).get_options() is None

    def test_run_from_argv(self):
        with patch('celery.bin.multi.MultiTool') as MultiTool:
            m = MultiTool.return_value = Mock()
            multi(self.app).run_from_argv('celery', ['arg'], command='multi')
            m.execute_from_commandline.assert_called_with(['multi', 'arg'])


class test_main:

    @patch('celery.bin.celery.CeleryCommand')
    def test_main(self, Command):
        cmd = Command.return_value = Mock()
        mainfun()
        cmd.execute_from_commandline.assert_called_with(None)

    @patch('celery.bin.celery.CeleryCommand')
    def test_main_KeyboardInterrupt(self, Command):
        cmd = Command.return_value = Mock()
        cmd.execute_from_commandline.side_effect = KeyboardInterrupt()
        mainfun()
        cmd.execute_from_commandline.assert_called_with(None)
