from __future__ import absolute_import, unicode_literals

import os

import pytest

from case import Mock, mock, patch
from celery.bin.base import Command, Extensions, Option
from celery.five import bytes_if_py2


class MyApp(object):
    user_options = {'preload': None}


APP = MyApp()  # <-- Used by test_with_custom_app


class MockCommand(Command):
    mock_args = ('arg1', 'arg2', 'arg3')

    def parse_options(self, prog_name, arguments, command=None):
        options = {'foo': 'bar', 'prog_name': prog_name}
        return options, self.mock_args

    def run(self, *args, **kwargs):
        return args, kwargs


class test_Extensions:

    def test_load(self):
        with patch('pkg_resources.iter_entry_points') as iterep:
            with patch('celery.utils.imports.symbol_by_name') as symbyname:
                ep = Mock()
                ep.name = 'ep'
                ep.module_name = 'foo'
                ep.attrs = ['bar', 'baz']
                iterep.return_value = [ep]
                cls = symbyname.return_value = Mock()
                register = Mock()
                e = Extensions('unit', register)
                e.load()
                symbyname.assert_called_with('foo:bar')
                register.assert_called_with(cls, name='ep')

            with patch('celery.utils.imports.symbol_by_name') as symbyname:
                symbyname.side_effect = SyntaxError()
                with patch('warnings.warn') as warn:
                    e.load()
                    warn.assert_called()

            with patch('celery.utils.imports.symbol_by_name') as symbyname:
                symbyname.side_effect = KeyError('foo')
                with pytest.raises(KeyError):
                    e.load()


class test_Command:

    def test_get_options(self):
        cmd = Command()
        cmd.option_list = (1, 2, 3)
        assert cmd.get_options() == (1, 2, 3)

    def test_custom_description(self):

        class C(Command):
            description = 'foo'

        c = C()
        assert c.description == 'foo'

    def test_format_epilog(self):
        assert Command()._format_epilog('hello')
        assert not Command()._format_epilog('')

    def test_format_description(self):
        assert Command()._format_description('hello')

    def test_register_callbacks(self):
        c = Command(on_error=8, on_usage_error=9)
        assert c.on_error == 8
        assert c.on_usage_error == 9

    def test_run_raises_UsageError(self):
        cb = Mock()
        c = Command(on_usage_error=cb)
        c.verify_args = Mock()
        c.run = Mock()
        exc = c.run.side_effect = c.UsageError('foo', status=3)

        assert c() == exc.status
        cb.assert_called_with(exc)
        c.verify_args.assert_called_with(())

    def test_default_on_usage_error(self):
        cmd = Command()
        cmd.handle_error = Mock()
        exc = Exception()
        cmd.on_usage_error(exc)
        cmd.handle_error.assert_called_with(exc)

    def test_verify_args_missing(self):
        c = Command()

        def run(a, b, c):
            pass
        c.run = run

        with pytest.raises(c.UsageError):
            c.verify_args((1,))
        c.verify_args((1, 2, 3))

    def test_run_interface(self):
        with pytest.raises(NotImplementedError):
            Command().run()

    @patch('sys.stdout')
    def test_early_version(self, stdout):
        cmd = Command()
        with pytest.raises(SystemExit):
            cmd.early_version(['--version'])

    def test_execute_from_commandline(self, app):
        cmd = MockCommand(app=app)
        args1, kwargs1 = cmd.execute_from_commandline()     # sys.argv
        assert args1 == cmd.mock_args
        assert kwargs1['foo'] == 'bar'
        assert kwargs1.get('prog_name')
        args2, kwargs2 = cmd.execute_from_commandline(['foo'])   # pass list
        assert args2 == cmd.mock_args
        assert kwargs2['foo'] == 'bar'
        assert kwargs2['prog_name'] == 'foo'

    def test_with_bogus_args(self, app):
        with mock.stdouts() as (_, stderr):
            cmd = MockCommand(app=app)
            cmd.supports_args = False
            with pytest.raises(SystemExit):
                cmd.execute_from_commandline(argv=['--bogus'])
            assert stderr.getvalue()
            assert 'Unrecognized' in stderr.getvalue()

    def test_with_custom_config_module(self, app):
        prev = os.environ.pop('CELERY_CONFIG_MODULE', None)
        try:
            cmd = MockCommand(app=app)
            cmd.setup_app_from_commandline(['--config=foo.bar.baz'])
            assert os.environ.get('CELERY_CONFIG_MODULE') == 'foo.bar.baz'
        finally:
            if prev:
                os.environ['CELERY_CONFIG_MODULE'] = prev
            else:
                os.environ.pop('CELERY_CONFIG_MODULE', None)

    def test_with_custom_broker(self, app):
        prev = os.environ.pop('CELERY_BROKER_URL', None)
        try:
            cmd = MockCommand(app=app)
            cmd.setup_app_from_commandline(['--broker=xyzza://'])
            assert os.environ.get('CELERY_BROKER_URL') == 'xyzza://'
        finally:
            if prev:
                os.environ['CELERY_BROKER_URL'] = prev
            else:
                os.environ.pop('CELERY_BROKER_URL', None)

    def test_with_custom_result_backend(self, app):
        prev = os.environ.pop('CELERY_RESULT_BACKEND', None)
        try:
            cmd = MockCommand(app=app)
            cmd.setup_app_from_commandline(['--result-backend=xyzza://'])
            assert os.environ.get('CELERY_RESULT_BACKEND') == 'xyzza://'
        finally:
            if prev:
                os.environ['CELERY_RESULT_BACKEND'] = prev
            else:
                os.environ.pop('CELERY_RESULT_BACKEND', None)

    def test_with_custom_app(self, app):
        cmd = MockCommand(app=app)
        appstr = '.'.join([__name__, 'APP'])
        cmd.setup_app_from_commandline(['--app=%s' % (appstr,),
                                        '--loglevel=INFO'])
        assert cmd.app is APP
        cmd.setup_app_from_commandline(['-A', appstr,
                                        '--loglevel=INFO'])
        assert cmd.app is APP

    def test_setup_app_sets_quiet(self, app):
        cmd = MockCommand(app=app)
        cmd.setup_app_from_commandline(['-q'])
        assert cmd.quiet
        cmd2 = MockCommand(app=app)
        cmd2.setup_app_from_commandline(['--quiet'])
        assert cmd2.quiet

    def test_setup_app_sets_chdir(self, app):
        with patch('os.chdir') as chdir:
            cmd = MockCommand(app=app)
            cmd.setup_app_from_commandline(['--workdir=/opt'])
            chdir.assert_called_with('/opt')

    def test_setup_app_sets_loader(self, app):
        prev = os.environ.get('CELERY_LOADER')
        try:
            cmd = MockCommand(app=app)
            cmd.setup_app_from_commandline(['--loader=X.Y:Z'])
            assert os.environ['CELERY_LOADER'] == 'X.Y:Z'
        finally:
            if prev is not None:
                os.environ['CELERY_LOADER'] = prev
            else:
                del(os.environ['CELERY_LOADER'])

    def test_setup_app_no_respect(self, app):
        cmd = MockCommand(app=app)
        cmd.respects_app_option = False
        with patch('celery.bin.base.Celery') as cp:
            cmd.setup_app_from_commandline(['--app=x.y:z'])
            cp.assert_called()

    def test_setup_app_custom_app(self, app):
        cmd = MockCommand(app=app)
        app = cmd.app = Mock()
        app.user_options = {'preload': None}
        cmd.setup_app_from_commandline([])
        assert cmd.app == app

    def test_find_app_suspects(self, app):
        cmd = MockCommand(app=app)
        assert cmd.find_app('t.unit.bin.proj.app')
        assert cmd.find_app('t.unit.bin.proj')
        assert cmd.find_app('t.unit.bin.proj:hello')
        assert cmd.find_app('t.unit.bin.proj.hello')
        assert cmd.find_app('t.unit.bin.proj.app:app')
        assert cmd.find_app('t.unit.bin.proj.app.app')
        with pytest.raises(AttributeError):
            cmd.find_app('t.unit.bin')

        with pytest.raises(AttributeError):
            cmd.find_app(__name__)

    def test_ask(self, app, patching):
        try:
            input = patching('celery.bin.base.input')
        except AttributeError:
            input = patching('builtins.input')
        cmd = MockCommand(app=app)
        input.return_value = 'yes'
        assert cmd.ask('q', ('yes', 'no'), 'no') == 'yes'
        input.return_value = 'nop'
        assert cmd.ask('q', ('yes', 'no'), 'no') == 'no'

    def test_host_format(self, app):
        cmd = MockCommand(app=app)
        with patch('celery.utils.nodenames.gethostname') as hn:
            hn.return_value = 'blacktron.example.com'
            assert cmd.host_format('') == ''
            assert (cmd.host_format('celery@%h') ==
                    'celery@blacktron.example.com')
            assert cmd.host_format('celery@%d') == 'celery@example.com'
            assert cmd.host_format('celery@%n') == 'celery@blacktron'

    def test_say_chat_quiet(self, app):
        cmd = MockCommand(app=app)
        cmd.quiet = True
        assert cmd.say_chat('<-', 'foo', 'foo') is None

    def test_say_chat_show_body(self, app):
        cmd = MockCommand(app=app)
        cmd.out = Mock()
        cmd.show_body = True
        cmd.say_chat('->', 'foo', 'body')
        cmd.out.assert_called_with('body')

    def test_say_chat_no_body(self, app):
        cmd = MockCommand(app=app)
        cmd.out = Mock()
        cmd.show_body = False
        cmd.say_chat('->', 'foo', 'body')

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_with_cmdline_config(self, app):
        cmd = MockCommand(app=app)
        cmd.enable_config_from_cmdline = True
        cmd.namespace = 'worker'
        rest = cmd.setup_app_from_commandline(argv=[
            '--loglevel=INFO', '--',
            'result.backend=redis://backend.example.com',
            'broker.url=amqp://broker.example.com',
            '.prefetch_multiplier=100'])
        assert cmd.app.conf.result_backend == 'redis://backend.example.com'
        assert cmd.app.conf.broker_url == 'amqp://broker.example.com'
        assert cmd.app.conf.worker_prefetch_multiplier == 100
        assert rest == ['--loglevel=INFO']

        cmd.app = None
        cmd.get_app = Mock(name='get_app')
        cmd.get_app.return_value = app
        app.user_options['preload'] = [
            Option('--foo', action='store_true'),
        ]
        cmd.setup_app_from_commandline(argv=[
            '--foo', '--loglevel=INFO', '--',
            'broker.url=amqp://broker.example.com',
            '.prefetch_multiplier=100'])
        assert cmd.app is cmd.get_app()

    def test_get_default_app(self, app, patching):
        patching('celery._state.get_current_app')
        cmd = MockCommand(app=app)
        from celery._state import get_current_app
        assert cmd._get_default_app() is get_current_app()

    def test_set_colored(self, app):
        cmd = MockCommand(app=app)
        cmd.colored = 'foo'
        assert cmd.colored == 'foo'

    def test_set_no_color(self, app):
        cmd = MockCommand(app=app)
        cmd.no_color = False
        _ = cmd.colored  # noqa
        cmd.no_color = True
        assert not cmd.colored.enabled

    def test_find_app(self, app):
        cmd = MockCommand(app=app)
        with patch('celery.utils.imports.symbol_by_name') as sbn:
            from types import ModuleType
            x = ModuleType(bytes_if_py2('proj'))

            def on_sbn(*args, **kwargs):

                def after(*args, **kwargs):
                    x.app = 'quick brown fox'
                    x.__path__ = None
                    return x
                sbn.side_effect = after
                return x
            sbn.side_effect = on_sbn
            x.__path__ = [True]
            assert cmd.find_app('proj') == 'quick brown fox'

    def test_parse_preload_options_shortopt(self):

        class TestCommand(Command):

            def add_preload_arguments(self, parser):
                parser.add_argument('-s', action='store', dest='silent')
        cmd = TestCommand()
        acc = cmd.parse_preload_options(['-s', 'yes'])
        assert acc.get('silent') == 'yes'

    def test_parse_preload_options_with_equals_and_append(self):

        class TestCommand(Command):

            def add_preload_arguments(self, parser):
                parser.add_argument('--zoom', action='append', default=[])
        cmd = Command()
        acc = cmd.parse_preload_options(['--zoom=1', '--zoom=2'])

        assert acc, {'zoom': ['1' == '2']}

    def test_parse_preload_options_without_equals_and_append(self):
        cmd = Command()
        opt = Option('--zoom', action='append', default=[])
        cmd.preload_options = (opt,)
        acc = cmd.parse_preload_options(['--zoom', '1', '--zoom', '2'])

        assert acc, {'zoom': ['1' == '2']}
