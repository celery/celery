from unittest.mock import Mock

import click
import pytest
from click.testing import CliRunner

from celery.bin.celery import celery


@pytest.fixture
def color_cli(celery_app, monkeypatch):
    monkeypatch.delenv('NO_COLOR', raising=False)
    monkeypatch.delenv('CELERY_NO_COLOR', raising=False)
    monkeypatch.setattr(celery_app, 'IS_WINDOWS', False)
    monkeypatch.setattr('celery.utils.term.IS_WINDOWS', False)
    celery_app.conf.worker_log_color = None

    @click.command('color-output')
    @click.pass_context
    def color_output(ctx):
        ctx.obj.echo(ctx.obj.style('stdout', fg='red'))
        ctx.obj.secho('styled', fg='red')
        click.secho('click', fg='red')
        ctx.obj.error('stderr', fg='red')
        print(str(ctx.obj.app.log.colored().red('log')))

    monkeypatch.setitem(celery.commands, 'color-output', color_output)
    runner = CliRunner()

    def invoke(options, tty=False, env=None):
        monkeypatch.setattr('celery.app.log.isatty', lambda stream: tty)
        return runner.invoke(
            celery, [*options, 'color-output'],
            default_map={'app': celery_app},
            auto_envvar_prefix='CELERY', color=tty, env=env,
        )

    return invoke


def assert_color(result, expected):
    assert result.exit_code == 0, result.output
    assert len(result.stdout.splitlines()) == 4
    assert len(result.stderr.splitlines()) == 1
    for line in result.stdout.splitlines() + result.stderr.splitlines():
        assert ('\x1b[' in line) is expected, repr(line)


@pytest.mark.parametrize('tty', (False, True))
@pytest.mark.parametrize('options,expected', (
    ([], None),
    (['--color'], True),
    (['--no-color'], False),
    (['-C'], False),
    (['--color', '--no-color'], False),
    (['--no-color', '--color'], True),
))
def test_cli_color(color_cli, options, expected, tty):
    assert_color(color_cli(options, tty), tty if expected is None else expected)


@pytest.mark.parametrize('tty', (False, True))
@pytest.mark.parametrize('env,options,expected', (
    ({'NO_COLOR': '1'}, [], False),
    ({'NO_COLOR': '1'}, ['--color'], True),
    ({'NO_COLOR': '0'}, [], None),
    ({'CELERY_NO_COLOR': '1'}, [], False),
    ({'CELERY_NO_COLOR': '1'}, ['--color'], True),
    ({'CELERY_NO_COLOR': '0'}, [], None),
))
def test_color_environment(color_cli, env, options, expected, tty):
    assert_color(color_cli(options, tty, env), tty if expected is None else expected)


def test_color_choice_does_not_leak_between_invocations(color_cli):
    assert_color(color_cli(['--color']), True)
    assert_color(color_cli([]), False)


def test_force_color_overrides_log_color_setting(color_cli, celery_app):
    celery_app.conf.worker_log_color = False
    assert_color(color_cli(['--color']), True)


@pytest.mark.parametrize('options,expected', (
    (['--color'], True),
    (['--no-color'], False),
    (['-C'], False),
))
def test_color_applies_to_app_load_errors(options, expected, monkeypatch):
    monkeypatch.delenv('NO_COLOR', raising=False)
    result = CliRunner().invoke(
        celery, [*options, '-A', 'missing_color_test_app', 'report'], color=True,
    )
    assert result.exit_code == 2
    assert 'Unable to load celery application' in result.stderr
    assert ('\x1b[' in result.stderr) is expected


@pytest.mark.parametrize('options,env', (
    (['-C'], {}),
    (['--no-color'], {}),
    ([], {'NO_COLOR': '1'}),
))
def test_amqp_without_color(celery_app, monkeypatch, options, env):
    monkeypatch.delenv('NO_COLOR', raising=False)
    monkeypatch.delenv('CELERY_NO_COLOR', raising=False)
    connection = Mock()
    connection.as_uri.return_value = 'memory://'
    monkeypatch.setattr(celery_app, 'connection', Mock(return_value=connection))

    result = CliRunner().invoke(
        celery, [*options, 'amqp', 'exchange.declare', 'color-test', 'direct'],
        default_map={'app': celery_app}, auto_envvar_prefix='CELERY',
        color=True, env=env,
    )

    assert result.exit_code == 0, result.output
    assert '-> connected.' in result.stdout
    assert 'OK' in result.stdout
    assert '\x1b[' not in result.output
    connection.connect.assert_called_once_with()
    connection.default_channel.exchange_declare.assert_called_once_with(
        exchange='color-test', type='direct', passive=False,
        durable=False, auto_delete=False,
    )
