import contextlib
from types import SimpleNamespace
from typing import Tuple
from unittest.mock import patch

import click
import pytest
from click.testing import CliRunner

from celery.bin.base import handle_preload_options
from celery.bin.celery import celery
from celery.signals import user_preload_options


@pytest.fixture(autouse=True)
def reset_command_params_between_each_test():
    with contextlib.ExitStack() as stack:
        for command in celery.commands.values():
            # We only need shallow copy -- preload options are appended to the list,
            # existing options are kept as-is
            params_copy = command.params[:]
            patch_instance = patch.object(command, "params", params_copy)
            stack.enter_context(patch_instance)

        yield


@pytest.mark.parametrize(
    "subcommand_with_params",
    [
        ("purge", "-f"),
        ("shell",),
    ]
)
def test_preload_options(subcommand_with_params: Tuple[str, ...], isolated_cli_runner: CliRunner):
    # Verify commands like shell and purge can accept preload options.
    # Projects like Pyramid-Celery's ini option should be valid preload
    # options.
    res_without_preload = isolated_cli_runner.invoke(
        celery,
        ["-A", "t.unit.bin.proj.app", *subcommand_with_params, "--ini", "some_ini.ini"],
        catch_exceptions=False,
    )

    assert "No such option" in res_without_preload.output
    assert "--ini" in res_without_preload.output
    assert res_without_preload.exit_code == 2

    res_with_preload = isolated_cli_runner.invoke(
        celery,
        [
            "-A",
            "t.unit.bin.proj.pyramid_celery_app",
            *subcommand_with_params,
            "--ini",
            "some_ini.ini",
        ],
        catch_exceptions=False,
    )

    assert res_with_preload.exit_code == 0, res_with_preload.output


@contextlib.contextmanager
def _temporary_command(command):
    """Register a Click command for one CLI invoke, then put the group back."""
    original_callbacks = {
        name: existing.callback for name, existing in celery.commands.items()
    }
    celery.add_command(command)
    try:
        yield
    finally:
        for name, callback in original_callbacks.items():
            existing = celery.commands.get(name)
            if existing is not None:
                existing.callback = callback
        celery.commands.pop(command.name, None)


def _on_preload(received):
    def _receive(sender=None, app=None, options=None, **kwargs):
        received.append(dict(options))

    return _receive


def test_plugin_command_accepts_preload_options():
    """Preload options reach plugin commands that have no ``**kwargs`` (#7894).

    ``celery.commands`` entry points such as Flower are not defined here.
    Celery still adds every preload option to those commands, and Click then
    passes the values into the callback. The documented receiver is
    ``user_preload_options``.
    """
    entered = []
    received = []

    @click.command("plugin-without-kwargs")
    @click.pass_context
    def plugin_without_kwargs(ctx):
        entered.append(True)

    receiver = _on_preload(received)
    user_preload_options.connect(receiver)
    try:
        with _temporary_command(plugin_without_kwargs):
            result = CliRunner().invoke(
                celery,
                [
                    "-A",
                    "t.unit.bin.proj.pyramid_celery_app",
                    "plugin-without-kwargs",
                    "--ini",
                    "some_ini.ini",
                ],
                catch_exceptions=False,
            )
    finally:
        user_preload_options.disconnect(receiver)

    assert result.exit_code == 0, result.output
    assert entered == [True]
    assert len(received) == 1
    assert received[0]["ini"] == "some_ini.ini"


def test_decorated_command_accepts_preload_options_without_var_keyword():
    """In-tree commands using ``handle_preload_options`` must not require ``**kwargs``."""
    entered = []
    received = []

    @click.command("decorated-without-kwargs")
    @click.pass_context
    @handle_preload_options
    def decorated_without_kwargs(ctx):
        entered.append(True)

    receiver = _on_preload(received)
    user_preload_options.connect(receiver)
    try:
        with _temporary_command(decorated_without_kwargs):
            result = CliRunner().invoke(
                celery,
                [
                    "-A",
                    "t.unit.bin.proj.pyramid_celery_app",
                    "decorated-without-kwargs",
                    "--ini",
                    "some_ini.ini",
                ],
                catch_exceptions=False,
            )
    finally:
        user_preload_options.disconnect(receiver)

    assert result.exit_code == 0, result.output
    assert entered == [True]
    assert len(received) == 1
    assert received[0]["ini"] == "some_ini.ini"


def test_plugin_command_without_preload_options_still_runs():
    entered = []

    @click.command("plugin-without-kwargs")
    @click.pass_context
    def plugin_without_kwargs(ctx):
        entered.append(True)

    with _temporary_command(plugin_without_kwargs):
        result = CliRunner().invoke(
            celery,
            ["-A", "t.unit.bin.proj.app", "plugin-without-kwargs"],
            catch_exceptions=False,
        )

    assert result.exit_code == 0, result.output
    assert entered == [True]


def test_command_option_survives_preload_option():
    """Pop only preload names; leave the command's own options in place."""
    entered = []
    received = []

    @click.command("decorated-with-own-option")
    @click.option("--force", is_flag=True)
    @click.pass_context
    @handle_preload_options
    def decorated_with_own_option(ctx, force):
        entered.append(force)

    receiver = _on_preload(received)
    user_preload_options.connect(receiver)
    try:
        with _temporary_command(decorated_with_own_option):
            result = CliRunner().invoke(
                celery,
                [
                    "-A",
                    "t.unit.bin.proj.pyramid_celery_app",
                    "decorated-with-own-option",
                    "--force",
                    "--ini",
                    "some_ini.ini",
                ],
                catch_exceptions=False,
            )
    finally:
        user_preload_options.disconnect(receiver)

    assert result.exit_code == 0, result.output
    assert entered == [True]
    assert len(received) == 1
    assert received[0]["ini"] == "some_ini.ini"


def test_preload_wrapper_is_applied_once():
    """A second CLI invoke must not deliver the same preload options twice."""
    received = []

    @click.command("plugin-without-kwargs")
    @click.pass_context
    def plugin_without_kwargs(ctx):
        return None

    receiver = _on_preload(received)
    user_preload_options.connect(receiver)
    try:
        with _temporary_command(plugin_without_kwargs):
            original_params = {
                name: list(command.params)
                for name, command in celery.commands.items()
            }
            args = [
                "-A",
                "t.unit.bin.proj.pyramid_celery_app",
                "plugin-without-kwargs",
                "--ini",
                "some_ini.ini",
            ]
            first = CliRunner().invoke(celery, args, catch_exceptions=False)
            for name, params in original_params.items():
                celery.commands[name].params[:] = params
            second = CliRunner().invoke(celery, args, catch_exceptions=False)
            depth = _preload_wrapper_depth(
                celery.commands["plugin-without-kwargs"].callback
            )
    finally:
        user_preload_options.disconnect(receiver)

    assert first.exit_code == 0, first.output
    assert second.exit_code == 0, second.output
    assert len(received) == 2
    assert depth == 1


def test_handle_preload_options_ignores_missing_values():
    """A direct call that did not receive a preload kwarg must not KeyError."""
    from t.unit.bin.proj.pyramid_celery_app import app as pyramid_app

    called = []
    received = []

    @handle_preload_options
    def command(ctx):
        called.append(True)

    ctx = SimpleNamespace(obj=SimpleNamespace(app=pyramid_app))
    receiver = _on_preload(received)
    user_preload_options.connect(receiver)
    try:
        command(ctx)
    finally:
        user_preload_options.disconnect(receiver)

    assert called == [True]
    assert received == []


def _preload_wrapper_depth(fun):
    depth = 0
    seen = set()
    while fun is not None and id(fun) not in seen:
        seen.add(id(fun))
        if getattr(fun, "_handles_preload_options", False):
            depth += 1
        fun = getattr(fun, "__wrapped__", None)
    return depth


def test_var_keyword_callback_receives_preload_options_only_via_signal():
    seen = {}
    received = []

    @click.command("plugin-with-kwargs")
    @click.pass_context
    def plugin_with_kwargs(ctx, **kwargs):
        seen.update(kwargs)

    receiver = _on_preload(received)
    user_preload_options.connect(receiver)
    try:
        with _temporary_command(plugin_with_kwargs):
            result = CliRunner().invoke(
                celery,
                [
                    "-A",
                    "t.unit.bin.proj.pyramid_celery_app",
                    "plugin-with-kwargs",
                    "--ini",
                    "some_ini.ini",
                    "--ini-var",
                    "a=b",
                ],
                catch_exceptions=False,
            )
    finally:
        user_preload_options.disconnect(receiver)

    assert result.exit_code == 0, result.output
    assert "ini" not in seen
    assert "ini_var" not in seen
    assert len(received) == 1
    assert received[0]["ini"] == "some_ini.ini"
    assert received[0]["ini_var"] == "a=b"


def test_report_accepts_preload_option():
    result = CliRunner().invoke(
        celery,
        [
            "-A",
            "t.unit.bin.proj.pyramid_celery_app",
            "report",
            "--ini",
            "some_ini.ini",
        ],
        catch_exceptions=False,
    )

    assert result.exit_code == 0, result.output


def test_handle_preload_options_applied_twice_sends_once():
    from t.unit.bin.proj.pyramid_celery_app import app as pyramid_app

    called = []
    received = []

    @handle_preload_options
    @handle_preload_options
    def command(ctx):
        called.append(True)

    ctx = SimpleNamespace(obj=SimpleNamespace(app=pyramid_app))
    receiver = _on_preload(received)
    user_preload_options.connect(receiver)
    try:
        command(ctx, ini="some_ini.ini", ini_var=None)
    finally:
        user_preload_options.disconnect(receiver)

    assert called == [True]
    assert len(received) == 1
    assert received[0]["ini"] == "some_ini.ini"
    assert _preload_wrapper_depth(command) == 1


def test_preload_defaults_flags_and_empty_values_reach_the_signal():
    from t.unit.bin.proj.pyramid_celery_app import app as pyramid_app

    extra = (
        click.Option(("--template",), default="default"),
        click.Option(("--debug-preload",), is_flag=True),
    )
    for option in extra:
        pyramid_app.user_options["preload"].add(option)

    entered = []
    received = []

    @click.command("plugin-without-kwargs")
    @click.pass_context
    def plugin_without_kwargs(ctx):
        entered.append(True)

    receiver = _on_preload(received)
    user_preload_options.connect(receiver)
    try:
        with _temporary_command(plugin_without_kwargs):
            result = CliRunner().invoke(
                celery,
                [
                    "-A",
                    "t.unit.bin.proj.pyramid_celery_app",
                    "plugin-without-kwargs",
                    "--ini",
                    "",
                    "--debug-preload",
                ],
                catch_exceptions=False,
            )
    finally:
        user_preload_options.disconnect(receiver)
        for option in extra:
            pyramid_app.user_options["preload"].discard(option)

    assert result.exit_code == 0, result.output
    assert entered == [True]
    assert len(received) == 1
    assert received[0]["ini"] == ""
    assert received[0]["template"] == "default"
    assert received[0]["debug_preload"] is True
