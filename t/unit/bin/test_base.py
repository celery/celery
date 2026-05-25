"""Tests for celery.bin.base — specifically the plugin loading utilities."""
import click
import pytest
from unittest.mock import MagicMock

from celery.bin.base import BrokenCommand, with_plugins


class test_with_plugins:
    def test_registers_valid_plugin(self):
        @click.command()
        def my_plugin():
            """A test plugin."""

        ep = MagicMock()
        ep.name = "my_plugin"
        ep.load.return_value = my_plugin

        @with_plugins([ep])
        @click.group()
        def cli():
            pass

        assert "my-plugin" in cli.commands

    def test_broken_plugin_registers_broken_command(self):
        ep = MagicMock()
        ep.name = "broken_plugin"
        ep.load.side_effect = Exception("plugin load failed")

        @with_plugins([ep])
        @click.group()
        def cli():
            pass

        assert "broken_plugin" in cli.commands
        assert isinstance(cli.commands["broken_plugin"], BrokenCommand)

    def test_none_plugins_is_handled(self):
        @with_plugins(None)
        @click.group()
        def cli():
            pass

        assert cli.commands == {}

    def test_empty_plugins_is_handled(self):
        @with_plugins([])
        @click.group()
        def cli():
            pass

        assert cli.commands == {}


class test_broken_command:
    def test_short_help_contains_warning(self):
        cmd = BrokenCommand("bad_plugin")
        assert "Warning" in cmd.short_help

    def test_invoke_exits_with_error(self):
        from click.testing import CliRunner

        @click.group()
        def cli():
            pass

        cli.add_command(BrokenCommand("bad_plugin"))
        runner = CliRunner()
        result = runner.invoke(cli, ["bad_plugin"])
        assert result.exit_code == 1