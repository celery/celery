"""Celery Command Line Interface."""
import os

import click
from click.types import ParamType
from click_didyoumean import DYMGroup

from celery import VERSION_BANNER
from celery.app.utils import find_app
from celery.bin.amqp import amqp
from celery.bin.base import CeleryCommand, CeleryOption, CLIContext
from celery.bin.beat import beat
from celery.bin.call import call
from celery.bin.control import control, inspect, status
from celery.bin.events import events
from celery.bin.graph import graph
from celery.bin.list import list_
from celery.bin.logtool import logtool
from celery.bin.migrate import migrate
from celery.bin.multi import multi
from celery.bin.purge import purge
from celery.bin.result import result
from celery.bin.shell import shell
from celery.bin.upgrade import upgrade
from celery.bin.worker import worker


class App(ParamType):
    """Application option."""

    name = "application"

    def convert(self, value, param, ctx):
        try:
            return find_app(value)
        except (ModuleNotFoundError, AttributeError) as e:
            self.fail(str(e))


APP = App()


@click.group(cls=DYMGroup, invoke_without_command=True)
@click.option('-A',
              '--app',
              envvar='APP',
              cls=CeleryOption,
              type=APP,
              help_group="Global Options")
@click.option('-b',
              '--broker',
              envvar='BROKER_URL',
              cls=CeleryOption,
              help_group="Global Options")
@click.option('--result-backend',
              envvar='RESULT_BACKEND',
              cls=CeleryOption,
              help_group="Global Options")
@click.option('--loader',
              envvar='LOADER',
              cls=CeleryOption,
              help_group="Global Options")
@click.option('--config',
              envvar='CONFIG_MODULE',
              cls=CeleryOption,
              help_group="Global Options")
@click.option('--workdir',
              cls=CeleryOption,
              help_group="Global Options")
@click.option('-C',
              '--no-color',
              envvar='NO_COLOR',
              is_flag=True,
              cls=CeleryOption,
              help_group="Global Options")
@click.option('-q',
              '--quiet',
              is_flag=True,
              cls=CeleryOption,
              help_group="Global Options")
@click.option('--version',
              cls=CeleryOption,
              is_flag=True,
              help_group="Global Options")
@click.pass_context
def celery(ctx, app, broker, result_backend, loader, config, workdir,
           no_color, quiet, version):
    """Celery command entrypoint."""
    if version:
        click.echo(VERSION_BANNER)
        ctx.exit()
    elif ctx.invoked_subcommand is None:
        click.echo(ctx.get_help())
        ctx.exit()

    if workdir:
        os.chdir(workdir)
    if loader:
        # Default app takes loader from this env (Issue #1066).
        os.environ['CELERY_LOADER'] = loader
    if broker:
        os.environ['CELERY_BROKER_URL'] = broker
    if result_backend:
        os.environ['CELERY_RESULT_BACKEND'] = result_backend
    if config:
        os.environ['CELERY_CONFIG_MODULE'] = config
    ctx.obj = CLIContext(app=app, no_color=no_color, workdir=workdir, quiet=quiet)

    # User options
    worker.params.extend(ctx.obj.app.user_options.get('worker', []))
    beat.params.extend(ctx.obj.app.user_options.get('beat', []))
    events.params.extend(ctx.obj.app.user_options.get('events', []))


@celery.command(cls=CeleryCommand)
@click.pass_context
def report(ctx):
    """Shows information useful to include in bug-reports."""
    app = ctx.obj.app
    app.loader.import_default_modules()
    ctx.obj.echo(app.bugreport())


celery.add_command(purge)
celery.add_command(call)
celery.add_command(beat)
celery.add_command(list_)
celery.add_command(result)
celery.add_command(migrate)
celery.add_command(status)
celery.add_command(worker)
celery.add_command(events)
celery.add_command(inspect)
celery.add_command(control)
celery.add_command(graph)
celery.add_command(upgrade)
celery.add_command(logtool)
celery.add_command(amqp)
celery.add_command(shell)
celery.add_command(multi)


def main() -> int:
    """Start celery umbrella command.

    This function is the main entrypoint for the CLI.

    :return: The exit code of the CLI.
    """
    return celery(auto_envvar_prefix="CELERY")
