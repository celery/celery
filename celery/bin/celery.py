"""Celery Command Line Interface."""
import os
import sys

import click
from click.types import IntParamType, ParamType, StringParamType

from celery import VERSION_BANNER, concurrency
from celery._state import get_current_app
from celery.app.utils import find_app
from celery.bin.base import CeleryDaemonCommand, CeleryOption
from celery.platforms import maybe_drop_privileges
from celery.utils.log import mlevel
from celery.utils.nodenames import default_nodename, host_format, node_format


class App(ParamType):
    """Application option."""

    name = "application"

    def convert(self, value, param, ctx):
        try:
            return find_app(value)
        except (ModuleNotFoundError, AttributeError) as e:
            raise click.BadParameter(str(e))


class LogLevel(click.Choice):
    """Log level option."""

    def convert(self, value, param, ctx):
        value = super().convert(value, param, ctx)
        return mlevel(value)


class Hostname(StringParamType):
    """Hostname option."""

    name = "hostname"

    def convert(self, value, param, ctx):
        return host_format(default_nodename(value))


class PrefetchMultiplier(IntParamType):
    """Prefetch multiplier option."""

    name = 'multiplier'


class Concurrency(IntParamType):
    """Concurrency option."""

    name = 'concurrency'


PREFETCH_MULTIPLIER = PrefetchMultiplier()
HOSTNAME = Hostname()
CONCURRENCY = Concurrency()
APP = App()


@click.group(invoke_without_command=True)
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
def celery(ctx, app, broker, result_backend, loader, config, workdir, no_color, quiet, version):
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
    ctx.ensure_object(dict)
    ctx.obj['app'] = app or get_current_app()


@celery.command(cls=CeleryDaemonCommand)
@click.option('-n',
              '--hostname',
              default=host_format(default_nodename(None)),
              cls=CeleryOption,
              type=HOSTNAME,
              help_group="Worker Options",
              help="Set custom hostname (e.g., 'w1@%%h').  Expands: %%h (hostname), %%n (name) and %%d, (domain).")
@click.option('-D',
              '--detach',
              cls=CeleryOption,
              is_flag=True,
              default=False,
              help_group="Worker Options",
              help="Start worker as a background process.")
@click.option('-S',
              '--statedb',
              cls=CeleryOption,
              type=click.Path(),
              help_group="Worker Options",
              help="Path to the state database. The extension '.db' may be appended to the filename.  Default: {default}")  # TODO: Load default from the app in the context
@click.option('-l',
              '--loglevel',
              default='WARNING',
              cls=CeleryOption,
              type=LogLevel(('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL', 'FATAL')),
              help_group="Worker Options",
              help="Logging level.")
@click.option('optimization',
              '-O',
              default='default',
              cls=CeleryOption,
              type=click.Choice(('default', 'fair')),
              help_group="Worker Options",
              help="Apply optimization profile.")
@click.option('--prefetch-multiplier',
              default=0,
              type=PREFETCH_MULTIPLIER,
              cls=CeleryOption,
              help_group="Worker Options",
              help="Set custom prefetch multiplier value for this worker instance.")  # TODO: Load default from the app in the context
@click.option('-c',
              '--concurrency',
              type=CONCURRENCY,
              cls=CeleryOption,
              help_group="Pool Options",
              help="Number of child processes processing the queue.  The default is the number of CPUs available on your system.")  # TODO: Load default from the app in the context
@click.option('-P',
              '--pool',
              default='prefork',
              type=click.Choice(('prefork', 'eventlet', 'gevent', 'solo')),
              cls=CeleryOption,
              help_group="Pool Options",
              help="Number of child processes processing the queue.  The default is the number of CPUs available on your system.")  # TODO: Load default from the app in the context
@click.option( '-E',
              '--task-events',
              '--events',
              is_flag=True,
              cls=CeleryOption,
              help_group="Pool Options",
              help="Send task-related events that can be captured by monitors like celery events, celerymon, and others.")
@click.option('--time-limit',
              type=float,
              cls=CeleryOption,
              help_group="Pool Options",
              help="Enables a hard time limit (in seconds int/float) for tasks.")
@click.option('--soft-time-limit',
              type=float,
              cls=CeleryOption,
              help_group="Pool Options",
              help="Enables a soft time limit (in seconds int/float) for tasks.")
@click.option('--max-tasks-per-child',
              type=int,
              cls=CeleryOption,
              help_group="Pool Options",
              help="Maximum number of tasks a pool worker can execute before it's terminated and replaced by a new worker.")
@click.option('--max-memory-per-child',
              type=int,
              cls=CeleryOption,
              help_group="Pool Options",
              help="""Maximum amount of resident memory, in KiB, that may be consumed by a
                   child process before it will be replaced by a new one.  If a single
                   task causes a child process to exceed this limit, the task will be
                   completed and the child process will be replaced afterwards.
                   Default: no limit.""")
@click.option('--purge',
              '--discard',
              is_flag=True,
              cls=CeleryOption,
              help_group="Queue Options",)
@click.option('--queues',
              '-Q',
              multiple=True,
              cls=CeleryOption,
              help_group="Queue Options",)
@click.option('--exclude-queues',
              '-X',
              is_flag=True,
              cls=CeleryOption,
              help_group="Queue Options",)
@click.option('--include',
              '-I',
              multiple=True,
              cls=CeleryOption,
              help_group="Queue Options",)
@click.option('--without-gossip',
              default=False,
              cls=CeleryOption,
              help_group="Features",)
@click.option('--without-mingle',
              default=False,
              cls=CeleryOption,
              help_group="Features",)
@click.option('--without-heartbeat',
              default=False,
              cls=CeleryOption,
              help_group="Features",)
@click.option('--heartbeat-interval',
              type=int,
              cls=CeleryOption,
              help_group="Features",)
@click.option('--autoscale',
              type=str,  # TODO: Parse this
              cls=CeleryOption,
              help_group="Features",)
@click.option('-B',
              '--beat',
              cls=CeleryOption,
              is_flag=True,
              help_group="Embedded Beat Options")
@click.option('-s',
              '--schedule-filename',
              '--schedule',
              cls=CeleryOption,
              help_group="Embedded Beat Options")  # TODO: Load default from the app in the context
@click.option('--scheduler',
              cls=CeleryOption,
              help_group="Embedded Beat Options")
@click.pass_context
def worker(ctx, hostname=None, pool_cls=None, app=None, uid=None, gid=None,
           loglevel=None, logfile=None, pidfile=None, statedb=None,
           **kwargs):
    """Start worker instance.

    Examples
    --------
    $ celery worker --app=proj -l info
    $ celery worker -A proj -l info -Q hipri,lopri
    $ celery worker -A proj --concurrency=4
    $ celery worker -A proj --concurrency=1000 -P eventlet
    $ celery worker --autoscale=10,0

    """
    app = ctx.obj['app']
    maybe_drop_privileges(uid=uid, gid=gid)
    # Pools like eventlet/gevent needs to patch libs as early
    # as possible.
    pool_cls = (concurrency.get_implementation(pool_cls) or
                app.conf.worker_pool)
    # TODO: Move this check to a param type
    if app.IS_WINDOWS and kwargs.get('beat'):
        raise click.BadParameter('-B option does not work on Windows.  '
                                 'Please run celery beat as a separate service.',
                                 ctx=ctx,
                                 param='-B')
    worker = app.Worker(
        hostname=hostname, pool_cls=pool_cls, loglevel=loglevel,
        logfile=logfile,  # node format handled by celery.app.log.setup
        pidfile=node_format(pidfile, hostname),
        statedb=node_format(statedb, hostname),
        **kwargs)
    worker.start()
    return worker.exitcode


def main():
    """Invoke the celery command.

    Main entrypoint.
    """
    sys.exit(celery(auto_envvar_prefix="CELERY"))
