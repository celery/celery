"""Celery Command Line Interface."""
import click
from click.types import IntParamType, StringParamType

from celery.bin.base import CeleryOption, CeleryDaemonCommand


class Hostname(StringParamType):
    """Hostname option."""

    name = "hostname"


class PrefetchMultiplier(IntParamType):
    """Prefetch multiplier option."""

    name = 'multiplier'


class Concurrency(IntParamType):
    """Concurrency option."""

    name = 'concurrency'


PREFETCH_MULTIPLIER = PrefetchMultiplier()
HOSTNAME = Hostname()
CONCURRENCY = Concurrency()


@click.group()
def celery():
    """Celery command entrypoint."""
    pass


@celery.command(cls=CeleryDaemonCommand)
@click.option('-n',
              '--hostname',
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
              type=click.Choice(('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL', 'FATAL')),
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
def worker(*args, **kwargs):
    """Start worker instance.

    Examples
    --------
    .. code-block:: console
        $ celery worker --app=proj -l info
        $ celery worker -A proj -l info -Q hipri,lopri
        $ celery worker -A proj --concurrency=4
        $ celery worker -A proj --concurrency=1000 -P eventlet
        $ celery worker --autoscale=10,0

    """
    pass


def main():
    """Invoke the celery command.

    Main entrypoint.
    """
    celery()
