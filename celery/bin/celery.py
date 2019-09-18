"""Celery Command Line Interface."""
import json
import os
from functools import partial

import click
from click.types import IntParamType, ParamType, StringParamType

from celery import VERSION_BANNER, concurrency
from celery._state import get_current_app
from celery.app.utils import find_app
from celery.bin.base import CeleryCommand, CeleryDaemonCommand, CeleryOption
from celery.platforms import maybe_drop_privileges
from celery.utils import text
from celery.utils.log import mlevel
from celery.utils.nodenames import default_nodename, host_format, node_format
from celery.utils.time import maybe_iso8601


class App(ParamType):
    """Application option."""

    name = "application"

    def convert(self, value, param, ctx):
        try:
            return find_app(value)
        except (ModuleNotFoundError, AttributeError) as e:
            self.fail(str(e))


class LogLevel(click.Choice):
    """Log level option."""

    def __init__(self):
        """Initialize the log level option with the relevant choices."""
        super().__init__(('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL', 'FATAL'))

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


class CeleryBeat(ParamType):
    """Celery Beat flag."""

    name = "beat"

    def convert(self, value, param, ctx):
        if ctx.obj['app'].IS_WINDOWS and value:
            self.fail('-B option does not work on Windows.  '
                      'Please run celery beat as a separate service.')

        return value


class WorkersPool(click.Choice):
    """Workers pool option."""

    name = "pool"

    def __init__(self):
        """Initialize the workers pool option with the relevant choices."""
        super().__init__(('prefork', 'eventlet', 'gevent', 'solo'))

    def convert(self, value, param, ctx):
        # Pools like eventlet/gevent needs to patch libs as early
        # as possible.
        return concurrency.get_implementation(value) or ctx.obj['app'].conf.worker_pool


class Json(ParamType):
    """JSON formatted argument."""

    name = "json"

    def convert(self, value, param, ctx):
        try:
            return json.loads(value)
        except ValueError as e:
            self.fail(str(e))


class ISO8601DateTime(ParamType):
    """ISO 8601 Date Time argument."""

    name = "iso-86091"

    def convert(self, value, param, ctx):
        try:
            return maybe_iso8601(value)
        except (TypeError, ValueError) as e:
            self.fail(e)


class ISO8601DateTimeOrFloat(ParamType):
    """ISO 8601 Date Time or float argument."""

    name = "iso-86091 or float"

    def convert(self, value, param, ctx):
        try:
            return float(value)
        except (TypeError, ValueError):
            pass

        try:
            return maybe_iso8601(value)
        except (TypeError, ValueError) as e:
            self.fail(e)


class CommaSeperatedList(ParamType):
    """Comma seperated list argument."""

    name = "comma seperated list"

    def convert(self, value, param, ctx):
        return set(text.str_to_list(value))


PREFETCH_MULTIPLIER = PrefetchMultiplier()
HOSTNAME = Hostname()
CONCURRENCY = Concurrency()
APP = App()
CELERY_BEAT = CeleryBeat()
LOG_LEVEL = LogLevel()
WORKERS_POOL = WorkersPool()
JSON = Json()
ISO8601 = ISO8601DateTime()
ISO8601_OR_FLOAT = ISO8601DateTimeOrFloat()
COMMA_SEPERATED_LIST = CommaSeperatedList()


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


@celery.command(cls=CeleryDaemonCommand, context_settings={'allow_extra_args': True})
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
              type=LOG_LEVEL,
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
              type=WORKERS_POOL,
              cls=CeleryOption,
              help_group="Pool Options",
              help="Pool implementation.")
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
              type=CELERY_BEAT,
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
    if ctx.args:
        try:
            app.config_from_cmdline(ctx.args, namespace='worker')
        except (KeyError, ValueError) as e:
            # TODO: Improve the error messages
            raise click.UsageError("Unable to parse extra configuration from command line.\n"
                                   f"Reason: {e}", ctx=ctx)
    maybe_drop_privileges(uid=uid, gid=gid)
    worker = app.Worker(
        hostname=hostname, pool_cls=pool_cls, loglevel=loglevel,
        logfile=logfile,  # node format handled by celery.app.log.setup
        pidfile=node_format(pidfile, hostname),
        statedb=node_format(statedb, hostname),
        **kwargs)
    worker.start()
    return worker.exitcode


@celery.command(cls=CeleryDaemonCommand, context_settings={'allow_extra_args': True})
@click.option('--detach',
              cls=CeleryOption,
              is_flag=True,
              default=False,
              help_group="Beat Options",
              help="Detach and run in the background as a daemon.")
@click.option('-s',
              '--schedule',
              cls=CeleryOption,
              help_group="Beat Options",
              help="Path to the schedule database.  Defaults to `celerybeat-schedule`."
                   "The extension '.db' may be appended to the filename."
                   "Default is {default}.")
@click.option('-S',
              '--scheduler',
              cls=CeleryOption,
              help_group="Beat Options",
              help="Scheduler class to use."
                   "Default is {default}.")
@click.option('--max-interval',
              cls=CeleryOption,
              type=int,
              help_group="Beat Options",
              help="Scheduler class to use."
                   "Default is {default}.")
@click.option('-l',
              '--loglevel',
              default='WARNING',
              cls=CeleryOption,
              type=LOG_LEVEL,
              help_group="Beat Options",
              help="Logging level.")
@click.pass_context
def beat(ctx, detach=False, logfile=None, pidfile=None, uid=None,
         gid=None, umask=None, workdir=None, **kwargs):
    """Start the beat periodic task scheduler."""
    app = ctx.obj['app']

    if ctx.args:
        try:
            app.config_from_cmdline(ctx.args)
        except (KeyError, ValueError) as e:
            # TODO: Improve the error messages
            raise click.UsageError("Unable to parse extra configuration from command line.\n"
                                   f"Reason: {e}", ctx=ctx)

    if not detach:
        maybe_drop_privileges(uid=uid, gid=gid)

    beat = partial(app.Beat,
                   logfile=logfile, pidfile=pidfile, **kwargs)

    if detach:
        # TODO: Implement this
        pass
    else:
        return beat().run()


@click.argument('name')
@click.option('-a',
              '--args',
              cls=CeleryOption,
              type=JSON,
              default='[]',
              help_group="Calling Options",
              help="Positional arguments.")
@click.option('-k',
              '--kwargs',
              cls=CeleryOption,
              type=JSON,
              default='{}',
              help_group="Calling Options",
              help="Keyword arguments.")
@click.option('--eta',
              cls=CeleryOption,
              type=ISO8601,
              help_group="Calling Options",
              help="scheduled time.")
@click.option('--countdown',
              cls=CeleryOption,
              type=float,
              help_group="Calling Options",
              help="eta in seconds from now.")
@click.option('--expires',
              cls=CeleryOption,
              type=ISO8601_OR_FLOAT,
              help_group="Calling Options",
              help="expiry time.")
@click.option('--serializer',
              cls=CeleryOption,
              default='json',
              help_group="Calling Options",
              help="task serializer.")
@click.option('--queue',
              cls=CeleryOption,
              help_group="Routing Options",
              help="custom queue name.")
@click.option('--exchange',
              cls=CeleryOption,
              help_group="Routing Options",
              help="custom exchange name.")
@click.option('--routing-key',
              cls=CeleryOption,
              help_group="Routing Options",
              help="custom routing key.")
@celery.command(cls=CeleryCommand)
@click.pass_context
def call(ctx, name, args, kwargs, eta, countdown, expires, serializer, queue, exchange, routing_key):
    """Call a task by name."""
    task_id = ctx.obj['app'].send_task(
        name,
        args=args, kwargs=kwargs,
        countdown=countdown,
        serializer=serializer,
        queue=queue,
        exchange=exchange,
        routing_key=routing_key,
        eta=eta,
        expires=expires
    ).id
    click.echo(task_id)


@celery.command(cls=CeleryCommand)
@click.option('-f',
              '--force',
              cls=CeleryOption,
              is_flag=True,
              help_group='Purging Options',
              help="Don't prompt for verification.")
@click.option('-Q',
              '--queues',
              cls=CeleryOption,
              type=COMMA_SEPERATED_LIST,
              help_group='Purging Options',
              help="Comma separated list of queue names to purge.")
@click.option('-X',
              '--exclude-queues',
              cls=CeleryOption,
              type=COMMA_SEPERATED_LIST,
              help_group='Purging Options',
              help="Comma separated list of queues names not to purge.")
@click.pass_context
def purge(ctx, force, queues, exclude_queues):
    """Erase all messages from all known task queues.

    Warning:

        There's no undo operation for this command.
    """
    queues = queues or set()
    exclude_queues = exclude_queues or set()
    app = ctx.obj['app']
    names = (queues or set(app.amqp.queues.keys())) - exclude_queues
    qnum = len(names)

    if names:
        if not force:
            click.confirm(f"{click.style('WARNING', fg='red')}: This will remove all tasks from {text.pluralize(qnum, 'queue')}: {', '.join(sorted(names))}.\n"
                          "         There is no undo for this operation!\n\n"
                          "(to skip this prompt use the -f option)\n"
                          "Are you sure you want to delete all tasks?", abort=True)

        def _purge(conn, queue):
            try:
                return conn.default_channel.queue_purge(queue) or 0
            except conn.channel_errors:
                return 0

        with app.connection_for_write() as conn:
            messages = sum(_purge(conn, queue) for queue in names)

        if messages:
            click.echo(f"Purged {messages} {text.pluralize(messages, 'message')} from {qnum} known task {text.pluralize(qnum, 'queue')}.")
        else:
            click.echo(f"No messages purged from {qnum} {text.pluralize(qnum, 'queue')}.")


@celery.group(name="list")
def list_():
    """Get info from broker.

    Note:

        For RabbitMQ the management plugin is required.
    """


@list_.command(cls=CeleryCommand)
@click.pass_context
def bindings(ctx):
    """Inspect queue bindings."""

    # TODO: Consider using a table formatter for this command.
    app = ctx.obj['app']
    with app.connection() as conn:
        app.amqp.TaskConsumer(conn).declare()

        try:
            bindings = conn.manager.get_bindings()
        except NotImplementedError:
            raise click.UsageError('Your transport cannot list bindings.')

        def fmt(q, e, r):
            click.echo('{0:<28} {1:<28} {2}'.format(q, e, r))
        fmt('Queue', 'Exchange', 'Routing Key')
        fmt('-' * 16, '-' * 16, '-' * 16)
        for b in bindings:
            fmt(b['destination'], b['source'], b['routing_key'])


def main() -> int:
    """Start celery umbrella command.

    This function is the main entrypoint for the CLI.

    :return: The exit code of the CLI.
    """
    return celery(auto_envvar_prefix="CELERY")
