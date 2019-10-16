import click
from kombu.utils.json import dumps

from celery.bin.base import CeleryCommand, CeleryOption, COMMA_SEPARATED_LIST
from celery.platforms import EX_UNAVAILABLE
from celery.utils import text


@click.command(cls=CeleryCommand)
@click.option('-t',
              '--timeout',
              cls=CeleryOption,
              type=float,
              default=1.0,
              help_group='Remote Control Options',
              help='Timeout in seconds waiting for reply.')
@click.option('-d',
              '--destination',
              cls=CeleryOption,
              type=COMMA_SEPARATED_LIST,
              help_group='Remote Control Options',
              help='Comma separated list of destination node names.')
@click.option('-j',
              '--json',
              cls=CeleryOption,
              is_flag=True,
              help_group='Remote Control Options',
              help='Use json as output format.')
@click.pass_context
def status(ctx, timeout, destination, json, **kwargs):
    """Show list of workers that are online."""
    def say_remote_command_reply(replies):
        node = next(iter(replies))  # <-- take first.
        node = click.style(f'{node}: ', fg='cyan')
        ctx.obj.secho(f'{node}{ctx.obj.OK}', bold=True)

    callback = None if json else say_remote_command_reply
    replies = ctx.obj.app.control.inspect(timeout=timeout,
                                          destination=destination,
                                          callback=callback).ping()

    if not replies:
        ctx.obj.echo('No nodes replied within time constraint')
        return EX_UNAVAILABLE

    if json:
        ctx.obj.echo(dumps(replies))
    nodecount = len(replies)
    if not kwargs.get('quiet', False):
        ctx.obj.echo('\n{0} {1} online.'.format(
            nodecount, text.pluralize(nodecount, 'node')))
