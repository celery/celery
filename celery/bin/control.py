from functools import partial

import click
from kombu.utils.json import dumps

from celery.app.control import Inspect, Control
from celery.bin.base import COMMA_SEPARATED_LIST, CeleryCommand, CeleryOption
from celery.platforms import EX_UNAVAILABLE
from celery.utils import text


def say_remote_command_reply(ctx, replies, show_reply=False):
    node = next(iter(replies))  # <-- take first.
    reply = replies[node]
    node = ctx.obj.style(f'{node}: ', fg='cyan', bold=True)
    status, preply = ctx.obj.pretty(reply)
    ctx.obj.say_chat('->', f'{node}{status}',
                     text.indent(preply, 4) if show_reply else '',
                     show_body=show_reply)


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

    callback = None if json else partial(say_remote_command_reply, ctx)
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


@click.command(cls=CeleryCommand)
@click.argument("action", type=click.Choice(
    [choice for choice in dir(Inspect) if not choice.startswith('_')]))
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
def inspect(ctx, action, timeout, destination, json, **kwargs):
    """Inspect the worker at runtime.

    Availability: RabbitMQ (AMQP) and Redis transports."""
    callback = None if json else partial(say_remote_command_reply, ctx,
                                         show_reply=True)
    replies = ctx.obj.app.control.inspect(timeout=timeout,
                                          destination=destination,
                                          callback=callback)._request(action)

    if not replies:
        ctx.obj.echo('No nodes replied within time constraint')
        return EX_UNAVAILABLE

    if json:
        ctx.obj.echo(dumps(replies))
    nodecount = len(replies)
    if not ctx.obj.quiet:
        ctx.obj.echo('\n{0} {1} online.'.format(
            nodecount, text.pluralize(nodecount, 'node')))


@click.command(cls=CeleryCommand)
@click.argument("action", type=click.Choice([
    choice for choice in dir(Control)
    if (choice not in ('inspect', 'broadcast')
        and choice.islower()
        and not choice.startswith('_'))
]))
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
def control(ctx, action, timeout, destination, json, **kwargs):
    """Workers remote control.

    Availability: RabbitMQ (AMQP), Redis, and MongoDB transports."""
    callback = None if json else partial(say_remote_command_reply, ctx,
                                         show_reply=True)
    replies = ctx.obj.app.control.broadcast(action, timeout=timeout,
                                            destination=destination,
                                            callback=callback,
                                            reply=True)

    if not replies:
        ctx.obj.echo('No nodes replied within time constraint')
        return EX_UNAVAILABLE

    if json:
        ctx.obj.echo(dumps(replies))
