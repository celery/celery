"""The ``celery control``, ``. inspect`` and ``. status`` programs."""
from functools import partial
from typing import Literal

import click
from kombu.utils.json import dumps

from celery.bin.base import COMMA_SEPARATED_LIST, CeleryCommand, CeleryOption, handle_preload_options
from celery.exceptions import CeleryCommandException
from celery.platforms import EX_UNAVAILABLE
from celery.utils import text
from celery.worker.control import Panel


def _say_remote_command_reply(ctx, replies, show_reply=False):
    node = next(iter(replies))  # <-- take first.
    reply = replies[node]
    node = ctx.obj.style(f'{node}: ', fg='cyan', bold=True)
    status, preply = ctx.obj.pretty(reply)
    ctx.obj.say_chat('->', f'{node}{status}',
                     text.indent(preply, 4) if show_reply else '',
                     show_body=show_reply)


def _consume_arguments(meta, method, args):
    i = 0
    try:
        for i, arg in enumerate(args):
            try:
                name, typ = meta.args[i]
            except IndexError:
                if meta.variadic:
                    break
                raise click.UsageError(
                    'Command {!r} takes arguments: {}'.format(
                        method, meta.signature))
            else:
                yield name, typ(arg) if typ is not None else arg
    finally:
        args[:] = args[i:]


def _compile_arguments(command, args):
    meta = Panel.meta[command]
    arguments = {}
    if meta.args:
        arguments.update({
            k: v for k, v in _consume_arguments(meta, command, args)
        })
    if meta.variadic:
        arguments.update({meta.variadic: args})
    return arguments


_RemoteControlType = Literal['inspect', 'control']


def _verify_command_name(type_: _RemoteControlType, command: str) -> None:
    choices = _get_commands_of_type(type_)

    if command not in choices:
        command_listing = ", ".join(choices)
        raise click.UsageError(
            message=f'Command {command} not recognized. Available {type_} commands: {command_listing}',
        )


def _list_option(type_: _RemoteControlType):
    def callback(ctx: click.Context, param, value) -> None:
        if not value:
            return
        choices = _get_commands_of_type(type_)

        formatter = click.HelpFormatter()

        with formatter.section(f'{type_.capitalize()} Commands'):
            command_list = []
            for command_name, info in choices.items():
                if info.signature:
                    command_preview = f'{command_name} {info.signature}'
                else:
                    command_preview = command_name
                command_list.append((command_preview, info.help))
            formatter.write_dl(command_list)
        ctx.obj.echo(formatter.getvalue(), nl=False)
        ctx.exit()

    return click.option(
        '--list',
        is_flag=True,
        help=f'List available {type_} commands and exit.',
        expose_value=False,
        is_eager=True,
        callback=callback,
    )


def _get_commands_of_type(type_: _RemoteControlType) -> dict:
    command_name_info_pairs = [
        (name, info) for name, info in Panel.meta.items()
        if info.type == type_ and info.visible
    ]
    return dict(sorted(command_name_info_pairs))


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
@handle_preload_options
def status(ctx, timeout, destination, json, **kwargs):
    """Show list of workers that are online."""
    callback = None if json else partial(_say_remote_command_reply, ctx)
    replies = ctx.obj.app.control.inspect(timeout=timeout,
                                          destination=destination,
                                          callback=callback).ping()

    if not replies:
        raise CeleryCommandException(
            message='No nodes replied within time constraint',
            exit_code=EX_UNAVAILABLE
        )

    if json:
        ctx.obj.echo(dumps(replies))
    nodecount = len(replies)
    if not kwargs.get('quiet', False):
        ctx.obj.echo('\n{} {} online.'.format(
            nodecount, text.pluralize(nodecount, 'node')))


@click.command(cls=CeleryCommand,
               context_settings={'allow_extra_args': True})
@click.argument('command')
@_list_option('inspect')
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
@handle_preload_options
def inspect(ctx, command, timeout, destination, json, **kwargs):
    """Inspect the workers by sending them the COMMAND inspect command.

    Availability: RabbitMQ (AMQP) and Redis transports.
    """
    _verify_command_name('inspect', command)
    callback = None if json else partial(_say_remote_command_reply, ctx,
                                         show_reply=True)
    arguments = _compile_arguments(command, ctx.args)
    inspect = ctx.obj.app.control.inspect(timeout=timeout,
                                          destination=destination,
                                          callback=callback)
    replies = inspect._request(command, **arguments)

    if not replies:
        raise CeleryCommandException(
            message='No nodes replied within time constraint',
            exit_code=EX_UNAVAILABLE
        )

    if json:
        ctx.obj.echo(dumps(replies))
        return

    nodecount = len(replies)
    if not ctx.obj.quiet:
        ctx.obj.echo('\n{} {} online.'.format(
            nodecount, text.pluralize(nodecount, 'node')))


@click.command(cls=CeleryCommand,
               context_settings={'allow_extra_args': True})
@click.argument('command')
@_list_option('control')
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
@handle_preload_options
def control(ctx, command, timeout, destination, json):
    """Send the COMMAND control command to the workers.

    Availability: RabbitMQ (AMQP), Redis, and MongoDB transports.
    """
    _verify_command_name('control', command)
    callback = None if json else partial(_say_remote_command_reply, ctx,
                                         show_reply=True)
    args = ctx.args
    arguments = _compile_arguments(command, args)
    replies = ctx.obj.app.control.broadcast(command, timeout=timeout,
                                            destination=destination,
                                            callback=callback,
                                            reply=True,
                                            arguments=arguments)

    if not replies:
        raise CeleryCommandException(
            message='No nodes replied within time constraint',
            exit_code=EX_UNAVAILABLE
        )

    if json:
        ctx.obj.echo(dumps(replies))
