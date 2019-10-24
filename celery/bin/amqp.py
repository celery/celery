import pprint
import sys
from functools import partial

import click
from amqp import Connection, Message
from click_repl import register_repl

from celery.utils.functional import padlist
from celery.utils.serialization import strtobool

__all__ = ('Spec', 'amqp')

# Map to coerce strings to other types.
COERCE = {bool: strtobool}

HELP_HEADER = """
Commands
--------
""".rstrip()

EXAMPLE_TEXT = """
Example:
    -> queue.delete myqueue yes no
"""

say = partial(print, file=sys.stderr)


class Spec(object):
    """AMQP Command specification.
    Used to convert arguments to Python values and display various help
    and tool-tips.
    Arguments:
        args (Sequence): see :attr:`args`.
        returns (str): see :attr:`returns`.
    """

    #: List of arguments this command takes.
    #: Should contain ``(argument_name, argument_type)`` tuples.
    args = None

    #: Helpful human string representation of what this command returns.
    #: May be :const:`None`, to signify the return type is unknown.
    returns = None

    def __init__(self, *args, **kwargs):
        self.args = args
        self.returns = kwargs.get('returns')

    def coerce(self, index, value):
        """Coerce value for argument at index."""
        arg_info = self.args[index]
        arg_type = arg_info[1]
        # Might be a custom way to coerce the string value,
        # so look in the coercion map.
        return COERCE.get(arg_type, arg_type)(value)

    def str_args_to_python(self, arglist):
        """Process list of string arguments to values according to spec.
        Example:
            >>> spec = Spec([('queue', str), ('if_unused', bool)])
            >>> spec.str_args_to_python('pobox', 'true')
            ('pobox', True)
        """
        return tuple(
            self.coerce(index, value) for index, value in enumerate(arglist))

    def format_response(self, response):
        """Format the return value of this command in a human-friendly way."""
        if not self.returns:
            return 'ok.' if response is None else response
        if callable(self.returns):
            return self.returns(response)
        return self.returns.format(response)

    def format_arg(self, name, type, default_value=None):
        if default_value is not None:
            return '{0}:{1}'.format(name, default_value)
        return name

    def format_signature(self):
        return ' '.join(self.format_arg(*padlist(list(arg), 3))
                        for arg in self.args)


def dump_message(message):
    if message is None:
        return 'No messages in queue. basic.publish something.'
    return {'body': message.body,
            'properties': message.properties,
            'delivery_info': message.delivery_info}


def format_declare_queue(ret):
    return 'ok. queue:{0} messages:{1} consumers:{2}.'.format(*ret)


class AMQPContext:
    def __init__(self, cli_context):
        self.cli_context = cli_context
        self.connection = self.cli_context.app.connection()
        self.cli_context.echo(f'-> connecting to {self.connection.as_uri()}.')
        self.connection.connect()
        self.cli_context.echo('-> connected.')
        self.channel = self.connection.default_channel

    def respond(self, retval):
        if isinstance(retval, str):
            self.cli_context.echo(retval)
        else:
            self.cli_context.echo(pprint.pprint(retval))


@click.group(invoke_without_command=True)
@click.pass_context
def amqp(ctx):
    """AMQP Administration Shell.

    Also works for non-AMQP transports (but not ones that
    store declarations in memory).
    """
    if not isinstance(ctx.obj, AMQPContext):
        ctx.obj = AMQPContext(ctx.obj)


@amqp.command(name='exchange.declare')
def exchange_declare():
    pass


@amqp.command(name='exchange.delete')
def exchange_delete():
    pass


@amqp.command(name='queue.bind')
def queue_bind():
    pass


@amqp.command(name='queue.declare')
def queue_declare():
    pass


@amqp.command(name='queue.delete')
def queue_delete():
    pass


@amqp.command(name='queue.purge')
def queue_delete():
    pass


@amqp.command(name='basic.get')
@click.argument('queue',
                type=str)
@click.argument('no_ack',
                type=bool,
                default=False)
@click.pass_obj
def basic_get(amqp_context, queue, no_ack):
    amqp_context.respond(dump_message(amqp_context.channel.basic_get(queue, no_ack=no_ack)))


@amqp.command(name='basic.publish')
@click.argument('msg',
                type=str)
@click.argument('exchange',
                type=str)
@click.argument('routing_key',
                type=str)
@click.argument('mandatory',
                type=bool,
                default=False)
@click.argument('immediate',
                type=bool,
                default=False)
@click.pass_obj
def basic_publish(amqp_context, msg, exchange, routing_key, mandatory, immediate):
    # XXX Hack to fix Issue #2013
    if isinstance(amqp_context.connection.connection, Connection):
        msg = Message(msg)
    amqp_context.channel.basic_publish(msg,
                                       exchange=exchange,
                                       routing_key=routing_key,
                                       mandatory=mandatory,
                                       immediate=immediate)
    amqp_context.cli_context.echo(amqp_context.cli_context.OK)


@amqp.command(name='basic.ack')
@click.argument('delivery_tag',
                type=int)
@click.pass_obj
def basic_ack(amqp_context, delivery_tag):
    amqp_context.channel.basic_ack(delivery_tag)


repl = register_repl(amqp)
