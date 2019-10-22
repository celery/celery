import sys
from functools import partial

import click
from click_repl import register_repl, repl

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


@click.group(invoke_without_command=True)
@click.pass_context
def amqp(ctx):
    """AMQP Administration Shell.

    Also works for non-AMQP transports (but not ones that
    store declarations in memory).
    """
    ctx.parent = None  # Force click-repl to use amqp's subcommands
    repl(ctx, allow_system_commands=False)


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
def basic_get():
    pass


@amqp.command(name='basic.publish')
def basic_publish():
    pass


@amqp.command(name='basic.ack')
def basic_ack():
    pass
