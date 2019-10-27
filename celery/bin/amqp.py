import pprint

import click
from amqp import Connection, Message
from click_repl import register_repl

__all__ = ('amqp',)


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
@click.argument('exchange',
                type=str)
@click.argument('type',
                type=str)
@click.argument('passive',
                type=bool,
                default=False)
@click.argument('durable',
                type=bool,
                default=False)
@click.argument('auto_delete',
                type=bool,
                default=False)
@click.pass_obj
def exchange_declare(amqp_context, exchange, type, passive, durable, auto_delete):
    amqp_context.channel.exchange_declare(exchange=exchange,
                                          type=type,
                                          passive=passive,
                                          durable=durable,
                                          auto_delete=auto_delete)
    amqp_context.cli_context.echo(amqp_context.cli_context.OK)


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
    message = amqp_context.channel.basic_get(queue, no_ack=no_ack)
    amqp_context.respond(dump_message(message))


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
    amqp_context.cli_context.echo(amqp_context.cli_context.OK)


repl = register_repl(amqp)
