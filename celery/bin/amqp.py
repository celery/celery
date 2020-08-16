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
def exchange_declare(amqp_context, exchange, type, passive, durable,
                     auto_delete):
    amqp_context.channel.exchange_declare(exchange=exchange,
                                          type=type,
                                          passive=passive,
                                          durable=durable,
                                          auto_delete=auto_delete)
    amqp_context.cli_context.echo(amqp_context.cli_context.OK)


@amqp.command(name='exchange.delete')
@click.argument('exchange',
                type=str)
@click.argument('if_unused',
                type=bool)
@click.pass_obj
def exchange_delete(amqp_context, exchange, if_unused):
    amqp_context.channel.exchange_delete(exchange=exchange,
                                         if_unused=if_unused)
    amqp_context.cli_context.echo(amqp_context.cli_context.OK)


@amqp.command(name='queue.bind')
@click.argument('queue',
                type=str)
@click.argument('exchange',
                type=str)
@click.argument('routing_key',
                type=str)
@click.pass_obj
def queue_bind(amqp_context, queue, exchange, routing_key):
    amqp_context.channel.queue_bind(queue=queue,
                                    exchange=exchange,
                                    routing_key=routing_key)
    amqp_context.cli_context.echo(amqp_context.cli_context.OK)


@amqp.command(name='queue.declare')
@click.argument('queue',
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
def queue_declare(amqp_context, queue, passive, durable, auto_delete):
    retval = amqp_context.channel.queue_declare(queue=queue,
                                                passive=passive,
                                                durable=durable,
                                                auto_delete=auto_delete)
    amqp_context.cli_context.secho('queue:{0} messages:{1} consumers:{2}'.format(*retval),
                                   fg='cyan', bold=True)
    amqp_context.cli_context.echo(amqp_context.cli_context.OK)


@amqp.command(name='queue.delete')
@click.argument('queue',
                type=str)
@click.argument('if_unused',
                type=bool,
                default=False)
@click.argument('if_empty',
                type=bool,
                default=False)
@click.pass_obj
def queue_delete(amqp_context, queue, if_unused, if_empty):
    amqp_context.channel.queue_delete(queue=queue,
                                      if_unused=if_unused,
                                      if_empty=if_empty)
    amqp_context.cli_context.echo(amqp_context.cli_context.OK)


@amqp.command(name='queue.purge')
def queue_purge():
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
def basic_publish(amqp_context, msg, exchange, routing_key, mandatory,
                  immediate):
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
