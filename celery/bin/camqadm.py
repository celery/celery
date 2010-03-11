#!/usr/bin/env python
"""camqadm

.. program:: camqadm

.. cmdoption:: -X, --x

    Description


"""
import os
import cmd
import sys
import shlex
import pprint
import readline
import optparse

from celery.utils import info
from celery.messaging import establish_connection


# Valid string -> bool coercions.
BOOLS = {"1": True, "0": False,
         "yes": True, "no": False,
         "true": True, "False": False}

# Map to coerce strings to other types.
COERCE = {bool: lambda value: BOOLS[value.lower()]}

OPTION_LIST = (
    #optparse.make_option('-c', '--concurrency',
    #    default=conf.CELERYD_CONCURRENCY,
    #        action="store", dest="concurrency", type="int",
    #        help="Number of child processes processing the queue."),
)



def say(m):
    sys.stderr.write("%s\n" % (m, ))


class Spec(object):
    """AMQP Command specification.

    Used to convert arguments to Python values and display various help
    and tooltips.

    :param args: see :attr:`args`.
    :keyword returns: see :attr:`returns`.

    .. attribute args::

        List of arguments this command takes. Should
        contain ``(argument_name, argument_type)`` tuples.

    .. attribute returns:

        Helpful human string representation of what this command returns.
        May be ``None``, to signify the return type is unknown.

    """
    def __init__(self, *args, **kwargs):
        self.args = args
        self.returns = kwargs.get("returns")

    def coerce(self, index, value):
        """Coerce value for argument at index.

        E.g. if :attr:`args` is ``[("is_active", bool)]``:

            >>> coerce(0, "False")
            False

        """
        arg_name, arg_type = self.args[index]
        # Might be a custom way to coerce the string value,
        # so look in the coercion map.
        return COERCE.get(arg_type, arg_type)(value)

    def str_args_to_python(self, arglist):
        """Process list of string arguments to values according to spec.

        e.g:

            >>> spec = Spec([("queue", str), ("if_unused", bool)])
            >>> spec.str_args_to_python("pobox", "true")
            ("pobox", True)

        """
        return tuple(self.coerce(index, value)
                for index, value in enumerate(arglist))


class AMQShell(cmd.Cmd):
    """AMQP API Shell.

    :keyword connect: Function used to connect to the server, must return
        connection object.

    :keyword silent: If ``True``, the commands won't have annoying output not
        relevant when running in non-shell mode.


    .. attribute: builtins

        Mapping of built-in command names -> method names

    .. attribute:: amqp

        Mapping of AMQP API commands and their :class:`Spec`.

    """
    conn = None
    chan = None
    prompt = "--> "
    identchars = cmd.IDENTCHARS = "."
    needs_reconnect = False


    builtins = {"exit": "do_exit", "EOF": "do_exit"}

    amqp = {
        "exchange.declare": Spec(("exchange", str),
                                 ("type", str),
                                 ("passive", bool),
                                 ("durable", bool),
                                 ("auto_delete", bool),
                                 ("internal", bool)),
        "exchange.delete": Spec(("exchange", str),
                                ("if_unused", bool)),
        "queue.bind": Spec(("queue", str),
                           ("exchange", str),
                           ("routing_key", str)),
        "queue.declare": Spec(("queue", str),
                              ("passive", bool),
                              ("durable", bool),
                              ("exclusive", bool),
                              ("auto_delete", bool),
                              returns="Messages purged"),
        "queue.delete": Spec(("queue", str),
                             ("if_unused", bool),
                             ("if_empty", bool)),
        "queue.purge": Spec(("queue", str), returns="Messages purged"),
        "basic.get": Spec(("queue", str),
                          ("no_ack", bool)),
    }

    def __init__(self, *args, **kwargs):
        self.connect = kwargs.pop("connect")
        self.silent = kwargs.pop("silent", False)
        cmd.Cmd.__init__(self, *args, **kwargs)
        self._reconnect()

    def say(self, m):
        """Say something to the user. Disabled if :attr:`silent``."""
        if not self.silent:
            say(m)

    def get_amqp_api_command(self, cmd, arglist):
        """With a command name and a list of arguments, convert the arguments
        to Python values and find the corresponding method on the AMQP channel
        object.

        :returns: tuple of ``(method, processed_args)``.

        Example:

            >>> get_amqp_api_command("queue.delete", ["pobox", "yes", "no"])
            (<bound method Channel.queue_delete of
             <amqplib.client_0_8.channel.Channel object at 0x...>>,
             ('testfoo', True, False))

        """
        spec = self.amqp[cmd]
        args = spec.str_args_to_python(arglist)
        attr_name = cmd.replace(".", "_")
        if self.needs_reconnect:
            self._reconnect()
        return (getattr(self.chan, attr_name), args)

    def do_exit(self, *args):
        """The ``"exit"`` command."""
        self.say("\n-> please, don't leave!")
        sys.exit(0)

    def completenames(self, text, *ignored):
        """Return all commands starting with ``text``, for tab-completion."""
        return [cmd for cmd in set(self.builtins.keys() + self.amqp.keys())
                        if cmd.startswith(text.replace(".", "_"))]

    def dispatch(self, cmd, argline):
        """Dispatch and execute the command.

        Lookup order is: :attr:`builtins` -> :attr:`amqp`.

        """
        arglist = shlex.split(argline)
        if cmd in self.builtins:
            return getattr(self, self.builtins[cmd])(*arglist)
        fun, args = self.get_amqp_api_command(cmd, arglist)
        print((fun, args))
        return fun(*args)

    def parseline(self, line):
        """Parse input line.

        :returns: tuple of three items:
            ``(command_name, arglist, original_line)``

        E.g::

            >>> parseline("queue.delete A 'B' C")
            ("queue.delete", "A 'B' C", "queue.delete A 'B' C")

        """
        parts = line.split()
        return parts[0], " ".join(parts[1:]), line

    def onecmd(self, line):
        """Parse line and execute command."""
        cmd, arg, line = self.parseline(line)
        if not line:
            return self.emptyline()
        if cmd is None:
            return self.default(line)
        self.lastcmd = line
        if cmd == '':
            return self.default(line)
        else:
            try:
                self.respond(self.dispatch(cmd, arg))
            except (AttributeError, KeyError), exc:
                self.default(line)
            except Exception, exc:
                say(exc)
                self.needs_reconnect = True

    def respond(self, retval):
        """What to do with the return value of a command."""
        pprint.pprint(retval)

    def _reconnect(self):
        """Re-establish connection to the AMQP server."""
        self.conn = self.connect(self.conn)
        self.chan = self.conn.create_backend().channel
        self.needs_reconnect = False


class AMQPAdmin(object):
    """The celery ``camqadm`` utility."""

    def __init__(self, *args, **kwargs):
        self.silent = bool(args)
        if "silent" in kwargs:
            self.silent = kwargs["silent"]
        self.args = args

    def connect(self, conn=None):
        if conn:
            conn.close()
        self.say("-> connecting to %s." % info.format_broker_info())
        conn = establish_connection()
        conn.connect()
        self.say("-> connected.")
        return conn

    def run(self):
        shell = AMQShell(connect=self.connect)
        if self.args:
            return shell.onecmd(" ".join(self.args))
        return shell.cmdloop()

    def say(self, m):
        if not self.silent:
            say(m)


def parse_options(arguments):
    """Parse the available options to ``celeryd``."""
    parser = optparse.OptionParser(option_list=OPTION_LIST)
    options, values = parser.parse_args(arguments)
    return options, values


def camqadm(*args, **options):
    return AMQPAdmin(*args, **options).run()


def main():
    options, values = parse_options(sys.argv[1:])
    return run_worker(*values, **vars(options))

if __name__ == "__main__":
    main()
