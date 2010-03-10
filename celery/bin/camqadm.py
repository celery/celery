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


def say(m):
    sys.stderr.write("%s\n" % (m, ))


OPTION_LIST = (
    #optparse.make_option('-c', '--concurrency',
    #    default=conf.CELERYD_CONCURRENCY,
    #        action="store", dest="concurrency", type="int",
    #        help="Number of child processes processing the queue."),
)

class Spec(object):

    def __init__(self, *arglist, **kwargs):
        self.arglist = arglist
        self.returns = kwargs.get("returns")


class AMQShell(cmd.Cmd):

    conn = None
    chan = None
    prompt = "--> "
    identchars = cmd.IDENTCHARS = "."
    needs_reconnect = False


    builtins = {"exit": "do_exit",
                "EOF": "do_exit"}

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
        if not self.silent:
            say(m)

    def _reconnect(self):
        self.conn = self.connect(self.conn)
        self.chan = self.conn.create_backend().channel
        self.needs_reconnect = False

    def _apply_spec(self, arglist, spec):
        return arglist

    def _get_amqp_api_command(self, cmd, arglist):
        spec = self.amqp[cmd]
        attr_name = cmd.replace(".", "_")
        if self.needs_reconnect:
            self._reconnect()
        return (getattr(self.chan, attr_name),
                self._apply_spec(arglist, spec))

    def do_exit(self, *args):
        self.say("\n-> please, don't leave!")
        sys.exit(0)

    def completenames(self, text, *ignored):
        return [cmd for cmd in set(self.builtins.keys() + self.amqp.keys())
                        if cmd.startswith(text.replace(".", "_"))]

    def dispatch(self, cmd, argline):
        arglist = shlex.split(argline)
        if cmd in self.builtins:
            return getattr(self, self.builtins[cmd])(*arglist)
        fun, args = self._get_amqp_api_command(cmd, arglist)
        return fun(*args)

    def parseline(self, line):
        parts = line.split()
        return parts[0], " ".join(parts[1:]), line

    def onecmd(self, line):
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
        pprint.pprint(retval)


class AMQPAdmin(object):

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
