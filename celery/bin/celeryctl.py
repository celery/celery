import os
import sys

from optparse import OptionParser, make_option as Option
from pprint import pprint, pformat
from textwrap import wrap

from anyjson import deserialize

from celery import __version__
from celery.utils import term as t


commands = {}


class Error(Exception):
    pass


def command(fun):
    commands[fun.__name__] = fun
    return fun


class Command(object):
    help = ""
    args = ""
    option_list = (
        Option("--conf", dest="conf",
            help="Celery config module name (default: celeryconfig)"),
        Option("--loader", dest="loader",
            help="Celery loaders module name (default: default)"),
    )

    def create_parser(self, prog_name, command):
        return OptionParser(prog=prog_name,
                            usage=self.usage(command),
                            version=self.get_version(),
                            option_list=self.option_list)

    def run_from_argv(self, argv):
        self.prog_name = os.path.basename(argv[0])
        self.command = argv[1]
        self.arglist = argv[2:]
        self.parser = self.create_parser(self.prog_name, self.command)
        options, args = self.parser.parse_args(self.arglist)
        if options.loader:
            os.environ["CELERY_LOADER"] = options.loader
        if options.conf:
            os.environ["CELERY_CONFIG_MODULE"] = options.conf
        self(*args, **options.__dict__)

    def __call__(self, *args, **kwargs):
        try:
            self.run(*args, **kwargs)
        except Error, exc:
            sys.stderr.write(t.colored(t.red(
                                t.bold("Error: %s\n" % (exc, )))))

    def run(self, *args, **kwargs):
        raise NotImplementedError()

    def get_version(self):
        return __version__

    def usage(self, command):
        return "%%prog %s [options] %s" % (command, self.args)


class apply(Command):
    args = "<task_name>"
    option_list = Command.option_list + (
            Option("--args", "-a", dest="args"),
            Option("--kwargs", "-k", dest="kwargs"),
            Option("--eta", dest="eta"),
            Option("--countdown", dest="countdown", type="int"),
            Option("--expires", dest="expires"),
            Option("--serializer", dest="serializer", default="json"),
            Option("--queue", dest="queue"),
            Option("--exchange", dest="exchange"),
            Option("--routing-key", dest="routing_key"),
    )

    def run(self, name, *_, **kw):
        from celery.execute import send_task

        # Positional args.
        args = kw.get("args") or ()
        if isinstance(args, basestring):
            args = deserialize(args)

        # Keyword args.
        kwargs = kw.get("kwargs") or {}
        if isinstance(kwargs, basestring):
            kwargs = deserialize(kwargs)

        # Expires can be int.
        expires = kw.get("expires") or None
        try:
            expires = int(expires)
        except (TypeError, ValueError):
            pass

        res = send_task(name, args=args, kwargs=kwargs,
                        countdown=kw.get("countdown"),
                        serializer=kw.get("serializer"),
                        queue=kw.get("queue"),
                        exchange=kw.get("exchange"),
                        routing_key=kw.get("routing_key"),
                        eta=kw.get("eta"),
                        expires=expires)

        print(res.task_id)
apply = command(apply)

class inspect(Command):
    choices = {"active": 10,
               "scheduled": 1.0,
               "reserved": 1.0,
               "stats": 1.0,
               "revoked": 1.0,
               "registered_tasks": 1.0,
               "enable_events": 1.0,
               "disable_events": 1.0,
               "diagnose": 2.0,
               "ping": 0.2}
    option_list = Command.option_list + (
                Option("--quiet", "-q", action="store_true", dest="quiet",
                       default=False),
                Option("--timeout", "-t", type="float", dest="timeout",
                    default=None,
                    help="Timeout in seconds (float) waiting for reply"),
                Option("--destination", "-d", dest="destination",
                    help="Comma separated list of destination node names."))

    def usage(self, command):
        return "%%prog %s [options] %s [%s]" % (
                command, self.args, "|".join(self.choices.keys()))

    def run(self, *args, **kwargs):
        self.quiet = kwargs.get("quiet", False)
        if not args:
            raise Error("Missing inspect command. See --help")
        command = args[0]
        if command not in self.choices:
            raise Error("Unknown inspect command: %s" % command)
        from celery.task.control import inspect

        destination = kwargs.get("destination")
        timeout = kwargs.get("timeout") or self.choices[command]
        if destination and isinstance(destination, basestring):
            destination = map(str.strip, destination.split(","))

        def on_reply(message_data):
            node = message_data.keys()[0]
            reply = message_data[node]
            status, preply = prettify(reply)
            self.say("->", t.cyan(node, ": ") + t.reset() + status,
                    indent(preply))

        self.say("<-", command)
        i = inspect(destination=destination,
                    timeout=timeout,
                    callback=on_reply)
        replies = getattr(i, command)()
        if not replies:
            raise Error("No nodes replied within time constraint.")
        return replies

    def say(self, direction, title, body=""):
        if direction == "<-" and self.quiet:
            return
        dirstr = not self.quiet and t.bold(t.white(direction), " ") or ""
        print(t.colored(dirstr, title))
        if body and not self.quiet:
            print(body)
inspect = command(inspect)

def prettify_list(n):
    if not n:
        return "- empty -"
    return "\n".join(t.colored(t.white("*"), t.reset(), " %s" % (item, ))
                        for item in n)

OK = t.colored(t.green("OK"))
ERROR = t.colored(t.red("ERROR"))
def prettify_dict_ok_error(n):
    if "ok" in n:
        return (OK,
                indent(prettify(n["ok"])[1]))
    elif "error" in n:
        return (ERROR,
                indent(prettify(n["error"])[1]))


def prettify(n):
    if isinstance(n, list):
        return OK, prettify_list(n)
    if isinstance(n, dict):
        if "ok" in n or "error" in n:
            return prettify_dict_ok_error(n)
    if isinstance(n, basestring):
        return OK, unicode(n)
    return OK, pformat(n)


def indent(s, n=4):
    i = [" " * 4 + l for l in s.split("\n")]
    return "\n".join("\n".join(wrap(j)) for j in i)


class status(Command):

    def run(self, *args, **kwargs):
        replies = inspect().run("ping", quiet=True)
        if not replies:
            raise Error("No nodes replied within time constraint")
        nodecount = len(replies)
        print("\n%s %s online." % (nodecount,
                                   nodecount > 1 and "nodes" or "node"))
status = command(status)




class help(Command):

    def usage(self, command):
        return "%%prog <command> [options] %s" % (self.args, )

    def run(self, *args, **kwargs):
        self.parser.print_help()
        usage = ["",
                "Type '%s <command> --help' for help on a "
                "specific command." % (self.prog_name, ),
                "",
                "Available commands:"]
        for command in list(sorted(commands.keys())):
            usage.append("    %s" % command)
        print("\n".join(usage))
help = command(help)


class celeryctl(object):
    commands = commands

    def execute(self, command, argv=None):
        if argv is None:
            argv = sys.arg
        argv = list(argv)
        try:
            cls = self.commands[command]
        except KeyError:
            cls = self.commands["help"]
            argv.insert(1, "help")
        cls = self.commands.get(command) or self.commands["help"]
        cls().run_from_argv(argv)

    def execute_from_commandline(self, argv=None):
        if argv is None:
            argv = sys.argv
        try:
            command = argv[1]
        except IndexError:
            command = "help"
        return self.execute(command, argv)


def main():
    celeryctl().execute_from_commandline()

if __name__ == "__main__":
    main()
