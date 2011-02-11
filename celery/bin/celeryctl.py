import sys

from optparse import OptionParser, make_option as Option
from pprint import pformat
from textwrap import wrap

from anyjson import deserialize

from celery import __version__
from celery.app import app_or_default, current_app
from celery.bin.base import Command as CeleryCommand
from celery.utils import term


commands = {}


class Error(Exception):
    pass


def command(fun):
    commands[fun.__name__] = fun
    return fun


class Command(object):
    help = ""
    args = ""
    version = __version__

    option_list = CeleryCommand.preload_options + (
        Option("--quiet", "-q", action="store_true", dest="quiet",
                default=False),
        Option("--no-color", "-C", dest="no_color", action="store_true",
            help="Don't colorize output."),
    )

    def __init__(self, app=None, no_color=False):
        self.app = app_or_default(app)
        self.colored = term.colored(enabled=not no_color)

    def __call__(self, *args, **kwargs):
        try:
            self.run(*args, **kwargs)
        except Error, exc:
            self.error(self.colored.red("Error: %s" % exc))

    def error(self, s):
        return self.out(s, fh=sys.stderr)

    def out(self, s, fh=sys.stdout):
        s = str(s)
        if not s.endswith("\n"):
            s += "\n"
        sys.stdout.write(s)

    def create_parser(self, prog_name, command):
        return OptionParser(prog=prog_name,
                            usage=self.usage(command),
                            version=self.version,
                            option_list=self.option_list)

    def run_from_argv(self, prog_name, argv):
        self.prog_name = prog_name
        self.command = argv[0]
        self.arglist = argv[1:]
        self.parser = self.create_parser(self.prog_name, self.command)
        options, args = self.parser.parse_args(self.arglist)
        self.colored = term.colored(enabled=not options.no_color)
        self(*args, **options.__dict__)

    def run(self, *args, **kwargs):
        raise NotImplementedError()

    def usage(self, command):
        return "%%prog %s [options] %s" % (command, self.args)

    def prettify_list(self, n):
        c = self.colored
        if not n:
            return "- empty -"
        return "\n".join(str(c.reset(c.white("*"), " %s" % (item, )))
                            for item in n)

    def prettify_dict_ok_error(self, n):
        c = self.colored
        if "ok" in n:
            return (c.green("OK"),
                    indent(self.prettify(n["ok"])[1]))
        elif "error" in n:
            return (c.red("ERROR"),
                    indent(self.prettify(n["error"])[1]))

    def prettify(self, n):
        OK = str(self.colored.green("OK"))
        if isinstance(n, list):
            return OK, self.prettify_list(n)
        if isinstance(n, dict):
            if "ok" in n or "error" in n:
                return self.prettify_dict_ok_error(n)
        if isinstance(n, basestring):
            return OK, unicode(n)
        return OK, pformat(n)


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

        res = self.app.send_task(name, args=args, kwargs=kwargs,
                                 countdown=kw.get("countdown"),
                                 serializer=kw.get("serializer"),
                                 queue=kw.get("queue"),
                                 exchange=kw.get("exchange"),
                                 routing_key=kw.get("routing_key"),
                                 eta=kw.get("eta"),
                                 expires=expires)
        self.out(res.task_id)
apply = command(apply)


def pluralize(n, text, suffix='s'):
    if n > 1:
        return text + suffix
    return text


class purge(Command):

    def run(self, *args, **kwargs):
        app = current_app()
        queues = len(app.amqp.queues.keys())
        messages_removed = app.control.discard_all()
        if messages_removed:
            self.out("Purged %s %s from %s known task %s." % (
                messages_removed, pluralize(messages_removed, "message"),
                queues, pluralize(queues, "queue")))
        else:
            self.out("No messages purged from %s known %s" % (
                queues, pluralize(queues, "queue")))
purge = command(purge)


class result(Command):
    args = "<task_id>"
    option_list = Command.option_list + (
            Option("--task", "-t", dest="task"),
    )

    def run(self, task_id, *args, **kwargs):
        from celery import registry
        result_cls = self.app.AsyncResult
        task = kwargs.get("task")

        if task:
            result_cls = registry.tasks[task].AsyncResult
        result = result_cls(task_id)
        self.out(self.prettify(result.get())[1])
result = command(result)


class inspect(Command):
    choices = {"active": 1.0,
               "active_queues": 1.0,
               "scheduled": 1.0,
               "reserved": 1.0,
               "stats": 1.0,
               "revoked": 1.0,
               "registered_tasks": 1.0,
               "enable_events": 1.0,
               "disable_events": 1.0,
               "ping": 0.2,
               "add_consumer": 1.0,
               "cancel_consumer": 1.0}
    option_list = Command.option_list + (
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
        if command == "help":
            raise Error("Did you mean 'inspect --help'?")
        if command not in self.choices:
            raise Error("Unknown inspect command: %s" % command)

        destination = kwargs.get("destination")
        timeout = kwargs.get("timeout") or self.choices[command]
        if destination and isinstance(destination, basestring):
            destination = map(str.strip, destination.split(","))

        def on_reply(body):
            c = self.colored
            node = body.keys()[0]
            reply = body[node]
            status, preply = self.prettify(reply)
            self.say("->", c.cyan(node, ": ") + status, indent(preply))

        self.say("<-", command)
        i = self.app.control.inspect(destination=destination,
                                     timeout=timeout,
                                     callback=on_reply)
        replies = getattr(i, command)(*args[1:])
        if not replies:
            raise Error("No nodes replied within time constraint.")
        return replies

    def say(self, direction, title, body=""):
        c = self.colored
        if direction == "<-" and self.quiet:
            return
        dirstr = not self.quiet and c.bold(c.white(direction), " ") or ""
        self.out(c.reset(dirstr, title))
        if body and not self.quiet:
            self.out(body)
inspect = command(inspect)


def indent(s, n=4):
    i = [" " * n + l for l in s.split("\n")]
    return "\n".join("\n".join(wrap(j)) for j in i)


class status(Command):
    option_list = inspect.option_list

    def run(self, *args, **kwargs):
        replies = inspect(app=self.app,
                          no_color=kwargs.get("no_color", False)) \
                    .run("ping", **dict(kwargs, quiet=True))
        if not replies:
            raise Error("No nodes replied within time constraint")
        nodecount = len(replies)
        if not kwargs.get("quiet", False):
            self.out("\n%s %s online." % (nodecount,
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
        self.out("\n".join(usage))
help = command(help)


class celeryctl(CeleryCommand):
    commands = commands

    def execute(self, command, argv=None):
        try:
            cls = self.commands[command]
        except KeyError:
            cls, argv = self.commands["help"], ["help"]
        cls = self.commands.get(command) or self.commands["help"]
        try:
            cls(app=self.app).run_from_argv(self.prog_name, argv)
        except Error:
            return self.execute("help", argv)

    def handle_argv(self, prog_name, argv):
        self.prog_name = prog_name
        try:
            command = argv[0]
        except IndexError:
            command, argv = "help", ["help"]
        return self.execute(command, argv)


def main():
    try:
        celeryctl().execute_from_commandline()
    except KeyboardInterrupt:
        pass

if __name__ == "__main__":          # pragma: no cover
    main()
