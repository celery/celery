# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import with_statement

import anyjson
import sys

from billiard import freeze_support
from importlib import import_module
from pprint import pformat

from celery import __version__
from celery.platforms import EX_OK, EX_FAILURE, EX_UNAVAILABLE, EX_USAGE
from celery.utils import term
from celery.utils.imports import symbol_by_name
from celery.utils.text import pluralize
from celery.utils.timeutils import maybe_iso8601

from celery.bin.base import Command as BaseCommand, Option

HELP = """
Type '%(prog_name)s <command> --help' for help using
a specific command.

Available commands:
%(commands)s
"""

commands = {}


class Error(Exception):

    def __init__(self, reason, status=EX_FAILURE):
        self.reason = reason
        self.status = status
        super(Error, self).__init__(reason, status)

    def __str__(self):
        return self.reason


def command(fun, name=None):
    commands[name or fun.__name__] = fun
    return fun


class Command(BaseCommand):
    help = ""
    args = ""
    version = __version__
    prog_name = "celery"

    option_list = (
        Option("--quiet", "-q", action="store_true"),
        Option("--no-color", "-C", action="store_true"),
    )

    def __init__(self, app=None, no_color=False, stdout=sys.stdout,
            stderr=sys.stderr):
        super(Command, self).__init__(app=app)
        self.colored = term.colored(enabled=not no_color)
        self.stdout = stdout
        self.stderr = stderr

    def __call__(self, *args, **kwargs):
        try:
            ret = self.run(*args, **kwargs)
        except Error, exc:
            self.error(self.colored.red("Error: %s" % exc))
            return exc.status

        return ret if ret is not None else EX_OK

    def show_help(self, command):
        self.run_from_argv(self.prog_name, [command, "--help"])
        return EX_USAGE

    def error(self, s):
        self.out(s, fh=self.stderr)

    def out(self, s, fh=None):
        s = str(s)
        if not s.endswith("\n"):
            s += "\n"
        (fh or self.stdout).write(s)

    def run_from_argv(self, prog_name, argv):
        self.prog_name = prog_name
        self.command = argv[0]
        self.arglist = argv[1:]
        self.parser = self.create_parser(self.prog_name, self.command)
        options, args = self.prepare_args(
                *self.parser.parse_args(self.arglist))
        self.colored = term.colored(enabled=not options["no_color"])
        return self(*args, **options)

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
        try:
            return (c.green("OK"),
                    indent(self.prettify(n["ok"])[1]))
        except KeyError:
            pass
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


class Delegate(Command):

    def __init__(self, *args, **kwargs):
        super(Delegate, self).__init__(*args, **kwargs)

        self.target = symbol_by_name(self.Command)(app=self.app)
        self.args = self.target.args

    def get_options(self):
        return self.option_list + self.target.get_options()

    def create_parser(self, prog_name, command):
        parser = super(Delegate, self).create_parser(prog_name, command)
        return self.target.prepare_parser(parser)

    def run(self, *args, **kwargs):
        self.target.check_args(args)
        return self.target.run(*args, **kwargs)


def create_delegate(name, Command):
    return command(type(name, (Delegate, ), {"Command": Command,
                                             "__module__": __name__}))


worker = create_delegate("worker", "celery.bin.celeryd:WorkerCommand")
events = create_delegate("events", "celery.bin.celeryev:EvCommand")
beat = create_delegate("beat", "celery.bin.celerybeat:BeatCommand")
amqp = create_delegate("amqp", "celery.bin.camqadm:AMQPAdminCommand")


class list_(Command):
    args = "[bindings]"

    def list_bindings(self, management):
        try:
            bindings = management.get_bindings()
        except NotImplementedError:
            raise Error("Your transport cannot list bindings.")

        fmt = lambda q, e, r: self.out("%s %s %s" % (q.ljust(28),
                                                     e.ljust(28), r))
        fmt("Queue", "Exchange", "Routing Key")
        fmt("-" * 16, "-" * 16, "-" * 16)
        for b in bindings:
            fmt(b["destination"], b["source"], b["routing_key"])

    def run(self, what=None, *_, **kw):
        topics = {"bindings": self.list_bindings}
        available = ', '.join(topics.keys())
        if not what:
            raise Error("You must specify what to list (%s)" % available)
        if what not in topics:
            raise Error("unknown topic %r (choose one of: %s)" % (
                            what, available))
        with self.app.broker_connection() as conn:
            self.app.amqp.TaskConsumer(conn).declare()
            topics[what](conn.manager)
list_ = command(list_, "list")


class apply(Command):
    args = "<task_name>"
    option_list = Command.option_list + (
            Option("--args", "-a"),
            Option("--kwargs", "-k"),
            Option("--eta"),
            Option("--countdown", type="int"),
            Option("--expires"),
            Option("--serializer", default="json"),
            Option("--queue"),
            Option("--exchange"),
            Option("--routing-key"),
    )

    def run(self, name, *_, **kw):
        # Positional args.
        args = kw.get("args") or ()
        if isinstance(args, basestring):
            args = anyjson.loads(args)

        # Keyword args.
        kwargs = kw.get("kwargs") or {}
        if isinstance(kwargs, basestring):
            kwargs = anyjson.loads(kwargs)

        # Expires can be int/float.
        expires = kw.get("expires") or None
        try:
            expires = float(expires)
        except (TypeError, ValueError):
            # or a string describing an ISO 8601 datetime.
            try:
                expires = maybe_iso8601(expires)
            except (TypeError, ValueError):
                raise

        res = self.app.send_task(name, args=args, kwargs=kwargs,
                                 countdown=kw.get("countdown"),
                                 serializer=kw.get("serializer"),
                                 queue=kw.get("queue"),
                                 exchange=kw.get("exchange"),
                                 routing_key=kw.get("routing_key"),
                                 eta=maybe_iso8601(kw.get("eta")),
                                 expires=expires)
        self.out(res.id)
apply = command(apply)


class purge(Command):

    def run(self, *args, **kwargs):
        queues = len(self.app.amqp.queues.keys())
        messages_removed = self.app.control.purge()
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
            Option("--task", "-t"),
    )

    def run(self, task_id, *args, **kwargs):
        result_cls = self.app.AsyncResult
        task = kwargs.get("task")

        if task:
            result_cls = self.app.tasks[task].AsyncResult
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
               "registered_tasks": 1.0,  # alias to registered
               "registered": 1.0,
               "enable_events": 1.0,
               "disable_events": 1.0,
               "ping": 0.2,
               "add_consumer": 1.0,
               "cancel_consumer": 1.0,
               "report": 1.0}
    option_list = Command.option_list + (
                Option("--timeout", "-t", type="float",
                    help="Timeout in seconds (float) waiting for reply"),
                Option("--destination", "-d",
                    help="Comma separated list of destination node names."))
    show_body = True

    def usage(self, command):
        return "%%prog %s [options] %s [%s]" % (
                command, self.args, "|".join(self.choices.keys()))

    def run(self, *args, **kwargs):
        self.quiet = kwargs.get("quiet", False)
        self.show_body = kwargs.get("show_body", True)
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
            raise Error("No nodes replied within time constraint.",
                        status=EX_UNAVAILABLE)
        return replies

    def say(self, direction, title, body=""):
        c = self.colored
        if direction == "<-" and self.quiet:
            return
        dirstr = not self.quiet and c.bold(c.white(direction), " ") or ""
        self.out(c.reset(dirstr, title))
        if body and self.show_body:
            self.out(body)
inspect = command(inspect)


def indent(s, n=4):
    return "\n".join(" " * n + l for l in s.split("\n"))


class status(Command):
    option_list = inspect.option_list

    def run(self, *args, **kwargs):
        replies = inspect(app=self.app,
                          no_color=kwargs.get("no_color", False),
                          stdout=self.stdout, stderr=self.stderr) \
                    .run("ping", **dict(kwargs, quiet=True, show_body=False))
        if not replies:
            raise Error("No nodes replied within time constraint",
                        status=EX_UNAVAILABLE)
        nodecount = len(replies)
        if not kwargs.get("quiet", False):
            self.out("\n%s %s online." % (nodecount,
                                          pluralize(nodecount, "node")))
status = command(status)


class migrate(Command):

    def usage(self, command):
        return "%%prog %s <source_url> <dest_url>" % (command, )

    def on_migrate_task(self, state, body, message):
        self.out("Migrating task %s/%s: %s[%s]" % (
            state.count, state.strtotal, body["task"], body["id"]))

    def run(self, *args, **kwargs):
        if len(args) != 2:
            return self.show_help("migrate")
        from kombu import BrokerConnection
        from celery.contrib.migrate import migrate_tasks

        migrate_tasks(BrokerConnection(args[0]),
                      BrokerConnection(args[1]),
                      callback=self.on_migrate_task)
migrate = command(migrate)


class shell(Command):  # pragma: no cover
    option_list = Command.option_list + (
                Option("--ipython", "-I",
                    action="store_true", dest="force_ipython",
                    help="Force IPython."),
                Option("--bpython", "-B",
                    action="store_true", dest="force_bpython",
                    help="Force bpython."),
                Option("--python", "-P",
                    action="store_true", dest="force_python",
                    help="Force default Python shell."),
                Option("--without-tasks", "-T", action="store_true",
                    help="Don't add tasks to locals."),
                Option("--eventlet", action="store_true",
                    help="Use eventlet."),
                Option("--gevent", action="store_true", help="Use gevent."),
    )

    def run(self, force_ipython=False, force_bpython=False,
            force_python=False, without_tasks=False, eventlet=False,
            gevent=False, **kwargs):
        if eventlet:
            import_module("celery.concurrency.eventlet")
        if gevent:
            import_module("celery.concurrency.gevent")
        import celery
        import celery.task.base
        self.app.loader.import_default_modules()
        self.locals = {"celery": self.app,
                       "BaseTask": celery.task.base.BaseTask,
                       "chord": celery.chord,
                       "group": celery.group,
                       "chain": celery.chain,
                       "chunks": celery.chunks,
                       "xmap": celery.xmap,
                       "xstarmap": celery.xstarmap,
                       "subtask": celery.subtask}

        if not without_tasks:
            self.locals.update(dict((task.__name__, task)
                                for task in self.app.tasks.itervalues()
                                    if not task.name.startswith("celery.")))

        if force_python:
            return self.invoke_fallback_shell()
        elif force_bpython:
            return self.invoke_bpython_shell()
        elif force_ipython:
            return self.invoke_ipython_shell()
        return self.invoke_default_shell()

    def invoke_default_shell(self):
        try:
            import IPython  # noqa
        except ImportError:
            try:
                import bpython  # noqa
            except ImportError:
                return self.invoke_fallback_shell()
            else:
                return self.invoke_bpython_shell()
        else:
            return self.invoke_ipython_shell()

    def invoke_fallback_shell(self):
        import code
        try:
            import readline
        except ImportError:
            pass
        else:
            import rlcompleter
            readline.set_completer(
                    rlcompleter.Completer(self.locals).complete)
            readline.parse_and_bind("tab:complete")
        code.interact(local=self.locals)

    def invoke_ipython_shell(self):
        try:
            from IPython.frontend.terminal import embed
            embed.TerminalInteractiveShell(user_ns=self.locals).mainloop()
        except ImportError:  # ipython < 0.11
            from IPython.Shell import IPShell
            IPShell(argv=[], user_ns=self.locals).mainloop()

    def invoke_bpython_shell(self):
        import bpython
        bpython.embed(self.locals)

shell = command(shell)


class help(Command):

    def usage(self, command):
        return "%%prog <command> [options] %s" % (self.args, )

    def run(self, *args, **kwargs):
        self.parser.print_help()
        self.out(HELP % {"prog_name": self.prog_name,
                         "commands": "\n".join(indent(command)
                                             for command in sorted(commands))})

        return EX_USAGE
help = command(help)


class report(Command):

    def run(self, *args, **kwargs):
        self.out(self.app.bugreport())
        return EX_OK
report = command(report)


class CeleryCommand(BaseCommand):
    commands = commands
    enable_config_from_cmdline = True
    prog_name = "celery"

    def execute(self, command, argv=None):
        try:
            cls = self.commands[command]
        except KeyError:
            cls, argv = self.commands["help"], ["help"]
        cls = self.commands.get(command) or self.commands["help"]
        try:
            return cls(app=self.app).run_from_argv(self.prog_name, argv)
        except Error:
            return self.execute("help", argv)

    def remove_options_at_beginning(self, argv, index=0):
        if argv:
            while index < len(argv):
                value = argv[index]
                if value.startswith("--"):
                    pass
                elif value.startswith("-"):
                    index += 1
                else:
                    return argv[index:]
                index += 1
        return []

    def handle_argv(self, prog_name, argv):
        self.prog_name = prog_name
        argv = self.remove_options_at_beginning(argv)
        _, argv = self.prepare_args(None, argv)
        try:
            command = argv[0]
        except IndexError:
            command, argv = "help", ["help"]
        return self.execute(command, argv)

    def execute_from_commandline(self, argv=None):
        try:
            sys.exit(determine_exit_status(
                super(CeleryCommand, self).execute_from_commandline(argv)))
        except KeyboardInterrupt:
            sys.exit(EX_FAILURE)


def determine_exit_status(ret):
    if isinstance(ret, int):
        return ret
    return EX_OK if ret else EX_FAILURE


def main():
    # Fix for setuptools generated scripts, so that it will
    # work with multiprocessing fork emulation.
    # (see multiprocessing.forking.get_preparation_data())
    if __name__ != "__main__":  # pragma: no cover
        sys.modules["__main__"] = sys.modules[__name__]
    freeze_support()
    CeleryCommand().execute_from_commandline()

if __name__ == "__main__":          # pragma: no cover
    main()
