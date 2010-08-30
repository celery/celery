import sys

from optparse import OptionParser, make_option as Option
from pprint import pprint

from anyjson import deserialize

from celery import __version__


class Command(object):
    help = ""
    args = ""
    option_list = (
        Option("--conf", dest="conf",
            help="Celery config module name (default: celeryconfig)"),
        Option("--loader", dest="loader",
            help="Celery loaders module name (default: default)"),
    )

    def create_parser(self, prog_name, subcommand):
        return OptionParser(prog=prog_name,
                            usage=self.usage(subcommand),
                            version=self.get_version(),
                            option_list=self.option_list)

    def run_from_argv(self, argv):
        parser = self.create_parser(argv[0], argv[1])
        options, args = parser.parse_args(argv[2:])
        if options.loader:
            os.environ["CELERY_LOADER"] = options.loader
        if options.conf:
            os.environ["CELERY_CONFIG_MODULE"] = options.conf
        self(*args, **options.__dict__)

    def __call__(self, *args, **kwargs):
        out = self.run(*args, **kwargs)
        if out:
            sys.stderr.write("%s\n" % out)

    def run(self, *args, **kwargs):
        raise NotImplementedError()

    def get_version(self):
        return __version__

    def usage(self, subcommand):
        return "%%prog %s [options] %s" % (subcommand, self.args)


class apply(Command):
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

class inspect(Command):
    choices = ("active", "scheduled", "reserved",
               "stats", "revoked", "registered_tasks",
               "enable_events", "disable_events")
    option_list = Command.option_list + (
                Option("--timeout", "-t", type="float", dest="timeout",
                    default=1.0,
                    help="Timeout in seconds (float) waiting for reply"),
                Option("--destination", "-d", dest="destination",
                    help="Comma separated list of destination node names."))

    def usage(self, subcommand):
        return "%%prog %s [options] %s [%s]" % (
                subcommand, self.args, "|".join(self.choices))

    def run(self, *args, **kwargs):
        if not args:
            return "Missing inspect command. See --help"
        command = args[0]
        if command not in self.choices:
            return "Unknown inspect command: %s" % command
        from celery.task.control import inspect

        destination = kwargs.get("destination")
        timeout = kwargs.get("timeout", 2.0)
        if destination and isinstance(destination, basestring):
            destination = map(str.strip, destination.split(","))

        i = inspect(destination=destination, timeout=timeout)
        pprint(getattr(i, command)())


commands = {"apply": apply,
            "inspect": inspect}


def main():
    subcommand = sys.argv[1]
    commands[subcommand]().run_from_argv(sys.argv)

if __name__ == "__main__":
    main()
