import errno
import os
import shlex
import signal
import socket
import sys

from celery.utils.compat import defaultdict

EXAMPLES = """
Some examples:

    # The "start" command just prints the commands needed to start the workers.
    # Advanced example with 10 workers:
    #   * Three of the workers processes the images and video queue
    #   * Two of the workers processes the data queue with loglevel DEBUG
    #   * the rest processes the default' queue.
    $ celeryd-multi start 10 -l INFO -Q:1-3 images,video -Q:4,5:data
        -Q default -L:4,5 DEBUG

    # To actually start the workers into the background you need to
    # use the "detach" command:
    $ celeryd-multi detach 10 -l INFO -Q:1-3 images,video -Q:4,5:data
        -Q default -L:4,5 DEBUG

    # get commands to start 10 workers, with 3 processes each
    $ celeryd-multi detach 3 -c 3
    celeryd -n celeryd1.myhost -c 3
    celeryd -n celeryd2.myhost -c 3
    celeryd- n celeryd3.myhost -c 3

    # start 3 named workers
    $ celeryd-multi detach image video data -c 3
    celeryd -n image.myhost -c 3
    celeryd -n video.myhost -c 3
    celeryd -n data.myhost -c 3

    # specify custom hostname
    $ celeryd-multi detach 2 -n worker.example.com -c 3
    celeryd -n celeryd1.worker.example.com -c 3
    celeryd -n celeryd2.worker.example.com -c 3

    # Additionl options are added to each celeryd',
    # but you can also modify the options for ranges of or single workers

    # 3 workers: Two with 3 processes, and one with 10 processes.
    $ celeryd-multi detach 3 -c 3 -c:1 10
    celeryd -n celeryd1.myhost -c 10
    celeryd -n celeryd2.myhost -c 3
    celeryd -n celeryd3.myhost -c 3

    # can also specify options for named workers
    $ celeryd-multi detach image video data -c 3 -c:image 10
    celeryd -n image.myhost -c 10
    celeryd -n video.myhost -c 3
    celeryd -n data.myhost -c 3

    # ranges and lists of workers in options is also allowed:
    # (-c:1-3 can also be written as -c:1,2,3)
    $ celeryd-multi detach 5 -c 3  -c:1-3 10
    celeryd -n celeryd1.myhost -c 10
    celeryd -n celeryd2.myhost -c 10
    celeryd -n celeryd3.myhost -c 10
    celeryd -n celeryd4.myhost -c 3
    celeryd -n celeryd5.myhost -c 3

    # lists also works with named workers
    $ celeryd-multi detach foo bar baz xuzzy -c 3 -c:foo,bar,baz 10
    celeryd -n foo.myhost -c 10
    celeryd -n bar.myhost -c 10
    celeryd -n baz.myhost -c 10
    celeryd -n xuzzy.myhost -c 3
"""


class NamespacedOptionParser(object):

    def __init__(self, args):
        self.args = args
        self.options = {}
        self.values = []
        self.namespaces = defaultdict(lambda: {})

        self.parse()

    def parse(self):
        rargs = list(self.args)
        pos = 0
        while pos < len(rargs):
            arg = rargs[pos]
            if arg[0] == "-":
                if arg[1] == "-":
                    self.process_long_opt(arg[2:])
                else:
                    value = None
                    if len(rargs) > pos + 1 and rargs[pos + 1][0] != '-':
                        value = rargs[pos + 1]
                        pos += 1
                    self.process_short_opt(arg[1:], value)
            else:
                self.values.append(arg)
            pos += 1

    def process_long_opt(self, arg, value=None):
        if "=" in arg:
            arg, value = arg.split("=", 1)
        self.add_option(arg, value, short=False)

    def process_short_opt(self, arg, value=None):
        self.add_option(arg, value, short=True)

    def optmerge(self, ns, defaults=None):
        if defaults is None:
            defaults = self.options
        return dict(defaults, **self.namespaces[ns])

    def add_option(self, name, value, short=False, ns=None):
        prefix = short and "-" or "--"
        dest = self.options
        if ":" in name:
            name, ns = name.split(":")
            dest = self.namespaces[ns]
        dest[prefix + name] = value


def quote(v):
    return "\\'".join("'" + p + "'" for p in v.split("'"))


def format_opt(opt, value):
    if not value:
        return opt
    if opt[0:2] == "--":
        return "%s=%s" % (opt, value)
    return "%s %s" % (opt, value)


def parse_ns_range(ns, ranges=False):
    ret = []
    for space in "," in ns and ns.split(",") or [ns]:
        if ranges and "-" in space:
            start, stop = space.split("-")
            x = map(str, range(int(start), int(stop) + 1))
            ret.extend(x)
        else:
            ret.append(space)
    return ret


def abbreviations(map):

    def expand(S):
        ret = S
        if S is not None:
            for short, long in map.items():
                ret = ret.replace(short, long)
        return ret

    return expand


def multi_args(p, cmd="celeryd", append="", prefix="", suffix=""):
    names = p.values
    options = dict(p.options)
    ranges = len(names) == 1
    if ranges:
        try:
            noderange = int(names[0])
        except ValueError:
            pass
        else:
            names = map(str, range(1, int(names[0]) + 1))
            prefix = "celery"
    cmd = options.pop("--cmd", cmd)
    append = options.pop("--append", append)
    hostname = options.pop("--hostname",
                   options.pop("-n", socket.gethostname()))
    prefix = options.pop("--prefix", prefix) or ""
    suffix = options.pop("--suffix", suffix) or "." + hostname

    for ns_name, ns_opts in p.namespaces.items():
        if "," in ns_name or (ranges and "-" in ns_name):
            for subns in parse_ns_range(ns_name, ranges):
                p.namespaces[subns].update(ns_opts)
            p.namespaces.pop(ns_name)

    for name in names:
        this_name = options["-n"] = prefix + name + suffix
        expand = abbreviations({"%h": this_name,
                                "%n": name})
        line = expand(cmd) + " " + " ".join(
                format_opt(opt, expand(value))
                    for opt, value in p.optmerge(name, options).items())
        if append:
            line += " %s" % expand(append)
        yield this_name, line, expand


def say(m):
    sys.stderr.write("%s\n" % (m, ))


def detach_worker(argv, path=sys.executable):
    if os.fork() == 0:
        os.execv(path, [path] + argv)


class MultiTool(object):

    def __init__(self):
        self.commands = {"start": self.start,
                         "stop": self.stop,
                         "detach": self.detach,
                         "names": self.names,
                         "expand": self.expand,
                         "get": self.get,
                         "help": self.help}

    def __call__(self, argv, cmd="celeryd"):
        if len(argv) == 0 or argv[0][0] == "-":
            self.usage()
            sys.exit(0)

        try:
            return self.commands[argv[0]](argv[1:], cmd)
        except KeyError:
            say("Invalid command: %s" % argv[0])
            self.usage()
            sys.exit(1)

    def names(self, argv, cmd):
        p = NamespacedOptionParser(argv)
        print("\n".join(hostname
                        for hostname, _, _ in multi_args(p, cmd)))

    def get(self, argv, cmd):
        wanted = argv[0]
        p = NamespacedOptionParser(argv[1:])
        for name, worker, _ in multi_args(p, cmd):
            if name == wanted:
                print(worker)
                return

    def start(self, argv, cmd):
        p = NamespacedOptionParser(argv)
        print("\n".join(worker
                        for _, worker, _ in multi_args(p, cmd)))

    def detach(self, argv, cmd):
        p = NamespacedOptionParser(argv)
        p.options.setdefault("--pidfile", "celeryd@%n.pid")
        p.options.setdefault("--logfile", "celeryd@%n.log")
        p.options.setdefault("--cmd", "-m celery.bin.celeryd_detach")
        for nodename, argv, _ in multi_args(p, cmd):
            print("> Starting node %s..." % (nodename, ))
            print(argv)
            detach_worker(shlex.split(argv))

    def stop(self, argv, cmd):
        from celery import platforms
        signames = set(sig for sig in dir(signal)
                            if sig.startswith("SIG") and "_" not in sig)

        def findsig(args, default=signal.SIGTERM):
            for arg in reversed(args):
                if len(arg) == 2 and arg[0] == "-" and arg[1].isdigit():
                    try:
                        return int(arg[1])
                    except ValueError:
                        pass
                if arg[0] == "-":
                    maybe_sig = "SIG" + arg[1:]
                    if maybe_sig in signames:
                        return getattr(signal, maybe_sig)
            return default

        p = NamespacedOptionParser(argv)
        restargs = p.args[len(p.values):]
        sig = findsig(restargs)
        pidfile_template = p.options.setdefault("--pidfile", "celeryd@%n.pid")

        for nodename, _, expander in multi_args(p, cmd):
            pidfile = expander(pidfile_template)
            try:
                pid = platforms.read_pid_from_pidfile(pidfile)
            except ValueError:
                pass
            if pid:
                print("> Stopping node %s (%s)..." % (nodename, pid))
                try:
                    os.kill(pid, sig)
                except OSError, exc:
                    if exc.errno != errno.ESRCH:
                        raise
                    print("Could not stop pid %s: No such process." % pid)
            else:
                print("> Skipping node %s: not alive." % (nodename, ))

    def expand(self, argv, cmd=None):
        template = argv[0]
        p = NamespacedOptionParser(argv[1:])
        for _, _, expander in multi_args(p, cmd):
            print(expander(template))

    def help(self, argv, cmd=None):
        say(EXAMPLES)

    def usage(self):
        say("Please use one of the following commands: %s" % (
            ", ".join(self.commands.keys())))


def main():
    MultiTool()(sys.argv[1:])


if __name__ == "__main__":              # pragma: no cover
    main()
