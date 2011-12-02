# -*- coding: utf-8 -*-
"""

Examples
========

::

    # Single worker with explicit name and events enabled.
    $ celeryd-multi start Leslie -E

    # Pidfiles and logfiles are stored in the current directory
    # by default.  Use --pidfile and --logfile argument to change
    # this.  The abbreviation %n will be expanded to the current
    # node name.
    $ celeryd-multi start Leslie -E --pidfile=/var/run/celery/%n.pid
                                    --logfile=/var/log/celery/%n.log


    # You need to add the same arguments when you restart,
    # as these are not persisted anywhere.
    $ celeryd-multi restart Leslie -E --pidfile=/var/run/celery/%n.pid
                                      --logfile=/var/run/celery/%n.log

    # To stop the node, you need to specify the same pidfile.
    $ celeryd-multi stop Leslie --pidfile=/var/run/celery/%n.pid

    # 3 workers, with 3 processes each
    $ celeryd-multi start 3 -c 3
    celeryd -n celeryd1.myhost -c 3
    celeryd -n celeryd2.myhost -c 3
    celeryd- n celeryd3.myhost -c 3

    # start 3 named workers
    $ celeryd-multi start image video data -c 3
    celeryd -n image.myhost -c 3
    celeryd -n video.myhost -c 3
    celeryd -n data.myhost -c 3

    # specify custom hostname
    $ celeryd-multi start 2 -n worker.example.com -c 3
    celeryd -n celeryd1.worker.example.com -c 3
    celeryd -n celeryd2.worker.example.com -c 3

    # Advanced example starting 10 workers in the background:
    #   * Three of the workers processes the images and video queue
    #   * Two of the workers processes the data queue with loglevel DEBUG
    #   * the rest processes the default' queue.
    $ celeryd-multi start 10 -l INFO -Q:1-3 images,video -Q:4,5:data
        -Q default -L:4,5 DEBUG

    # You can show the commands necessary to start the workers with
    # the "show" command:
    $ celeryd-multi show 10 -l INFO -Q:1-3 images,video -Q:4,5:data
        -Q default -L:4,5 DEBUG

    # Additional options are added to each celeryd',
    # but you can also modify the options for ranges of, or specific workers

    # 3 workers: Two with 3 processes, and one with 10 processes.
    $ celeryd-multi start 3 -c 3 -c:1 10
    celeryd -n celeryd1.myhost -c 10
    celeryd -n celeryd2.myhost -c 3
    celeryd -n celeryd3.myhost -c 3

    # can also specify options for named workers
    $ celeryd-multi start image video data -c 3 -c:image 10
    celeryd -n image.myhost -c 10
    celeryd -n video.myhost -c 3
    celeryd -n data.myhost -c 3

    # ranges and lists of workers in options is also allowed:
    # (-c:1-3 can also be written as -c:1,2,3)
    $ celeryd-multi start 5 -c 3  -c:1-3 10
    celeryd -n celeryd1.myhost -c 10
    celeryd -n celeryd2.myhost -c 10
    celeryd -n celeryd3.myhost -c 10
    celeryd -n celeryd4.myhost -c 3
    celeryd -n celeryd5.myhost -c 3

    # lists also works with named workers
    $ celeryd-multi start foo bar baz xuzzy -c 3 -c:foo,bar,baz 10
    celeryd -n foo.myhost -c 10
    celeryd -n bar.myhost -c 10
    celeryd -n baz.myhost -c 10
    celeryd -n xuzzy.myhost -c 3

"""
from __future__ import absolute_import

import errno
import os
import signal
import socket
import sys

from collections import defaultdict
from subprocess import Popen
from time import sleep

from celery import __version__
from celery.platforms import shellsplit
from celery.utils import term
from celery.utils.encoding import from_utf8

SIGNAMES = set(sig for sig in dir(signal)
                        if sig.startswith("SIG") and "_" not in sig)
SIGMAP = dict((getattr(signal, name), name) for name in SIGNAMES)


USAGE = """\
usage: %(prog_name)s start <node1 node2 nodeN|range> [celeryd options]
       %(prog_name)s stop <n1 n2 nN|range> [-SIG (default: -TERM)]
       %(prog_name)s restart <n1 n2 nN|range> [-SIG] [celeryd options]
       %(prog_name)s kill <n1 n2 nN|range>

       %(prog_name)s show <n1 n2 nN|range> [celeryd options]
       %(prog_name)s get hostname <n1 n2 nN|range> [-qv] [celeryd options]
       %(prog_name)s names <n1 n2 nN|range>
       %(prog_name)s expand template <n1 n2 nN|range>
       %(prog_name)s help

additional options (must appear after command name):

    * --nosplash:   Don't display program info.
    * --quiet:      Don't show as much output.
    * --verbose:    Show more output.
    * --no-color:   Don't display colors.
"""


def main():
    sys.exit(MultiTool().execute_from_commandline(sys.argv))


class MultiTool(object):
    retcode = 0  # Final exit code.

    def __init__(self, env=None, fh=None):
        self.fh = fh or sys.stderr
        self.env = env
        self.commands = {"start": self.start,
                         "show": self.show,
                         "stop": self.stop,
                         "stop_verify": self.stop_verify,
                         "restart": self.restart,
                         "kill": self.kill,
                         "names": self.names,
                         "expand": self.expand,
                         "get": self.get,
                         "help": self.help}

    def execute_from_commandline(self, argv, cmd="celeryd"):
        argv = list(argv)   # don't modify callers argv.

        # Reserve the --nosplash|--quiet|-q/--verbose options.
        self.nosplash = False
        self.quiet = False
        self.verbose = False
        self.no_color = False
        if "--nosplash" in argv:
            self.nosplash = argv.pop(argv.index("--nosplash"))
        if "--quiet" in argv:
            self.quiet = argv.pop(argv.index("--quiet"))
        if "-q" in argv:
            self.quiet = argv.pop(argv.index("-q"))
        if "--verbose" in argv:
            self.verbose = argv.pop(argv.index("--verbose"))
        if "--no-color" in argv:
            self.no_color = argv.pop(argv.index("--no-color"))

        self.colored = term.colored(enabled=not self.no_color)
        self.OK = str(self.colored.green("OK"))
        self.FAILED = str(self.colored.red("FAILED"))
        self.DOWN = str(self.colored.magenta("DOWN"))

        self.prog_name = os.path.basename(argv.pop(0))
        if len(argv) == 0 or argv[0][0] == "-":
            return self.error()

        try:
            self.commands[argv[0]](argv[1:], cmd)
        except KeyError:
            self.error("Invalid command: %s" % argv[0])

        return self.retcode

    def say(self, msg):
        self.fh.write("%s\n" % (msg, ))

    def names(self, argv, cmd):
        p = NamespacedOptionParser(argv)
        self.say("\n".join(hostname
                        for hostname, _, _ in multi_args(p, cmd)))

    def get(self, argv, cmd):
        wanted = argv[0]
        p = NamespacedOptionParser(argv[1:])
        for name, worker, _ in multi_args(p, cmd):
            if name == wanted:
                self.say(" ".join(worker))
                return

    def show(self, argv, cmd):
        p = NamespacedOptionParser(argv)
        self.note("> Starting nodes...")
        self.say("\n".join(" ".join(worker)
                        for _, worker, _ in multi_args(p, cmd)))

    def start(self, argv, cmd):
        self.splash()
        p = NamespacedOptionParser(argv)
        self.with_detacher_default_options(p)
        retcodes = []
        self.note("> Starting nodes...")
        for nodename, argv, _ in multi_args(p, cmd):
            self.note("\t> %s: " % (nodename, ), newline=False)
            retcode = self.waitexec(argv)
            self.note(retcode and self.FAILED or self.OK)
            retcodes.append(retcode)
        self.retcode = int(any(retcodes))

    def with_detacher_default_options(self, p):
        p.options.setdefault("--pidfile", "celeryd@%n.pid")
        p.options.setdefault("--logfile", "celeryd@%n.log")
        p.options.setdefault("--cmd", "-m celery.bin.celeryd_detach")

    def signal_node(self, nodename, pid, sig):
        try:
            os.kill(pid, sig)
        except OSError, exc:
            if exc.errno != errno.ESRCH:
                raise
            self.note("Could not signal %s (%s): No such process" % (
                        nodename, pid))
            return False
        return True

    def node_alive(self, pid):
        try:
            os.kill(pid, 0)
        except OSError, exc:
            if exc.errno == errno.ESRCH:
                return False
            raise
        return True

    def shutdown_nodes(self, nodes, sig=signal.SIGTERM, retry=None,
            callback=None):
        if not nodes:
            return
        P = set(nodes)

        def on_down(node):
            P.discard(node)
            if callback:
                callback(*node)

        self.note(self.colored.blue("> Stopping nodes..."))
        for node in list(P):
            if node in P:
                nodename, _, pid = node
                self.note("\t> %s: %s -> %s" % (nodename,
                                                SIGMAP[sig][3:],
                                                pid))
                if not self.signal_node(nodename, pid, sig):
                    on_down(node)

        def note_waiting():
            left = len(P)
            if left:
                self.note(self.colored.blue("> Waiting for %s %s..." % (
                    left, left > 1 and "nodes" or "node")), newline=False)

        if retry:
            note_waiting()
            its = 0
            while P:
                for node in P:
                    its += 1
                    self.note(".", newline=False)
                    nodename, _, pid = node
                    if not self.node_alive(pid):
                        self.note("\n\t> %s: %s" % (nodename, self.OK))
                        on_down(node)
                        note_waiting()
                        break
                if P and not its % len(P):
                    sleep(float(retry))
            self.note("")

    def getpids(self, p, cmd, callback=None):
        from celery import platforms
        pidfile_template = p.options.setdefault("--pidfile", "celeryd@%n.pid")

        nodes = []
        for nodename, argv, expander in multi_args(p, cmd):
            pidfile = expander(pidfile_template)
            try:
                pid = platforms.PIDFile(pidfile).read_pid()
            except ValueError:
                pass
            if pid:
                nodes.append((nodename, tuple(argv), pid))
            else:
                self.note("> %s: %s" % (nodename, self.DOWN))
                if callback:
                    callback(nodename, argv, pid)

        return nodes

    def kill(self, argv, cmd):
        self.splash()
        p = NamespacedOptionParser(argv)
        for nodename, _, pid in self.getpids(p, cmd):
            self.note("Killing node %s (%s)" % (nodename, pid))
            self.signal_node(nodename, pid, signal.SIGKILL)

    def stop(self, argv, cmd):
        self.splash()
        p = NamespacedOptionParser(argv)
        return self._stop_nodes(p, cmd)

    def _stop_nodes(self, p, cmd, retry=None, callback=None):
        restargs = p.args[len(p.values):]
        self.shutdown_nodes(self.getpids(p, cmd, callback=callback),
                            sig=findsig(restargs),
                            retry=retry,
                            callback=callback)

    def restart(self, argv, cmd):
        self.splash()
        p = NamespacedOptionParser(argv)
        self.with_detacher_default_options(p)
        retvals = []

        def on_node_shutdown(nodename, argv, pid):
            self.note(self.colored.blue(
                "> Restarting node %s: " % nodename), newline=False)
            retval = self.waitexec(argv)
            self.note(retval and self.FAILED or self.OK)
            retvals.append(retval)

        self._stop_nodes(p, cmd, retry=2, callback=on_node_shutdown)
        self.retval = int(any(retvals))

    def stop_verify(self, argv, cmd):
        self.splash()
        p = NamespacedOptionParser(argv)
        self.with_detacher_default_options(p)
        return self._stop_nodes(p, cmd, retry=2)

    def expand(self, argv, cmd=None):
        template = argv[0]
        p = NamespacedOptionParser(argv[1:])
        for _, _, expander in multi_args(p, cmd):
            self.say(expander(template))

    def help(self, argv, cmd=None):
        say(__doc__)

    def usage(self):
        self.splash()
        say(USAGE % {"prog_name": self.prog_name})

    def splash(self):
        if not self.nosplash:
            c = self.colored
            self.note(c.cyan("celeryd-multi v%s" % __version__))

    def waitexec(self, argv, path=sys.executable):
        args = " ".join([path] + list(argv))
        argstr = shellsplit(from_utf8(args))
        pipe = Popen(argstr, env=self.env)
        self.info("  %s" % " ".join(argstr))
        retcode = pipe.wait()
        if retcode < 0:
            self.note("* Child was terminated by signal %s" % (-retcode, ))
            return -retcode
        elif retcode > 0:
            self.note("* Child terminated with failure code %s" % (retcode, ))
        return retcode

    def error(self, msg=None):
        if msg:
            say(msg)
        self.usage()
        self.retcode = 1
        return 1

    def info(self, msg, newline=True):
        if self.verbose:
            self.note(msg, newline=newline)

    def note(self, msg, newline=True):
        if not self.quiet:
            say(str(msg), newline=newline)


def multi_args(p, cmd="celeryd", append="", prefix="", suffix=""):
    names = p.values
    options = dict(p.options)
    passthrough = p.passthrough
    ranges = len(names) == 1
    if ranges:
        try:
            noderange = int(names[0])
        except ValueError:
            pass
        else:
            names = map(str, range(1, noderange + 1))
            prefix = "celery"
    cmd = options.pop("--cmd", cmd)
    append = options.pop("--append", append)
    hostname = options.pop("--hostname",
                   options.pop("-n", socket.gethostname()))
    prefix = options.pop("--prefix", prefix) or ""
    suffix = options.pop("--suffix", suffix) or "." + hostname
    if suffix in ('""', "''"):
        suffix = ""

    for ns_name, ns_opts in p.namespaces.items():
        if "," in ns_name or (ranges and "-" in ns_name):
            for subns in parse_ns_range(ns_name, ranges):
                p.namespaces[subns].update(ns_opts)
            p.namespaces.pop(ns_name)

    for name in names:
        this_name = options["-n"] = prefix + name + suffix
        expand = abbreviations({"%h": this_name,
                                "%n": name})
        argv = ([expand(cmd)] +
                [format_opt(opt, expand(value))
                        for opt, value in p.optmerge(name, options).items()] +
                [passthrough])
        if append:
            argv.append(expand(append))
        yield this_name, argv, expand


class NamespacedOptionParser(object):

    def __init__(self, args):
        self.args = args
        self.options = {}
        self.values = []
        self.passthrough = ""
        self.namespaces = defaultdict(lambda: {})

        self.parse()

    def parse(self):
        rargs = list(self.args)
        pos = 0
        while pos < len(rargs):
            arg = rargs[pos]
            if arg == "--":
                self.passthrough = " ".join(rargs[pos:])
                break
            elif arg[0] == "-":
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


def say(m, newline=True):
    sys.stderr.write(newline and "%s\n" % (m, ) or m)


def findsig(args, default=signal.SIGTERM):
    for arg in reversed(args):
        if len(arg) == 2 and arg[0] == "-" and arg[1].isdigit():
            try:
                return int(arg[1])
            except ValueError:
                pass
        if arg[0] == "-":
            maybe_sig = "SIG" + arg[1:]
            if maybe_sig in SIGNAMES:
                return getattr(signal, maybe_sig)
    return default

if __name__ == "__main__":              # pragma: no cover
    main()
