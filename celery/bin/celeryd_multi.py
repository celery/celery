import sys
import shlex
import socket

from celery.utils.compat import defaultdict
from carrot.utils import rpartition


class OptionParser(object):

    def __init__(self, args):
        self.args = args
        self.options = {}
        self.values = []
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
                    if rargs[pos + 1][0] != '-':
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

    def set_option(self, arg, value, short=False):
        prefix = short and "-" or "--"
        self.options[prefix + arg] = value


class NamespacedOptionParser(OptionParser):

    def __init__(self, args):
        self.namespaces = defaultdict(lambda: {})
        super(NamespacedOptionParser, self).__init__(args)

    def add_option(self, name, value, short=False, ns=None):
        prefix = short and "-" or "--"
        dest = self.options
        if ":" in name:
            name, ns = name.split(":")
            dest = self.namespaces[ns]
        dest[prefix + name] = value

    def optmerge(self, ns, defaults=None):
        if defaults is None:
            defaults = self.options
        return dict(defaults, **self.namespaces[ns])


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
        for short, long in map.items():
            ret = ret.replace(short, long)
        return ret

    return expand


def multi_args(p, cmd="celeryd", prefix="", suffix=""):
    names = p.values
    options = dict(p.options)
    ranges = len(names) == 1
    if ranges:
        names = map(str, range(1, int(names[0]) + 1))
        prefix = "celery"
    cmd = options.pop("--cmd", cmd)
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
        expand = abbreviations({"%n": this_name,
                                "%p": prefix + name})
        line = expand(cmd) + " " + " ".join(
                format_opt(opt, expand(value))
                    for opt, value in p.optmerge(name, options).items())
        yield this_name, line, expand




def names(argv, cmd):
    p = NamespacedOptionParser(argv)
    print("\n".join(hostname
                        for hostname, _, _ in multi_args(p, cmd)))

def get(argv, cmd):
    wanted = argv[0]
    p = NamespacedOptionParser(argv[1:])
    for name, worker, _ in multi_args(p, cmd):
        if name == wanted:
            print(worker)
            return


def start(argv, cmd):
    p = NamespacedOptionParser(argv)
    print("\n".join(worker
                        for _, worker, _ in multi_args(p, cmd)))

def expand(argv, cmd=None):
    template = argv[0]
    p = NamespacedOptionParser(argv[1:])
    for _, _, expander in multi_args(p, cmd):
        print(expander(template))



COMMANDS = {"start": start,
            "names": names,
            "expand": expand,
            "get": get}


def celeryd_multi(argv, cmd="celeryd"):
    return COMMANDS[argv[0]](argv[1:], cmd)


def main():
    celeryd_multi(sys.argv[1:])


if __name__ == "__main__":
    main()
