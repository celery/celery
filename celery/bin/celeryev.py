import sys
import optparse

from celery.events.cursesmon import evtop
from celery.events.dumper import evdump
from celery.events.snapshot import evcam


OPTION_LIST = (
    optparse.make_option('-d', '--dump',
        action="store_true", dest="dump",
        help="Dump events to stdout."),
    optparse.make_option('-c', '--camera',
        action="store", dest="camera",
        help="Camera class to take event snapshots with."),
    optparse.make_option('-f', '--frequency', '--freq',
        action="store", dest="frequency", type="float", default=1.0,
        help="Recording: Snapshot frequency."),
    optparse.make_option('-x', '--verbose',
        action="store_true", dest="verbose",
        help="Show more output."),
    optparse.make_option('-r', '--maxrate',
        action="store", dest="maxrate", default=None,
        help="Recording: Shutter rate limit (e.g. 10/m)"),
)


def run_celeryev(dump=False, camera=None, frequency=1.0, maxrate=None,
        verbose=None, **kwargs):
    if dump:
        return evdump()
    if camera:
        return evcam(camera, frequency, maxrate, verbose=verbose)
    return evtop()


def parse_options(arguments):
    """Parse the available options to ``celeryev``."""
    parser = optparse.OptionParser(option_list=OPTION_LIST)
    options, values = parser.parse_args(arguments)
    return options


def main():
    options = parse_options(sys.argv[1:])
    return run_celeryev(**vars(options))

if __name__ == "__main__":
    main()
