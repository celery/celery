import logging
import optparse
import sys

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
    optparse.make_option('-F', '--frequency', '--freq',
        action="store", dest="frequency", type="float", default=1.0,
        help="Recording: Snapshot frequency."),
    optparse.make_option('-r', '--maxrate',
        action="store", dest="maxrate", default=None,
        help="Recording: Shutter rate limit (e.g. 10/m)"),
    optparse.make_option('-l', '--loglevel',
        action="store", dest="loglevel", default="WARNING",
        help="Loglevel. Default is WARNING."),
    optparse.make_option('-f', '--logfile',
        action="store", dest="logfile", default=None,
        help="Log file. Default is <stderr>"),
)


def run_celeryev(dump=False, camera=None, frequency=1.0, maxrate=None,
        loglevel=logging.WARNING, logfile=None, **kwargs):
    if dump:
        return evdump()
    if camera:
        return evcam(camera, frequency, maxrate,
                     loglevel=loglevel, logfile=logfile)
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
