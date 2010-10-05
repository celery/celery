import logging
import sys

from optparse import OptionParser, make_option as Option

from celery import platforms
from celery.events.cursesmon import evtop
from celery.events.dumper import evdump
from celery.events.snapshot import evcam


OPTION_LIST = (
    Option('-d', '--dump',
        action="store_true", dest="dump",
        help="Dump events to stdout."),
    Option('-c', '--camera',
        action="store", dest="camera",
        help="Camera class to take event snapshots with."),
    Option('-F', '--frequency', '--freq',
        action="store", dest="frequency", type="float", default=1.0,
        help="Recording: Snapshot frequency."),
    Option('-r', '--maxrate',
        action="store", dest="maxrate", default=None,
        help="Recording: Shutter rate limit (e.g. 10/m)"),
    Option('-l', '--loglevel',
        action="store", dest="loglevel", default="WARNING",
        help="Loglevel. Default is WARNING."),
    Option('-f', '--logfile',
        action="store", dest="logfile", default=None,
        help="Log file. Default is <stderr>"),
)


def set_process_status(prog, info=""):
        info = "%s %s" % (info, platforms.strargv(sys.argv))
        return platforms.set_process_title(prog,
                                           info=info)


def run_celeryev(dump=False, camera=None, frequency=1.0, maxrate=None,
        loglevel=logging.WARNING, logfile=None, **kwargs):
    if dump:
        set_process_status("celeryev:dump")
        return evdump()
    if camera:
        set_process_status("celeryev:cam")
        return evcam(camera, frequency, maxrate,
                     loglevel=loglevel, logfile=logfile)
    set_process_status("celeryev:top")
    return evtop()


def parse_options(arguments):
    """Parse the available options to ``celeryev``."""
    parser = OptionParser(option_list=OPTION_LIST)
    options, values = parser.parse_args(arguments)
    return options


def main():
    options = parse_options(sys.argv[1:])
    return run_celeryev(**vars(options))

if __name__ == "__main__":              # pragma: no cover
    main()
