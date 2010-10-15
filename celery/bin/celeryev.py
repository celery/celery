import sys
import platform

from celery import platforms
from celery.bin.base import Command, Option


class EvCommand(Command):

    def run(self, dump=False, camera=None, frequency=1.0, maxrate=None,
            loglevel="INFO", logfile=None, prog_name="celeryev", **kwargs):
        self.prog_name = prog_name

        if dump:
            return self.run_evdump()
        if camera:
            return self.run_evcam(camera, frequency, maxrate,
                                  loglevel=loglevel, logfile=logfile)
        return self.run_evtop()

    def run_evdump(self):
        from celery.events.dumper import evdump
        self.set_process_status("dump")
        return evdump(app=self.app)

    def run_evtop(self):
        from celery.events.cursesmon import evtop
        self.set_process_status("top")
        return evtop(app=self.app)

    def run_evcam(self, *args, **kwargs):
        from celery.events.snapshot import evcam
        self.set_process_status("cam")
        kwargs["app"] = self.app
        return evcam(*args, **kwargs)

    def set_process_status(self, prog, info=""):
        prog = "%s:%s" % (self.prog_name, prog)
        info = "%s %s" % (info, platforms.strargv(sys.argv))
        return platform.set_process_title(prog, info=info)

    def get_options(self):
        return (
            Option('-d', '--dump',
                   action="store_true", dest="dump",
                   help="Dump events to stdout."),
            Option('-c', '--camera',
                   action="store", dest="camera",
                   help="Camera class to take event snapshots with."),
            Option('-F', '--frequency', '--freq',
                   action="store", dest="frequency",
                   type="float", default=1.0,
                   help="Recording: Snapshot frequency."),
            Option('-r', '--maxrate',
                   action="store", dest="maxrate", default=None,
                   help="Recording: Shutter rate limit (e.g. 10/m)"),
            Option('-l', '--loglevel',
                   action="store", dest="loglevel", default="INFO",
                   help="Loglevel. Default is WARNING."),
            Option('-f', '--logfile',
                   action="store", dest="logfile", default=None,
                   help="Log file. Default is <stderr>"),
        )


def main():
    ev = EvCommand()
    ev.execute_from_commandline()

if __name__ == "__main__":              # pragma: no cover
    main()
