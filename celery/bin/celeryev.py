import sys

from celery import platforms
from celery.bin.base import Command, Option, daemon_options
from celery.platforms import create_daemon_context


class EvCommand(Command):
    supports_args = False

    def run(self, dump=False, camera=None, frequency=1.0, maxrate=None,
            loglevel="INFO", logfile=None, prog_name="celeryev",
            pidfile=None, uid=None, gid=None, umask=None,
            working_directory=None, detach=False, **kwargs):
        self.prog_name = prog_name

        if dump:
            return self.run_evdump()
        if camera:
            return self.run_evcam(camera, freq=frequency, maxrate=maxrate,
                                  loglevel=loglevel, logfile=logfile,
                                  pidfile=pidfile, uid=uid, gid=gid,
                                  umask=umask,
                                  working_directory=working_directory,
                                  detach=detach)
        return self.run_evtop()

    def run_evdump(self):
        from celery.events.dumper import evdump
        self.set_process_status("dump")
        return evdump(app=self.app)

    def run_evtop(self):
        from celery.events.cursesmon import evtop
        self.set_process_status("top")
        return evtop(app=self.app)

    def run_evcam(self, camera, logfile=None, pidfile=None, uid=None,
            gid=None, umask=None, working_directory=None,
            detach=False, **kwargs):
        from celery.events.snapshot import evcam
        self.set_process_status("cam")
        kwargs["app"] = self.app
        if not detach:
            return evcam(camera, logfile=logfile, pidfile=pidfile, **kwargs)
        context, on_stop = create_daemon_context(
                                logfile=logfile,
                                pidfile=pidfile,
                                uid=uid,
                                gid=gid,
                                umask=umask,
                                working_directory=working_directory)
        context.open()
        try:
            return evcam(camera, logfile=logfile, pidfile=pidfile, **kwargs)
        finally:
            on_stop()

    def set_process_status(self, prog, info=""):
        prog = "%s:%s" % (self.prog_name, prog)
        info = "%s %s" % (info, platforms.strargv(sys.argv))
        return platforms.set_process_title(prog, info=info)

    def get_options(self):
        return (
            Option('-d', '--dump',
                   action="store_true", dest="dump",
                   help="Dump events to stdout."),
            Option('-c', '--camera',
                   action="store", dest="camera",
                   help="Camera class to take event snapshots with."),
            Option('--detach',
                default=False, action="store_true", dest="detach",
                help="Recording: Detach and run in the background."),
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
        ) + daemon_options(default_pidfile="celeryev.pid",
                           default_logfile=None)


def main():
    ev = EvCommand()
    ev.execute_from_commandline()

if __name__ == "__main__":              # pragma: no cover
    main()
