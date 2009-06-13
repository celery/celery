import os
import sys
sys.path.append(os.getcwd())
django_project_dir = os.environ.get("DJANGO_PROJECT_DIR")
if django_project_dir:
    sys.path.append(django_project_dir)
from django.conf import settings
from optparse import OptionParser, make_option
from django.core.management.base import BaseCommand
from django.core.management import LaxOptionParser, ManagementUtility


class DiscardCommand(BaseCommand):
    """Discard all messages waiting in the task queue."""
    option_list = BaseCommand.option_list + (
            make_option('--error-empty', action="store_true",
            help="Exit with errorcode if the queue is empty."),
    )
    help = "Discard all messages waiting in the task queue."

    def handle(self, *args, **kwargs):
        from celery.task import discard_all
        error_empty = kwargs.get("error_empty", False)
        discarded = discard_all()
        if not discarded:
            sys.stderr.write("No tasks deleted: the queue is empty.\n")
            if error_empty:
                sys.exit(255)
        else:
            print("Deleted %d task(s) from the queue." % discarded)


COMMANDS = {
    "discard": DiscardCommand(),
}

class CeleryUtility(ManagementUtility):

    def __init__(self, *args, **kwargs):
        super(CeleryUtility, self).__init__(*args, **kwargs)
        self.is_manage_py = "manage" in self.prog_name
        if self.is_manage_py:
            self.prog_name = "%s celeryctl" % self.prog_name

    def fetch_command(self, subcommand):
        """Tries to fetch the given subcommand."""
        if subcommand not in COMMANDS:
            sys.stderr.write(
                "Unknown command: %r\nType '%s help' for usage\n" % (
                    subcommand, self.prog_name))
            sys.exit(1)
        return COMMANDS[subcommand]


def execute_from_command_line(argv=None):
    utility = CeleryUtility(argv)
    utility.execute()
