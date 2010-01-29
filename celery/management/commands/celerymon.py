"""

Start the celery clock service from the Django management command.

"""
import sys
from django.core.management.base import BaseCommand

#try:
from celerymonitor.bin.celerymond import run_monitor, OPTION_LIST
#except ImportError:
#    OPTION_LIST = ()
#    run_monitor = None

MISSING = """
You don't have celerymon installed, please install it by running the following
command:

    $ easy_install celerymon

or if you're using pip (like you should be):

    $ pip install celerymon
"""


class Command(BaseCommand):
    """Run the celery monitor."""
    option_list = BaseCommand.option_list + OPTION_LIST
    help = 'Run the celery monitor'

    def handle(self, *args, **options):
        """Handle the management command."""
        if run_monitor is None:
            sys.stderr.write(MISSING)
        else:
            run_monitor(**options)
