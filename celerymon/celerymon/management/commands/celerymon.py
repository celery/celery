"""

Start the celery clock service from the Django management command.

"""
from django.core.management.base import BaseCommand

from celery.bin.celerymon import run_monitor, OPTION_LIST


class Command(BaseCommand):
    """Run the celery monitor."""
    option_list = BaseCommand.option_list + OPTION_LIST
    help = 'Run the celery monitor'

    def handle(self, *args, **options):
        """Handle the management command."""
        run_monitor(**options)
