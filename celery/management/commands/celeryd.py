"""

Start the celery daemon from the Django management command.

"""
from django.core.management.base import BaseCommand

from celery.bin.celeryd import run_worker, OPTION_LIST


class Command(BaseCommand):
    """Run the celery daemon."""
    option_list = BaseCommand.option_list + OPTION_LIST
    help = 'Run the celery daemon'

    def handle(self, *args, **options):
        """Handle the management command."""
        run_worker(**options)
