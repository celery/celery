"""

Start the celery clock service from the Django management command.

"""
from django.core.management.base import BaseCommand

from celery.bin.celerybeat import run_clockservice, OPTION_LIST


class Command(BaseCommand):
    """Run the celery periodic task scheduler."""
    option_list = BaseCommand.option_list + OPTION_LIST
    help = 'Run the celery periodic task scheduler'

    def handle(self, *args, **options):
        """Handle the management command."""
        run_clockservice(**options)
