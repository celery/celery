"""

Start the celery daemon from the Django management command.

"""
from django.core.management.base import BaseCommand

import celery.models            # <-- shows upgrade instructions at exit.


class Command(BaseCommand):
    """Run the celery daemon."""
    help = 'Run the celery daemon'

    def handle(self, *args, **options):
        pass
