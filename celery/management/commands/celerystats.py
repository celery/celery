"""

Start the celery daemon from the Django management command.

"""
from django.core.management.base import BaseCommand
from celery.monitoring import StatsCollector


class Command(BaseCommand):
    """Run the celery daemon."""
    option_list = BaseCommand.option_list
    help = "Collect/flush and dump a report from the currently available "
           "statistics"

    def handle(self, *args, **options):
        """Handle the management command."""
        stats = StatsCollector()
        print("* Gathering statistics...")
        stats.collect()
        stats.report()
