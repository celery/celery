"""

Start the celery daemon from the Django management command.

"""
from django.core.management.base import BaseCommand
from celery.bin.celeryd import main, OPTION_LIST
from celery.conf import LOG_LEVELS


class Command(BaseCommand):
    """Run the celery daemon."""
    option_list = BaseCommand.option_list + OPTION_LIST
    help = 'Run the celery daemon'

    def handle(self, *args, **options):
        """Handle the management command."""
        if not isinstance(options.get('loglevel'), int):
            options['loglevel'] = LOG_LEVELS[options.get('loglevel').upper()]
        main(concurrency=options.get('concurrency'),
             daemon=options.get('daemon'),
             logfile=options.get('logfile'),
             loglevel=options.get('loglevel'),
             pidfile=options.get('pidfile'),
             queue_wakeup_after=options.get('queue_wakeup_after'))
