"""

Celery AMQP Administration Tool using the AMQP API.

"""
from django.core.management.base import BaseCommand

from celery.bin.camqadm import camqadm, OPTION_LIST


class Command(BaseCommand):
    """Run the celery daemon."""
    option_list = BaseCommand.option_list + OPTION_LIST
    help = 'Celery AMQP Administration Tool using the AMQP API.'

    def handle(self, *args, **options):
        """Handle the management command."""
        camqadm(*args, **options)
