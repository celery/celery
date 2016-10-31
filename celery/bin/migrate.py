"""The ``celery migrate`` command, used to filter and move messages."""
from __future__ import absolute_import, unicode_literals
from celery.bin.base import Command

MIGRATE_PROGRESS_FMT = """\
Migrating task {state.count}/{state.strtotal}: \
{body[task]}[{body[id]}]\
"""


class migrate(Command):
    """Migrate tasks from one broker to another.

    Warning:
        This command is experimental, make sure you have a backup of
        the tasks before you continue.

    Example:
        .. code-block:: console

            $ celery migrate amqp://A.example.com amqp://guest@B.example.com//
            $ celery migrate redis://localhost amqp://guest@localhost//
    """

    args = '<source_url> <dest_url>'
    progress_fmt = MIGRATE_PROGRESS_FMT

    def add_arguments(self, parser):
        group = parser.add_argument_group('Migration Options')
        group.add_argument(
            '--limit', '-n', type=int,
            help='Number of tasks to consume (int)',
        )
        group.add_argument(
            '--timeout', '-t', type=float, default=1.0,
            help='Timeout in seconds (float) waiting for tasks',
        )
        group.add_argument(
            '--ack-messages', '-a', action='store_true', default=False,
            help='Ack messages from source broker.',
        )
        group.add_argument(
            '--tasks', '-T',
            help='List of task names to filter on.',
        )
        group.add_argument(
            '--queues', '-Q',
            help='List of queues to migrate.',
        )
        group.add_argument(
            '--forever', '-F', action='store_true', default=False,
            help='Continually migrate tasks until killed.',
        )

    def on_migrate_task(self, state, body, message):
        self.out(self.progress_fmt.format(state=state, body=body))

    def run(self, source, destination, **kwargs):
        from kombu import Connection
        from celery.contrib.migrate import migrate_tasks

        migrate_tasks(Connection(source),
                      Connection(destination),
                      callback=self.on_migrate_task,
                      **kwargs)
