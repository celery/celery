"""The ``celery purge`` program, used to delete messages from queues."""
from __future__ import absolute_import, unicode_literals

from celery.bin.base import Command
from celery.five import keys
from celery.utils import text


class purge(Command):
    """Erase all messages from all known task queues.

    Warning:
        There's no undo operation for this command.
    """

    warn_prelude = (
        '{warning}: This will remove all tasks from {queues}: {names}.\n'
        '         There is no undo for this operation!\n\n'
        '(to skip this prompt use the -f option)\n'
    )
    warn_prompt = 'Are you sure you want to delete all tasks'

    fmt_purged = 'Purged {mnum} {messages} from {qnum} known task {queues}.'
    fmt_empty = 'No messages purged from {qnum} {queues}'

    def add_arguments(self, parser):
        group = parser.add_argument_group('Purging Options')
        group.add_argument(
            '--force', '-f', action='store_true', default=False,
            help="Don't prompt for verification",
        )
        group.add_argument(
            '--queues', '-Q', default=[],
            help='Comma separated list of queue names to purge.',
        )
        group.add_argument(
            '--exclude-queues', '-X', default=[],
            help='Comma separated list of queues names not to purge.',
        )

    def run(self, force=False, queues=None, exclude_queues=None, **kwargs):
        queues = set(text.str_to_list(queues or []))
        exclude = set(text.str_to_list(exclude_queues or []))
        names = (queues or set(keys(self.app.amqp.queues))) - exclude
        qnum = len(names)

        messages = None
        if names:
            if not force:
                self.out(self.warn_prelude.format(
                    warning=self.colored.red('WARNING'),
                    queues=text.pluralize(qnum, 'queue'),
                    names=', '.join(sorted(names)),
                ))
                if self.ask(self.warn_prompt, ('yes', 'no'), 'no') != 'yes':
                    return
            with self.app.connection_for_write() as conn:
                messages = sum(self._purge(conn, queue) for queue in names)
        fmt = self.fmt_purged if messages else self.fmt_empty
        self.out(fmt.format(
            mnum=messages, qnum=qnum,
            messages=text.pluralize(messages, 'message'),
            queues=text.pluralize(qnum, 'queue')))

    def _purge(self, conn, queue):
        try:
            return conn.default_channel.queue_purge(queue) or 0
        except conn.channel_errors:
            return 0
