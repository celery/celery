"""The ``celery result`` program, used to inspect task results."""
from __future__ import absolute_import, unicode_literals

from celery.bin.base import Command


class result(Command):
    """Gives the return value for a given task id.

    Examples:
        .. code-block:: console

            $ celery result 8f511516-e2f5-4da4-9d2f-0fb83a86e500
            $ celery result 8f511516-e2f5-4da4-9d2f-0fb83a86e500 -t tasks.add
            $ celery result 8f511516-e2f5-4da4-9d2f-0fb83a86e500 --traceback
    """

    args = '<task_id>'

    def add_arguments(self, parser):
        group = parser.add_argument_group('Result Options')
        group.add_argument(
            '--task', '-t', help='name of task (if custom backend)',
        )
        group.add_argument(
            '--traceback', action='store_true', default=False,
            help='show traceback instead',
        )

    def run(self, task_id, *args, **kwargs):
        result_cls = self.app.AsyncResult
        task = kwargs.get('task')
        traceback = kwargs.get('traceback', False)

        if task:
            result_cls = self.app.tasks[task].AsyncResult
        task_result = result_cls(task_id)
        if traceback:
            value = task_result.traceback
        else:
            value = task_result.get()
        self.out(self.pretty(value)[1])
