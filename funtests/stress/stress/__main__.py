from __future__ import absolute_import, print_function

from celery.bin.base import Command, Option

from .app import app
from .suite import Suite


class Stress(Command):

    def run(self, *names, **options):
        try:
            return Suite(
                self.app,
                block_timeout=options.get('block_timeout'),
            ).run(names, **options)
        except KeyboardInterrupt:
            pass

    def get_options(self):
        return (
            Option('-i', '--iterations', type='int', default=50,
                   help='Number of iterations for each test'),
            Option('-n', '--numtests', type='int', default=None,
                   help='Number of tests to execute'),
            Option('-o', '--offset', type='int', default=0,
                   help='Start at custom offset'),
            Option('--block-timeout', type='int', default=30 * 60),
            Option('-l', '--list', action='store_true', dest='list_all',
                   help='List all tests'),
            Option('-r', '--repeat', type='float', default=0,
                   help='Number of times to repeat the test suite'),
            Option('-g', '--group', default='all',
                   help='Specify test group (all|green)'),
            Option('--diag', default=False, action='store_true',
                   help='Enable diagnostics (slow)'),
            Option('-J', '--no-join', default=False, action='store_true',
                   help='Do not wait for task results'),
        )


if __name__ == '__main__':
    Stress(app=app).execute_from_commandline()
