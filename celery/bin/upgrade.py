"""The ``celery upgrade`` command, used to upgrade from previous versions."""
from __future__ import absolute_import, print_function, unicode_literals

import codecs

from celery.app import defaults
from celery.bin.base import Command
from celery.utils.functional import pass1


class upgrade(Command):
    """Perform upgrade between versions."""

    choices = {'settings'}

    def add_arguments(self, parser):
        group = parser.add_argument_group('Upgrading Options')
        group.add_argument(
            '--django', action='store_true', default=False,
            help='Upgrade Django project',
        )
        group.add_argument(
            '--compat', action='store_true', default=False,
            help='Maintain backwards compatibility',
        )
        group.add_argument(
            '--no-backup', action='store_true', default=False,
            help='Dont backup original files',
        )

    def usage(self, command):
        return '%(prog)s <command> settings [filename] [options]'

    def run(self, *args, **kwargs):
        try:
            command = args[0]
        except IndexError:
            raise self.UsageError(
                'missing upgrade type: try `celery upgrade settings` ?')
        if command not in self.choices:
            raise self.UsageError('unknown upgrade type: {0}'.format(command))
        return getattr(self, command)(*args, **kwargs)

    def settings(self, command, filename=None,
                 no_backup=False, django=False, compat=False, **kwargs):

        if filename is None:
            raise self.UsageError('missing settings filename to upgrade')

        lines = self._slurp(filename)
        keyfilter = self._compat_key if django or compat else pass1
        print('processing {0}...'.format(filename), file=self.stderr)
        # gives list of tuples: ``(did_change, line_contents)``
        new_lines = [
            self._to_new_key(line, keyfilter) for line in lines
        ]
        if any(n[0] for n in new_lines):  # did have changes
            if not no_backup:
                self._backup(filename)
            with codecs.open(filename, 'w', 'utf-8') as write_fh:
                for _, line in new_lines:
                    write_fh.write(line)
            print('Changes to your setting have been made!',
                  file=self.stdout)
        else:
            print('Does not seem to require any changes :-)',
                  file=self.stdout)

    def _slurp(self, filename):
        with codecs.open(filename, 'r', 'utf-8') as read_fh:
            return [line for line in read_fh]

    def _backup(self, filename, suffix='.orig'):
        lines = []
        backup_filename = ''.join([filename, suffix])
        print('writing backup to {0}...'.format(backup_filename),
              file=self.stderr)
        with codecs.open(filename, 'r', 'utf-8') as read_fh:
            with codecs.open(backup_filename, 'w', 'utf-8') as backup_fh:
                for line in read_fh:
                    backup_fh.write(line)
                    lines.append(line)
        return lines

    def _to_new_key(self, line, keyfilter=pass1, source=defaults._TO_NEW_KEY):
        # sort by length to avoid, for example, broker_transport overriding
        # broker_transport_options.
        for old_key in reversed(sorted(source, key=lambda x: len(x))):
            new_line = line.replace(old_key, keyfilter(source[old_key]))
            if line != new_line and 'CELERY_CELERY' not in new_line:
                return 1, new_line  # only one match per line.
        return 0, line

    def _compat_key(self, key, namespace='CELERY'):
        key = key.upper()
        if not key.startswith(namespace):
            key = '_'.join([namespace, key])
        return key
