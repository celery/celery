# -*- coding: utf-8 -*-
"""The :program:`celery logtool` command.

.. program:: celery logtool
"""

from __future__ import absolute_import, unicode_literals

import re

from collections import Counter
from fileinput import FileInput

from .base import Command

__all__ = ['logtool']

RE_LOG_START = re.compile(r'^\[\d\d\d\d\-\d\d-\d\d ')
RE_TASK_RECEIVED = re.compile(r'.+?\] Received')
RE_TASK_READY = re.compile(r'.+?\] Task')
RE_TASK_INFO = re.compile(r'.+?([\w\.]+)\[(.+?)\].+')
RE_TASK_RESULT = re.compile(r'.+?[\w\.]+\[.+?\] (.+)')

REPORT_FORMAT = """
Report
======

Task total: {task[total]}
Task errors: {task[errors]}
Task success: {task[succeeded]}
Task completed: {task[completed]}

Tasks
=====
{task[types].format}
"""


class _task_counts(list):

    @property
    def format(self):
        return '\n'.join('{0}: {1}'.format(*i) for i in self)


def task_info(line):
    m = RE_TASK_INFO.match(line)
    return m.groups()


class Audit(object):

    def __init__(self, on_task_error=None, on_trace=None, on_debug=None):
        self.ids = set()
        self.names = {}
        self.results = {}
        self.ready = set()
        self.task_types = Counter()
        self.task_errors = 0
        self.on_task_error = on_task_error
        self.on_trace = on_trace
        self.on_debug = on_debug
        self.prev_line = None

    def run(self, files):
        for line in FileInput(files):
            self.feed(line)
        return self

    def task_received(self, line, task_name, task_id):
        self.names[task_id] = task_name
        self.ids.add(task_id)
        self.task_types[task_name] += 1

    def task_ready(self, line, task_name, task_id, result):
        self.ready.add(task_id)
        self.results[task_id] = result
        if 'succeeded' not in result:
            self.task_error(line, task_name, task_id, result)

    def task_error(self, line, task_name, task_id, result):
        self.task_errors += 1
        if self.on_task_error:
            self.on_task_error(line, task_name, task_id, result)

    def feed(self, line):
        if RE_LOG_START.match(line):
            if RE_TASK_RECEIVED.match(line):
                task_name, task_id = task_info(line)
                self.task_received(line, task_name, task_id)
            elif RE_TASK_READY.match(line):
                task_name, task_id = task_info(line)
                result = RE_TASK_RESULT.match(line)
                if result:
                    result, = result.groups()
                self.task_ready(line, task_name, task_id, result)
            else:
                if self.on_debug:
                    self.on_debug(line)
            self.prev_line = line
        else:
            if self.on_trace:
                self.on_trace('\n'.join(filter(None, [self.prev_line, line])))
            self.prev_line = None

    def incomplete_tasks(self):
        return self.ids ^ self.ready

    def report(self):
        return {
            'task': {
                'types': _task_counts(self.task_types.most_common()),
                'total': len(self.ids),
                'errors': self.task_errors,
                'completed': len(self.ready),
                'succeeded': len(self.ready) - self.task_errors,
            }
        }


class logtool(Command):
    """The ``celery logtool`` command."""

    args = """<action> [arguments]
            .....  stats      [file1|- [file2 [...]]]
            .....  traces     [file1|- [file2 [...]]]
            .....  errors     [file1|- [file2 [...]]]
            .....  incomplete [file1|- [file2 [...]]]
            .....  debug      [file1|- [file2 [...]]]
    """

    def run(self, what=None, *files, **kwargs):
        map = {
            'stats': self.stats,
            'traces': self.traces,
            'errors': self.errors,
            'incomplete': self.incomplete,
            'debug': self.debug,
        }
        if not what:
            raise self.UsageError('missing action')
        elif what not in map:
            raise self.Error(
                'action {0} not in {1}'.format(what, '|'.join(map)),
            )

        return map[what](files)

    def stats(self, files):
        self.out(REPORT_FORMAT.format(
            **Audit().run(files).report()
        ))

    def traces(self, files):
        Audit(on_trace=self.out).run(files)

    def errors(self, files):
        Audit(on_task_error=self.say1).run(files)

    def incomplete(self, files):
        audit = Audit()
        audit.run(files)
        for task_id in audit.incomplete_tasks():
            self.error('Did not complete: %r' % (task_id,))

    def debug(self, files):
        Audit(on_debug=self.out).run(files)

    def say1(self, line, *_):
        self.out(line)
