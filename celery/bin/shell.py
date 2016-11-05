"""The ``celery shell`` program, used to start a REPL."""
from __future__ import absolute_import, unicode_literals
import os
import sys
from importlib import import_module
from celery.five import values
from celery.bin.base import Command


class shell(Command):  # pragma: no cover
    """Start shell session with convenient access to celery symbols.

    The following symbols will be added to the main globals:

        - ``celery``:  the current application.
        - ``chord``, ``group``, ``chain``, ``chunks``,
          ``xmap``, ``xstarmap`` ``subtask``, ``Task``
        - all registered tasks.
    """

    def add_arguments(self, parser):
        group = parser.add_argument_group('Shell Options')
        group.add_argument(
            '--ipython', '-I',
            action='store_true', help='force iPython.', default=False,
        )
        group.add_argument(
            '--bpython', '-B',
            action='store_true', help='force bpython.', default=False,
        )
        group.add_argument(
            '--python',
            action='store_true', default=False,
            help='force default Python shell.',
        )
        group.add_argument(
            '--without-tasks', '-T',
            action='store_true', default=False,
            help="don't add tasks to locals.",
        )
        group.add_argument(
            '--eventlet',
            action='store_true', default=False,
            help='use eventlet.',
        )
        group.add_argument(
            '--gevent', action='store_true', default=False,
            help='use gevent.',
        )

    def run(self, *args, **kwargs):
        if args:
            raise self.UsageError(
                'shell command does not take arguments: {0}'.format(args))
        return self._run(**kwargs)

    def _run(self, ipython=False, bpython=False,
             python=False, without_tasks=False, eventlet=False,
             gevent=False, **kwargs):
        sys.path.insert(0, os.getcwd())
        if eventlet:
            import_module('celery.concurrency.eventlet')
        if gevent:
            import_module('celery.concurrency.gevent')
        import celery
        import celery.task.base
        self.app.loader.import_default_modules()

        # pylint: disable=attribute-defined-outside-init
        self.locals = {
            'app': self.app,
            'celery': self.app,
            'Task': celery.Task,
            'chord': celery.chord,
            'group': celery.group,
            'chain': celery.chain,
            'chunks': celery.chunks,
            'xmap': celery.xmap,
            'xstarmap': celery.xstarmap,
            'subtask': celery.subtask,
            'signature': celery.signature,
        }

        if not without_tasks:
            self.locals.update({
                task.__name__: task for task in values(self.app.tasks)
                if not task.name.startswith('celery.')
            })

        if python:
            return self.invoke_fallback_shell()
        elif bpython:
            return self.invoke_bpython_shell()
        elif ipython:
            return self.invoke_ipython_shell()
        return self.invoke_default_shell()

    def invoke_default_shell(self):
        try:
            import IPython  # noqa
        except ImportError:
            try:
                import bpython  # noqa
            except ImportError:
                return self.invoke_fallback_shell()
            else:
                return self.invoke_bpython_shell()
        else:
            return self.invoke_ipython_shell()

    def invoke_fallback_shell(self):
        import code
        try:
            import readline
        except ImportError:
            pass
        else:
            import rlcompleter
            readline.set_completer(
                rlcompleter.Completer(self.locals).complete)
            readline.parse_and_bind('tab:complete')
        code.interact(local=self.locals)

    def invoke_ipython_shell(self):
        for ip in (self._ipython, self._ipython_pre_10,
                   self._ipython_terminal, self._ipython_010,
                   self._no_ipython):
            try:
                return ip()
            except ImportError:
                pass

    def _ipython(self):
        from IPython import start_ipython
        start_ipython(argv=[], user_ns=self.locals)

    def _ipython_pre_10(self):  # pragma: no cover
        from IPython.frontend.terminal.ipapp import TerminalIPythonApp
        app = TerminalIPythonApp.instance()
        app.initialize(argv=[])
        app.shell.user_ns.update(self.locals)
        app.start()

    def _ipython_terminal(self):  # pragma: no cover
        from IPython.terminal import embed
        embed.TerminalInteractiveShell(user_ns=self.locals).mainloop()

    def _ipython_010(self):  # pragma: no cover
        from IPython.Shell import IPShell
        IPShell(argv=[], user_ns=self.locals).mainloop()

    def _no_ipython(self):  # pragma: no cover
        raise ImportError('no suitable ipython found')

    def invoke_bpython_shell(self):
        import bpython
        bpython.embed(self.locals)
