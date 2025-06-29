# This file is part of 'click-plugins': https://github.com/click-contrib/click-plugins
#
# New BSD License
#
# Copyright (c) 2015-2025, Kevin D. Wurster, Sean C. Gillies
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are met:
#
# * Redistributions of source code must retain the above copyright notice, this
#   list of conditions and the following disclaimer.
#
# * Redistributions in binary form must reproduce the above copyright notice,
#   this list of conditions and the following disclaimer in the documentation
#   and/or other materials provided with the distribution.
#
# * Neither click-plugins nor the names of its contributors may not be used to
#   endorse or promote products derived from this software without specific prior
#   written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
# AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
# IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
# FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
# DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
# CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
# OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


"""Support CLI plugins with click and entry points.

See :func:`with_plugins`.
"""


import importlib.metadata
import os
import sys
import traceback

import click


__version__ = '2.0'


def with_plugins(entry_points):

    """Decorator for loading and attaching plugins to a ``click.Group()``.

    Plugins are loaded from an ``importlib.metadata.EntryPoint()``. Each entry
    point must point to a ``click.Command()``. An entry point that fails to
    load will be wrapped in a ``BrokenCommand()`` to allow the CLI user to
    discover and potentially debug the problem.

    >>> from importlib.metadata import entry_points
    >>>
    >>> import click
    >>> from click_plugins import with_plugins
    >>>
    >>> @with_plugins('group_name')
    >>> @click.group()
    >>> def group():
    ...     '''Group'''
    >>>
    >>> @with_plugins(entry_points('group_name'))
    >>> @click.group()
    >>> def group():
    ...     '''Group'''
    >>>
    >>> @with_plugins(importlib.metadata.EntryPoint(...))
    >>> @click.group()
    >>> def group():
    ...     '''Group'''
    >>>
    >>> @with_plugins("group1")
    >>> @with_plugins("group2")
    >>> def group():
    ...     '''Group'''

    :param str or EntryPoint or sequence[EntryPoint] entry_points:
        Entry point group name, a single ``importlib.metadata.EntryPoint()``,
        or a sequence of ``EntryPoint()``s.

    :rtype function:
    """

    # Note that the explicit full path reference to:
    #
    #     importlib.metadata.entry_points()
    #
    # in this function allows the call to be mocked in the tests. Replacing
    # with:
    #
    # from importlib.metadata import entry_points
    #
    # breaks this ability.

    def decorator(group):
        if not isinstance(group, click.Group):
            raise TypeError(
                f"plugins can only be attached to an instance of"
                f" 'click.Group()' not: {repr(group)}")

        # Load 'EntryPoint()' objects.
        if isinstance(entry_points, str):

            # Older versions of Python do not support filtering.
            if sys.version_info >= (3, 10):
                all_entry_points = importlib.metadata.entry_points(
                    group=entry_points)

            else:
                all_entry_points = importlib.metadata.entry_points()
                all_entry_points = all_entry_points[entry_points]

        # A single 'importlib.metadata.EntryPoint()'
        elif isinstance(entry_points, importlib.metadata.EntryPoint):
            all_entry_points = [entry_points]

        # Sequence of 'EntryPoints()'.
        else:
            all_entry_points = entry_points

        for ep in all_entry_points:

            try:
                group.add_command(ep.load())

            # Catch all exceptions (technically not 'BaseException') and
            # instead register a special 'BrokenCommand()'. Otherwise, a single
            # plugin that fails to load and/or register will make the CLI
            # inoperable. 'BrokenCommand()' explains the situation to users.
            except Exception as e:
                group.add_command(BrokenCommand(ep, e))

        return group

    return decorator


class BrokenCommand(click.Command):

    """Represents a plugin ``click.Command()`` that failed to load.

    Can be executed just like a ``click.Command()``, but prints information
    for debugging and exits with an error code.
    """

    def __init__(self, entry_point, exception):

        """
        :param importlib.metadata.EntryPoint entry_point:
            Entry point that failed to load.
        :param Exception exception:
            Raised when attempting to load the entry point associated with
            this instance.
        """

        super().__init__(entry_point.name)

        # There are several ways to get a traceback from an exception, but
        # 'TracebackException()' seems to be the most portable across actively
        # supported versions of Python.
        tbe = traceback.TracebackException.from_exception(exception)

        # A message for '$ cli command --help'. Contains full traceback and a
        # helpful note. The intention is to nudge users to figure out which
        # project should get a bug report since users are likely to report the
        # issue to the developers of the CLI utility they are directly
        # interacting with. These are not necessarily the right developers.
        self.help = (
            "{ls}ERROR: entry point '{module}:{name}' could not be loaded."
            " Contact its author for help.{ls}{ls}{tb}").format(
            module=_module(entry_point),
            name=entry_point.name,
            ls=os.linesep,
            tb=''.join(tbe.format())
        )

        # Replace the broken command's summary with a warning about how it
        # was not loaded successfully. The idea is that '$ cli --help' should
        # include a clear indicator that a subcommand is not functional, and
        # a little hint for what to do about it. U+2020 is a "dagger", whose
        # modern use typically indicates a footnote.
        self.short_help = (
            f"\u2020 Warning: could not load plugin. Invoke command with"
            f" '--help' for traceback."
        )

    def invoke(self, ctx):

        """Print traceback and debugging message.

        :param click.Context ctx:
            Active context.
        """

        click.echo(self.help, color=ctx.color, err=True)
        ctx.exit(1)

    def parse_args(self, ctx, args):

        """Pass arguments along without parsing.

        :param click.Context ctx:
            Active context.
        :param list args:
            List of command line arguments.
        """

        # Do not attempt to parse these arguments. We do not know why the
        # entry point failed to load, but it is reasonable to assume that
        # argument parsing will not work. Ultimately the goal is to get the
        # 'Command.invoke()' method (overloaded in this class) to execute
        # and provide the user with a bit of debugging information.

        return args


def _module(ep):

    """Module name for a given entry point.

    Parameters
    ----------
    ep : importlib.metadata.EntryPoint
        Determine parent module for this entry point.

    Returns
    -------
    str
    """

    if sys.version_info >= (3, 10):
        module = ep.module

    else:
        # From 'importlib.metadata.EntryPoint.module'.
        match = ep.pattern.match(ep.value)
        module = match.group('module')

    return module
