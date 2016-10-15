# -*- coding: utf-8 -*-
"""Distributed Task Queue."""
# :copyright: (c) 2015-2016 Ask Solem.  All rights reserved.
# :copyright: (c) 2012-2014 GoPivotal, Inc., All rights reserved.
# :copyright: (c) 2009 - 2012 Ask Solem and individual contributors,
#                 All rights reserved.
# :license:   BSD (3 Clause), see LICENSE for more details.

from __future__ import absolute_import, print_function, unicode_literals

import os
import re
import sys

from collections import namedtuple

SERIES = '0today8'

__version__ = '4.0.0rc5'
__author__ = 'Ask Solem'
__contact__ = 'ask@celeryproject.org'
__homepage__ = 'http://celeryproject.org'
__docformat__ = 'restructuredtext'
__keywords__ = 'task job queue distributed messaging actor'

# -eof meta-

__all__ = [
    'Celery', 'bugreport', 'shared_task', 'task',
    'current_app', 'current_task', 'maybe_signature',
    'chain', 'chord', 'chunks', 'group', 'signature',
    'xmap', 'xstarmap', 'uuid',
]

VERSION_BANNER = '{0} ({1})'.format(__version__, SERIES)

version_info_t = namedtuple('version_info_t', (
    'major', 'minor', 'micro', 'releaselevel', 'serial',
))

# bumpversion can only search for {current_version}
# so we have to parse the version here.
_temp = re.match(
    r'(\d+)\.(\d+).(\d+)(.+)?', __version__).groups()
VERSION = version_info = version_info_t(
    int(_temp[0]), int(_temp[1]), int(_temp[2]), _temp[3] or '', '')
del _temp
del re

if os.environ.get('C_IMPDEBUG'):  # pragma: no cover
    from .five import builtins

    def debug_import(name, locals=None, globals=None,
                     fromlist=None, level=-1, real_import=builtins.__import__):
        glob = globals or getattr(sys, 'emarfteg_'[::-1])(1).f_globals
        importer_name = glob and glob.get('__name__') or 'unknown'
        print('-- {0} imports {1}'.format(importer_name, name))
        return real_import(name, locals, globals, fromlist, level)
    builtins.__import__ = debug_import

# This is never executed, but tricks static analyzers (PyDev, PyCharm,
# pylint, etc.) into knowing the types of these symbols, and what
# they contain.
STATICA_HACK = True
globals()['kcah_acitats'[::-1].upper()] = False
if STATICA_HACK:  # pragma: no cover
    from celery.app import shared_task                   # noqa
    from celery.app.base import Celery                   # noqa
    from celery.app.utils import bugreport               # noqa
    from celery.app.task import Task                     # noqa
    from celery._state import current_app, current_task  # noqa
    from celery.canvas import (                          # noqa
        chain, chord, chunks, group,
        signature, maybe_signature, xmap, xstarmap, subtask,
    )
    from celery.utils import uuid                        # noqa

# Eventlet/gevent patching must happen before importing
# anything else, so these tools must be at top-level.


def _find_option_with_arg(argv, short_opts=None, long_opts=None):
    """Search argv for options specifying short and longopt alternatives.

    Returns:
        str: value for option found
    Raises:
        KeyError: if option not found.
    """
    for i, arg in enumerate(argv):
        if arg.startswith('-'):
            if long_opts and arg.startswith('--'):
                name, sep, val = arg.partition('=')
                if name in long_opts:
                    return val if sep else argv[i + 1]
            if short_opts and arg in short_opts:
                return argv[i + 1]
    raise KeyError('|'.join(short_opts or [] + long_opts or []))


def _patch_eventlet():
    import eventlet
    import eventlet.debug

    eventlet.monkey_patch()
    blockdetect = float(os.environ.get('EVENTLET_NOBLOCK', 0))
    if blockdetect:
        eventlet.debug.hub_blocking_detection(blockdetect, blockdetect)


def _patch_gevent():
    import gevent
    from gevent import monkey, signal as gevent_signal

    monkey.patch_all()
    if gevent.version_info[0] == 0:  # pragma: no cover
        # Signals aren't working in gevent versions <1.0,
        # and aren't monkey patched by patch_all()
        _signal = __import__('signal')
        _signal.signal = gevent_signal


def maybe_patch_concurrency(argv=sys.argv,
                            short_opts=['-P'], long_opts=['--pool'],
                            patches={'eventlet': _patch_eventlet,
                                     'gevent': _patch_gevent}):
    """Apply eventlet/gevent monkeypatches.

    With short and long opt alternatives that specify the command line
    option to set the pool, this makes sure that anything that needs
    to be patched is completed as early as possible.
    (e.g., eventlet/gevent monkey patches).
    """
    try:
        pool = _find_option_with_arg(argv, short_opts, long_opts)
    except KeyError:
        pass
    else:
        try:
            patcher = patches[pool]
        except KeyError:
            pass
        else:
            patcher()

        # set up eventlet/gevent environments ASAP
        from celery import concurrency
        concurrency.get_implementation(pool)

# Lazy loading
from . import local  # noqa

old_module, new_module = local.recreate_module(  # pragma: no cover
    __name__,
    by_module={
        'celery.app': ['Celery', 'bugreport', 'shared_task'],
        'celery.app.task': ['Task'],
        'celery._state': ['current_app', 'current_task'],
        'celery.canvas': [
            'Signature', 'chain', 'chord', 'chunks', 'group',
            'signature', 'maybe_signature', 'subtask',
            'xmap', 'xstarmap',
        ],
        'celery.utils': ['uuid'],
    },
    direct={'task': 'celery.task'},
    __package__='celery', __file__=__file__,
    __path__=__path__, __doc__=__doc__, __version__=__version__,
    __author__=__author__, __contact__=__contact__,
    __homepage__=__homepage__, __docformat__=__docformat__, local=local,
    VERSION=VERSION, SERIES=SERIES, VERSION_BANNER=VERSION_BANNER,
    version_info_t=version_info_t,
    version_info=version_info,
    maybe_patch_concurrency=maybe_patch_concurrency,
    _find_option_with_arg=_find_option_with_arg,
    absolute_import=absolute_import,
    unicode_literals=unicode_literals,
    print_function=print_function,
)
