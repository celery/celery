# -*- coding: utf-8 -*-
"""Distributed Task Queue"""
# :copyright: (c) 2009 - 2012 Ask Solem and individual contributors,
#                 All rights reserved.
# :copyright: (c) 2012 VMware, Inc., All rights reserved.
# :license:   BSD (3 Clause), see LICENSE for more details.

from __future__ import absolute_import

SERIES = 'Chiastic Slide'
VERSION = (3, 0, 19)
__version__ = '.'.join(str(p) for p in VERSION[0:3]) + ''.join(VERSION[3:])
__author__ = 'Ask Solem'
__contact__ = 'ask@celeryproject.org'
__homepage__ = 'http://celeryproject.org'
__docformat__ = 'restructuredtext'
__all__ = [
    'Celery', 'bugreport', 'shared_task', 'Task',
    'current_app', 'current_task',
    'chain', 'chord', 'chunks', 'group', 'subtask',
    'xmap', 'xstarmap', 'uuid', 'VERSION', '__version__',
]
VERSION_BANNER = '%s (%s)' % (__version__, SERIES)

# -eof meta-

STATICA_HACK = True
globals()['kcah_acitats'[::-1].upper()] = False
if STATICA_HACK:
    # This is never executed, but tricks static analyzers (PyDev, PyCharm,
    # pylint, etc.) into knowing the types of these symbols, and what
    # they contain.
    from celery.app.base import Celery
    from celery.app.utils import bugreport
    from celery.app.task import Task
    from celery._state import current_app, current_task
    from celery.canvas import (
        chain, chord, chunks, group, subtask, xmap, xstarmap,
    )
    from celery.utils import uuid

# Lazy loading
from .__compat__ import recreate_module

old_module, new_module = recreate_module(  # pragma: no cover
    __name__,
    by_module={
        'celery.app': ['Celery', 'bugreport', 'shared_task'],
        'celery.app.task': ['Task'],
        'celery._state': ['current_app', 'current_task'],
        'celery.canvas': ['chain', 'chord', 'chunks', 'group',
                          'subtask', 'xmap', 'xstarmap'],
        'celery.utils': ['uuid'],
    },
    direct={'task': 'celery.task'},
    __package__='celery', __file__=__file__,
    __path__=__path__, __doc__=__doc__, __version__=__version__,
    __author__=__author__, __contact__=__contact__,
    __homepage__=__homepage__, __docformat__=__docformat__,
    VERSION=VERSION, SERIES=SERIES, VERSION_BANNER=VERSION_BANNER,
)
