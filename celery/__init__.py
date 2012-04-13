# -*- coding: utf-8 -*-
"""Distributed Task Queue"""
# :copyright: (c) 2009 - 2012 by Ask Solem.
# :license:   BSD, see LICENSE for more details.

from __future__ import absolute_import

<<<<<<< HEAD
VERSION = (2, 6, 0, "a1")
=======
VERSION = (2, 5, 2)
>>>>>>> 2.5
__version__ = ".".join(map(str, VERSION[0:3])) + "".join(VERSION[3:])
__author__ = "Ask Solem"
__contact__ = "ask@celeryproject.org"
__homepage__ = "http://celeryproject.org"
__docformat__ = "restructuredtext"

# -eof meta-

# Lazy loading
from .__compat__ import recreate_module


old_module, new_module = recreate_module(__name__,
    by_module={
        "celery.app":         ["Celery", "bugreport"],
        "celery.app.state":   ["current_app", "current_task"],
        "celery.task.sets":   ["chain", "group", "subtask"],
        "celery.task.chords": ["chord"],
    },
    direct={"task": "celery.task"},
    __package__="celery",
    __file__=__file__,
    __path__=__path__,
    __doc__=__doc__,
    __version__=__version__,
    __author__=__author__,
    __contact__=__contact__,
    __homepage__=__homepage__,
    __docformat__=__docformat__,
    VERSION=VERSION,
)
