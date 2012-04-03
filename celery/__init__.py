# -*- coding: utf-8 -*-
"""Distributed Task Queue"""
# :copyright: (c) 2009 - 2012 by Ask Solem.
# :license:   BSD, see LICENSE for more details.

from __future__ import absolute_import

VERSION = (2, 6, 0, "a1")
__version__ = ".".join(map(str, VERSION[0:3])) + "".join(VERSION[3:])
__author__ = "Ask Solem"
__contact__ = "ask@celeryproject.org"
__homepage__ = "http://celeryproject.org"
__docformat__ = "restructuredtext"

# -eof meta-

import sys

if sys.version_info < (2, 5):
    raise Exception(
        "Python 2.4 is not supported by this version. "
        "Please use Celery versions 2.1.x or earlier.")

# Lazy loading
from types import ModuleType
from .local import Proxy


compat_modules = ("messaging", "log", "registry", "decorators")


class module(ModuleType):
    __all__ = ("Celery", "current_app", "bugreport")

    def __getattr__(self, name):
        if name in compat_modules:
            from .__compat__ import get_compat
            setattr(self, name, get_compat(self.current_app, self, name))
        return ModuleType.__getattribute__(self, name)

    def __dir__(self):
        result = list(new_module.__all__)
        result.extend(("__file__", "__path__", "__doc__", "__all__",
                       "__docformat__", "__name__", "__path__", "VERSION",
                       "__package__", "__version__", "__author__",
                       "__contact__", "__homepage__", "__docformat__"))
        return result

# 2.5 does not define __package__
try:
    package = __package__
except NameError:
    package = "kombu"

# keep a reference to this module so that it's not garbage collected
old_module = sys.modules[__name__]

new_module = sys.modules[__name__] = module(__name__)
new_module.__dict__.update({
    "__file__": __file__,
    "__path__": __path__,
    "__doc__": __doc__,
    "__version__": __version__,
    "__author__": __author__,
    "__contact__": __contact__,
    "__homepage__": __homepage__,
    "__docformat__": __docformat__,
    "__package__": package,
    "VERSION": VERSION})

def Celery(*args, **kwargs):
    from .app import App
    return App(*args, **kwargs)

def _get_current_app():
    from .app import current_app
    return current_app()
current_app = Proxy(_get_current_app)

def bugreport():
    return current_app.bugreport()

new_module.Celery = Celery
new_module.current_app = current_app
new_module.bugreport = bugreport

