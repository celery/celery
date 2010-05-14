"""Distributed Task Queue"""

VERSION = (1, 0, 3, "-pre2")

__version__ = ".".join(map(str, VERSION[0:3])) + "".join(VERSION[3:])
__author__ = "Ask Solem"
__contact__ = "askh@opera.com"
__homepage__ = "http://github.com/ask/celery/"
__docformat__ = "restructuredtext"


def is_stable_release():
    if len(VERSION) > 3 and isinstance(VERSION[3], basestring):
        return False
    return not VERSION[1] % 2


def version_with_meta():
    return "%s (%s)" % (__version__,
                        is_stable_release() and "stable" or "unstable")
