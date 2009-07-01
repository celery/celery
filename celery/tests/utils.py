from __future__ import with_statement
from contextlib import contextmanager
from StringIO import StringIO
import os
import sys
import __builtin__
 

@contextmanager
def mask_modules(*modnames):
    """Ban some modules from being importable inside the context
 
    For example:
 
        >>> with missing_modules("sys"):
        ...     try:
        ...         import sys
        ...     except ImportError:
        ...         print "sys not found"
        sys not found
 
        >>> import sys
        >>> sys.version
        (2, 5, 2, 'final', 0)
 
    """
 
    realimport = __builtin__.__import__
    
    def myimp(name, *args, **kwargs):
        if name in modnames:
            raise ImportError("No module named %s" % name)
        else:
            return realimport(name, *args, **kwargs)

    __builtin__.__import__ = myimp
    yield
    __builtin__.__import__ = realimport


class OverrideStdout(object):
    """Override ``sys.stdout`` and ``sys.stderr`` with ``StringIO``."""

    def __enter__(self):
        mystdout = StringIO()
        mystderr = StringIO()
        sys.stdout = mystdout
        sys.stderr = mystderr
        return mystdout, mystderr

    def __exit__(self, e_type, e_value, e_trace):
        if e_type:
            raise e_type(e_value)
        sys.stdout = sys.__stdout__
        sys.stderr = sys.__stderr__
