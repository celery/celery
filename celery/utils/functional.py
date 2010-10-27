"""Functional utilities for Python 2.4 compatibility."""
# License for code in this file that was taken from Python 2.5.

# PYTHON SOFTWARE FOUNDATION LICENSE VERSION 2
# --------------------------------------------
#
# 1. This LICENSE AGREEMENT is between the Python Software Foundation
# ("PSF"), and the Individual or Organization ("Licensee") accessing and
# otherwise using this software ("Python") in source or binary form and
# its associated documentation.
#
# 2. Subject to the terms and conditions of this License Agreement, PSF
# hereby grants Licensee a nonexclusive, royalty-free, world-wide
# license to reproduce, analyze, test, perform and/or display publicly,
# prepare derivative works, distribute, and otherwise use Python
# alone or in any derivative version, provided, however, that PSF's
# License Agreement and PSF's notice of copyright, i.e., "Copyright (c)
# 2001, 2002, 2003, 2004, 2005, 2006, 2007 Python Software Foundation;
# All Rights Reserved" are retained in Python alone or in any derivative
# version prepared by Licensee.
#
# 3. In the event Licensee prepares a derivative work that is based on
# or incorporates Python or any part thereof, and wants to make
# the derivative work available to others as provided herein, then
# Licensee hereby agrees to include in any such work a brief summary of
# the changes made to Python.
#
# 4. PSF is making Python available to Licensee on an "AS IS"
# basis.  PSF MAKES NO REPRESENTATIONS OR WARRANTIES, EXPRESS OR
# IMPLIED.  BY WAY OF EXAMPLE, BUT NOT LIMITATION, PSF MAKES NO AND
# DISCLAIMS ANY REPRESENTATION OR WARRANTY OF MERCHANTABILITY OR FITNESS
# FOR ANY PARTICULAR PURPOSE OR THAT THE USE OF PYTHON WILL NOT
# INFRINGE ANY THIRD PARTY RIGHTS.
#
# 5. PSF SHALL NOT BE LIABLE TO LICENSEE OR ANY OTHER USERS OF PYTHON
# FOR ANY INCIDENTAL, SPECIAL, OR CONSEQUENTIAL DAMAGES OR LOSS AS
# A RESULT OF MODIFYING, DISTRIBUTING, OR OTHERWISE USING PYTHON,
# OR ANY DERIVATIVE THEREOF, EVEN IF ADVISED OF THE POSSIBILITY THEREOF.
#
# 6. This License Agreement will automatically terminate upon a material
# breach of its terms and conditions.
#
# 7. Nothing in this License Agreement shall be deemed to create any
# relationship of agency, partnership, or joint venture between PSF and
# Licensee.  This License Agreement does not grant permission to use PSF
# trademarks or trade name in a trademark sense to endorse or promote
# products or services of Licensee, or any third party.
#
# 8. By copying, installing or otherwise using Python, Licensee
# agrees to be bound by the terms and conditions of this License
# Agreement.

### Begin from Python 2.5 functools.py ########################################

# Summary of changes made to the Python 2.5 code below:
#   * Wrapped the `setattr` call in `update_wrapper` with a try-except
#     block to make it compatible with Python 2.3, which doesn't allow
#     assigning to `__name__`.

# Copyright (c) 2001, 2002, 2003, 2004, 2005, 2006, 2007 Python Software
# Foundation. All Rights Reserved.

###############################################################################

# update_wrapper() and wraps() are tools to help write
# wrapper functions that can handle naive introspection


def _compat_partial(fun, *args, **kwargs):
    """New function with partial application of the given arguments
    and keywords."""

    def _curried(*addargs, **addkwargs):
        return fun(*(args + addargs), **dict(kwargs, **addkwargs))
    return _curried


try:
    from functools import partial
except ImportError:
    partial = _compat_partial

WRAPPER_ASSIGNMENTS = ('__module__', '__name__', '__doc__')
WRAPPER_UPDATES = ('__dict__',)


def _compat_update_wrapper(wrapper, wrapped, assigned=WRAPPER_ASSIGNMENTS,
        updated=WRAPPER_UPDATES):
    """Update a wrapper function to look like the wrapped function

       wrapper is the function to be updated
       wrapped is the original function
       assigned is a tuple naming the attributes assigned directly
       from the wrapped function to the wrapper function (defaults to
       functools.WRAPPER_ASSIGNMENTS)
       updated is a tuple naming the attributes off the wrapper that
       are updated with the corresponding attribute from the wrapped
       function (defaults to functools.WRAPPER_UPDATES)

    """
    for attr in assigned:
        try:
            setattr(wrapper, attr, getattr(wrapped, attr))
        except TypeError:   # Python 2.3 doesn't allow assigning to __name__.
            pass
    for attr in updated:
        getattr(wrapper, attr).update(getattr(wrapped, attr))
    # Return the wrapper so this can be used as a decorator via partial()
    return wrapper

try:
    from functools import update_wrapper
except ImportError:
    update_wrapper = _compat_update_wrapper


def _compat_wraps(wrapped, assigned=WRAPPER_ASSIGNMENTS,
        updated=WRAPPER_UPDATES):
    """Decorator factory to apply update_wrapper() to a wrapper function

    Returns a decorator that invokes update_wrapper() with the decorated
    function as the wrapper argument and the arguments to wraps() as the
    remaining arguments. Default arguments are as for update_wrapper().
    This is a convenience function to simplify applying partial() to
    update_wrapper().

    """
    return partial(update_wrapper, wrapped=wrapped,
                   assigned=assigned, updated=updated)

try:
    from functools import wraps
except ImportError:
    wraps = _compat_wraps

### End from Python 2.5 functools.py ##########################################
