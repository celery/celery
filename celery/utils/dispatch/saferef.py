# -*- coding: utf-8 -*-
"""
"Safe weakrefs", originally from pyDispatcher.

Provides a way to safely weakref any function, including bound methods (which
aren't handled by the core weakref module).
"""
from __future__ import absolute_import

import weakref
import traceback


def safe_ref(target, on_delete=None):
    """Return a *safe* weak reference to a callable target

    :param target: the object to be weakly referenced, if it's a
        bound method reference, will create a :class:`BoundMethodWeakref`,
        otherwise creates a simple :class:`weakref.ref`.

    :keyword on_delete: if provided, will have a hard reference stored
        to the callable to be called after the safe reference
        goes out of scope with the reference object, (either a
        :class:`weakref.ref` or a :class:`BoundMethodWeakref`) as argument.
    """
    if getattr(target, "im_self", None) is not None:
        # Turn a bound method into a BoundMethodWeakref instance.
        # Keep track of these instances for lookup by disconnect().
        assert hasattr(target, 'im_func'), \
            """safe_ref target %r has im_self, but no im_func, " \
            "don't know how to create reference""" % (target, )
        return get_bound_method_weakref(target=target,
                                        on_delete=on_delete)
    if callable(on_delete):
        return weakref.ref(target, on_delete)
    else:
        return weakref.ref(target)


class BoundMethodWeakref(object):
    """'Safe' and reusable weak references to instance methods.

    BoundMethodWeakref objects provide a mechanism for
    referencing a bound method without requiring that the
    method object itself (which is normally a transient
    object) is kept alive.  Instead, the BoundMethodWeakref
    object keeps weak references to both the object and the
    function which together define the instance method.

    .. attribute:: key

        the identity key for the reference, calculated
        by the class's :meth:`calculate_key` method applied to the
        target instance method

    .. attribute:: deletion_methods

        sequence of callable objects taking
        single argument, a reference to this object which
        will be called when *either* the target object or
        target function is garbage collected (i.e. when
        this object becomes invalid).  These are specified
        as the on_delete parameters of :func:`safe_ref` calls.

    .. attribute:: weak_self

        weak reference to the target object

    .. attribute:: weak_func

        weak reference to the target function

    .. attribute:: _all_instances

        class attribute pointing to all live
        BoundMethodWeakref objects indexed by the class's
        `calculate_key(target)` method applied to the target
        objects. This weak value dictionary is used to
        short-circuit creation so that multiple references
        to the same (object, function) pair produce the
        same BoundMethodWeakref instance.

    """

    _all_instances = weakref.WeakValueDictionary()

    def __new__(cls, target, on_delete=None, *arguments, **named):
        """Create new instance or return current instance

        Basically this method of construction allows us to
        short-circuit creation of references to already-
        referenced instance methods.  The key corresponding
        to the target is calculated, and if there is already
        an existing reference, that is returned, with its
        deletionMethods attribute updated.  Otherwise the
        new instance is created and registered in the table
        of already-referenced methods.

        """
        key = cls.calculate_key(target)
        current = cls._all_instances.get(key)
        if current is not None:
            current.deletion_methods.append(on_delete)
            return current
        else:
            base = super(BoundMethodWeakref, cls).__new__(cls)
            cls._all_instances[key] = base
            base.__init__(target, on_delete, *arguments, **named)
            return base

    def __init__(self, target, on_delete=None):
        """Return a weak-reference-like instance for a bound method

        :param target: the instance-method target for the weak
            reference, must have `im_self` and `im_func` attributes
            and be reconstructable via::

                target.im_func.__get__(target.im_self)

            which is true of built-in instance methods.

        :keyword on_delete: optional callback which will be called
            when this weak reference ceases to be valid
            (i.e. either the object or the function is garbage
            collected).  Should take a single argument,
            which will be passed a pointer to this object.

        """
        def remove(weak, self=self):
            """Set self.is_dead to true when method or instance is destroyed"""
            methods = self.deletion_methods[:]
            del(self.deletion_methods[:])
            try:
                del(self.__class__._all_instances[self.key])
            except KeyError:
                pass
            for function in methods:
                try:
                    if callable(function):
                        function(self)
                except Exception, exc:
                    try:
                        traceback.print_exc()
                    except AttributeError:
                        print("Exception during saferef %s cleanup function "
                              "%s: %s" % (self, function, exc))

        self.deletion_methods = [on_delete]
        self.key = self.calculate_key(target)
        self.weak_self = weakref.ref(target.im_self, remove)
        self.weak_func = weakref.ref(target.im_func, remove)
        self.self_name = str(target.im_self)
        self.func_name = str(target.im_func.__name__)

    def calculate_key(cls, target):
        """Calculate the reference key for this reference

        Currently this is a two-tuple of the `id()`'s of the
        target object and the target function respectively.
        """
        return id(target.im_self), id(target.im_func)
    calculate_key = classmethod(calculate_key)

    def __str__(self):
        """Give a friendly representation of the object"""
        return """%s( %s.%s )""" % (
            self.__class__.__name__,
            self.self_name,
            self.func_name,
        )

    __repr__ = __str__

    def __nonzero__(self):
        """Whether we are still a valid reference"""
        return self() is not None

    def __cmp__(self, other):
        """Compare with another reference"""
        if not isinstance(other, self.__class__):
            return cmp(self.__class__, type(other))
        return cmp(self.key, other.key)

    def __call__(self):
        """Return a strong reference to the bound method

        If the target cannot be retrieved, then will
        return None, otherwise returns a bound instance
        method for our object and function.

        Note:
            You may call this method any number of times,
            as it does not invalidate the reference.
        """
        target = self.weak_self()
        if target is not None:
            function = self.weak_func()
            if function is not None:
                return function.__get__(target)
        return None


class BoundNonDescriptorMethodWeakref(BoundMethodWeakref):
    """A specialized :class:`BoundMethodWeakref`, for platforms where
    instance methods are not descriptors.

    It assumes that the function name and the target attribute name are the
    same, instead of assuming that the function is a descriptor. This approach
    is equally fast, but not 100% reliable because functions can be stored on
    an attribute named differenty than the function's name such as in::

        >>> class A(object):
        ...     pass

        >>> def foo(self):
        ...     return "foo"
        >>> A.bar = foo

    But this shouldn't be a common use case. So, on platforms where methods
    aren't descriptors (such as Jython) this implementation has the advantage
    of working in the most cases.

    """
    def __init__(self, target, on_delete=None):
        """Return a weak-reference-like instance for a bound method

        :param target: the instance-method target for the weak
            reference, must have `im_self` and `im_func` attributes
            and be reconstructable via::

                target.im_func.__get__(target.im_self)

            which is true of built-in instance methods.

        :keyword on_delete: optional callback which will be called
            when this weak reference ceases to be valid
            (i.e. either the object or the function is garbage
            collected). Should take a single argument,
            which will be passed a pointer to this object.

        """
        assert getattr(target.im_self, target.__name__) == target, \
               "method %s isn't available as the attribute %s of %s" % (
                    target, target.__name__, target.im_self)
        super(BoundNonDescriptorMethodWeakref, self).__init__(target,
                                                              on_delete)

    def __call__(self):
        """Return a strong reference to the bound method

        If the target cannot be retrieved, then will
        return None, otherwise returns a bound instance
        method for our object and function.

        Note:
            You may call this method any number of times,
            as it does not invalidate the reference.

        """
        target = self.weak_self()
        if target is not None:
            function = self.weak_func()
            if function is not None:
                # Using curry() would be another option, but it erases the
                # "signature" of the function. That is, after a function is
                # curried, the inspect module can't be used to determine how
                # many arguments the function expects, nor what keyword
                # arguments it supports, and pydispatcher needs this
                # information.
                return getattr(target, function.__name__)
        return None


def get_bound_method_weakref(target, on_delete):
    """Instantiates the appropiate :class:`BoundMethodWeakRef`, depending
    on the details of the underlying class method implementation."""
    if hasattr(target, '__get__'):
        # target method is a descriptor, so the default implementation works:
        return BoundMethodWeakref(target=target, on_delete=on_delete)
    else:
        # no luck, use the alternative implementation:
        return BoundNonDescriptorMethodWeakref(target=target,
                                               on_delete=on_delete)
