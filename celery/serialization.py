from functools import partial as curry
import operator
try:
    import cPickle as pickle
except ImportError:
    import pickle


def find_nearest_pickleable_exception(exc):
    """With an exception instance, iterate over its super classes (by mro)
    and find the first super exception that is pickleable. It does
    not go below :exc:`Exception` (i.e. it skips :exc:`Exception`,
    :class:`BaseException` and :class:`object`). If that happens
    you should use :exc:`UnpickleableException` instead.

    :param exc: An exception instance.

    :returns: the nearest exception if it's not :exc:`Exception` or below,
        if it is it returns ``None``.

    :rtype: :exc:`Exception`

    """

    unwanted = (Exception, BaseException, object)
    is_unwanted = lambda exc: any(map(curry(operator.is_, exc), unwanted))

    mro_ = getattr(exc.__class__, "mro", lambda: [])
    for supercls in mro_():
        if is_unwanted(supercls):
            # only BaseException and object, from here on down,
            # we don't care about these.
            return None
        try:
            exc_args = getattr(exc, "args", [])
            superexc = supercls(*exc_args)
            pickle.dumps(superexc)
        except:
            pass
        else:
            return superexc
    return None


def create_exception_cls(name, module, parent=None):
    """Dynamically create an exception class."""
    if not parent:
        parent = Exception
    return type(name, (parent, ), {"__module__": module})


class UnpickleableExceptionWrapper(Exception):
    """Wraps unpickleable exceptions.

    :param exc_module: see :attr:`exc_module`.

    :param exc_cls_name: see :attr:`exc_cls_name`.

    :param exc_args: see :attr:`exc_args`

    .. attribute:: exc_module

        The module of the original exception.

    .. attribute:: exc_cls_name

        The name of the original exception class.

    .. attribute:: exc_args

        The arguments for the original exception.

    Example

        >>> try:
        ...     something_raising_unpickleable_exc()
        >>> except Exception, e:
        ...     exc = UnpickleableException(e.__class__.__module__,
        ...                                 e.__class__.__name__,
        ...                                 e.args)
        ...     pickle.dumps(exc) # Works fine.

    """

    def __init__(self, exc_module, exc_cls_name, exc_args):
        self.exc_module = exc_module
        self.exc_cls_name = exc_cls_name
        self.exc_args = exc_args
        super(Exception, self).__init__(exc_module, exc_cls_name, exc_args)


def get_pickleable_exception(exc):
    """Make sure exception is pickleable."""
    nearest = find_nearest_pickleable_exception(exc)
    if nearest:
        return nearest

    try:
        pickle.dumps(exc)
    except pickle.PickleError:
        excwrapper = UnpickleableExceptionWrapper(
                        exc.__class__.__module__,
                        exc.__class__.__name__,
                        getattr(exc, "args", []))
        return excwrapper


def get_pickled_exception(exc):
    """Get original exception from exception pickled using
    :meth:`get_pickleable_exception`."""
    if isinstance(exc, UnpickleableExceptionWrapper):
        exc_cls = create_exception_cls(exc.exc_cls_name,
                                       exc.exc_module)
        return exc_cls(*exc.exc_args)
    return exc
