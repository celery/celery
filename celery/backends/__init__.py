"""celery.backends"""
from functools import partial
from celery import conf
import sys


def get_backend_cls(backend):
    """Get backend class by name.

    If the name does not include "``.``" (is not fully qualified),
    ``"celery.backends."`` will be prepended to the name. e.g.
    ``"database"`` becomes ``"celery.backends.database"``.

    """
    if backend.find(".") == -1:
        backend = "celery.backends.%s" % backend
    __import__(backend)
    backend_module = sys.modules[backend]
    return getattr(backend_module, "Backend")

"""
.. function:: get_default_backend_cls()

    Get the backend class specified in :setting:`CELERY_BACKEND`.

"""
get_default_backend_cls = partial(get_backend_cls, conf.CELERY_BACKEND)


"""
.. class:: DefaultBackend

    The default backend class used for storing task results and status,
    specified in :setting:`CELERY_BACKEND`.

"""
DefaultBackend = get_default_backend_cls()


"""
.. data:: default_backend

    An instance of :class:`DefaultBackend`.

"""
default_backend = DefaultBackend()
