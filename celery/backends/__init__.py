"""celery.backends"""
from functools import partial
from django.conf import settings
import sys

DEFAULT_BACKEND = "database"
CELERY_BACKEND = getattr(settings, "CELERY_BACKEND", DEFAULT_BACKEND)


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

    Get the backend class specified in :settings:`CELERY_BACKEND`.

"""
get_default_backend_cls = partial(get_backend_cls, CELERY_BACKEND)


"""
.. class:: DefaultBackend

    The backend class specified in :setting:`CELERY_BACKEND`.

"""
DefaultBackend = get_default_backend_cls()

"""
.. data:: default_backend

    An instance of :class:`DefaultBackend`.

"""
default_backend = DefaultBackend()
