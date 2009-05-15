from functools import partial
from django.conf import settings
import sys

DEFAULT_BACKEND = "database"
CELERY_BACKEND = getattr(settings, "CELERY_BACKEND", DEFAULT_BACKEND)


def get_backend_cls(backend):
    if backend.find(".") == -1:
        backend = "celery.backends.%s" % backend
    __import__(backend)
    backend_module = sys.modules[backend]
    return getattr(backend_module, 'Backend')

get_default_backend_cls = partial(get_backend_cls, CELERY_BACKEND)
DefaultBackend = get_default_backend_cls()
default_backend = DefaultBackend()
