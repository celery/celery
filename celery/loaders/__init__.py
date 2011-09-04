from __future__ import absolute_import

import os

from .. import current_app
from ..utils import get_cls_by_name

LOADER_ALIASES = {"app": "celery.loaders.app.AppLoader",
                  "default": "celery.loaders.default.Loader",
                  "django": "djcelery.loaders.DjangoLoader"}


def get_loader_cls(loader):
    """Get loader class by name/alias"""
    return get_cls_by_name(loader, LOADER_ALIASES)


def setup_loader():     # XXX Deprecate
    return get_loader_cls(os.environ.setdefault("CELERY_LOADER", "default"))()


def current_loader():   # XXX Deprecate
    return current_app.loader


def load_settings():    # XXX Deprecate
    return current_app.conf
