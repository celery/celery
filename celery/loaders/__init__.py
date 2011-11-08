# -*- coding: utf-8 -*-
"""
    celery.loaders
    ~~~~~~~~~~~~~~

    Loaders define how configuration is read, what happens
    when workers start, when tasks are executed and so on.

    :copyright: (c) 2009 - 2011 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from .. import current_app
from ..utils import deprecated, get_cls_by_name

LOADER_ALIASES = {"app": "celery.loaders.app.AppLoader",
                  "default": "celery.loaders.default.Loader",
                  "django": "djcelery.loaders.DjangoLoader"}


def get_loader_cls(loader):
    """Get loader class by name/alias"""
    return get_cls_by_name(loader, LOADER_ALIASES)


@deprecated(deprecation="2.5", removal="3.0",
        alternative="celery.current_app.loader")
def current_loader():
    return current_app.loader


@deprecated(deprecation="2.5", removal="3.0",
            alternative="celery.current_app.conf")
def load_settings():
    return current_app.conf
