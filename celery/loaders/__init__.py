# -*- coding: utf-8 -*-
"""
    celery.loaders
    ~~~~~~~~~~~~~~

    Loaders define how configuration is read, what happens
    when workers start, when tasks are executed and so on.

    :copyright: (c) 2009 - 2012 by Ask Solem.
    :license: BSD, see LICENSE for more details.

"""
from __future__ import absolute_import

from celery.utils import deprecated
from celery.utils.imports import symbol_by_name

LOADER_ALIASES = {"app": "celery.loaders.app:AppLoader",
                  "default": "celery.loaders.default:Loader",
                  "django": "djcelery.loaders:DjangoLoader"}

def get_loader_cls(loader):
    """Get loader class by name/alias"""
    return symbol_by_name(loader, LOADER_ALIASES)


@deprecated(deprecation="2.5", removal="3.0",
        alternative="celery.current_app.loader")
def current_loader():
    from celery.app.state import current_app
    return current_app.loader


@deprecated(deprecation="2.5", removal="3.0",
            alternative="celery.current_app.conf")
def load_settings():
    from celery.app.state import current_app
    return current_app.conf
