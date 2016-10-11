# -*- coding: utf-8 -*-
"""Get loader by name.

Loaders define how configuration is read, what happens
when workers start, when tasks are executed and so on.
"""
from __future__ import absolute_import, unicode_literals

from celery.utils.imports import symbol_by_name, import_from_cwd

__all__ = ['get_loader_cls']

LOADER_ALIASES = {
    'app': 'celery.loaders.app:AppLoader',
    'default': 'celery.loaders.default:Loader',
    'django': 'djcelery.loaders:DjangoLoader',
}


def get_loader_cls(loader):
    """Get loader class by name/alias."""
    return symbol_by_name(loader, LOADER_ALIASES, imp=import_from_cwd)
