# -*- coding: utf-8 -*-
"""Pool implementation abstract factory, and alias definitions."""
from __future__ import absolute_import, unicode_literals

# Import from kombu directly as it's used
# early in the import stage, where celery.utils loads
# too much (e.g., for eventlet patching)
from kombu.utils.imports import symbol_by_name

__all__ = ['get_implementation']

ALIASES = {
    'prefork': 'celery.concurrency.prefork:TaskPool',
    'eventlet': 'celery.concurrency.eventlet:TaskPool',
    'gevent': 'celery.concurrency.gevent:TaskPool',
    'solo': 'celery.concurrency.solo:TaskPool',
    'processes': 'celery.concurrency.prefork:TaskPool',  # XXX compat alias
}


def get_implementation(cls):
    """Return pool implementation by name."""
    return symbol_by_name(cls, ALIASES)
