"""Entry-point for the :program:`celery` umbrella command."""
from __future__ import absolute_import, print_function, unicode_literals

import sys

from . import maybe_patch_concurrency

__all__ = ('main',)


def main():
    """Entrypoint to the ``celery`` umbrella command."""
    if 'multi' not in sys.argv:
        maybe_patch_concurrency()
    from celery.bin.celery import main as _main
    _main()


if __name__ == '__main__':  # pragma: no cover
    main()
