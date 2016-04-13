from __future__ import absolute_import, print_function, unicode_literals

import sys

from . import maybe_patch_concurrency

__all__ = ['main']


def main():
    if 'multi' not in sys.argv:
        maybe_patch_concurrency()
    from celery.bin.celery import main
    main()


if __name__ == '__main__':  # pragma: no cover
    main()
