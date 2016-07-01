"""Entry-point for the :program:`celery` umbrella command."""
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
