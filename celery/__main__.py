"""Entry-point for the :program:`celery` umbrella command."""

import sys


__all__ = ('main',)


def main():
    """Entrypoint to the ``celery`` umbrella command."""
    from celery.bin.celery import main as _main
    sys.exit(_main())


if __name__ == '__main__':  # pragma: no cover
    main()
