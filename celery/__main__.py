from __future__ import absolute_import

import sys


def maybe_patch_concurrency():
    from celery.platforms import maybe_patch_concurrency
    maybe_patch_concurrency(sys.argv, ['-P'], ['--pool'])


def main():
    maybe_patch_concurrency()
    from celery.bin.celery import main
    main()


def _compat_worker():
    maybe_patch_concurrency()
    from celery.bin.celeryd import main
    main()


def _compat_multi():
    maybe_patch_concurrency()
    from celery.bin.celeryd_multi import main
    main()


def _compat_beat():
    maybe_patch_concurrency()
    from celery.bin.celerybeat import main
    main()


if __name__ == '__main__':
    main()
