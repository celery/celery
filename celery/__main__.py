from __future__ import absolute_import

import sys


def main():
    from celery.platforms import maybe_patch_concurrency
    maybe_patch_concurrency(sys.argv, ['-P'], ['--pool'])
    from celery.bin.celery import main
    main()

if __name__ == '__main__':
    main()
