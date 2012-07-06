# -*- coding: utf-8 -*-
"""
    celery.bin.celeryctl
    ~~~~~~~~~~~~~~~~~~~~

    Now replaced by the :program:`celery` command.

"""
from __future__ import absolute_import

from celery.bin.celery import (  # noqa
    CeleryCommand as celeryctl, Command, main,
)

if __name__ == '__main__':  # pragma: no cover
    main()
