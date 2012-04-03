# -*- coding: utf-8 -*-
from __future__ import absolute_import
from __future__ import with_statement

if __name__ == "__main__" and __package__ is None:
    __package__ = "celery.bin.celeryctl"

from .celery import CeleryCommand as celeryctl, main  # noqa

if __name__ == "__main__":  # pragma: no cover
    main()
