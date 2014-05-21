# -*- coding: utf-8 -*-
from __future__ import absolute_import

import os
import time

if os.environ.get('C_SLEEP'):

    _orig_sleep = time.sleep

    def _sleep(n):
        print('WARNING: Time sleep for {0}s'.format(n))
        import traceback
        traceback.print_stack()
        _orig_sleep(n)
    time.sleep = _sleep


from .app import app  # noqa
