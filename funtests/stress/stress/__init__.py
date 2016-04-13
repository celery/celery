# -*- coding: utf-8 -*-
from __future__ import absolute_import, unicode_literals

import os
import time

from .data import install_json  # noqa

if os.environ.get('C_SLEEP'):

    _orig_sleep = time.sleep

    def _sleep(n):
        print('warning: time sleep for {0}s'.format(n))
        import traceback
        traceback.print_stack()
        _orig_sleep(n)
    time.sleep = _sleep

from .app import app  # noqa
