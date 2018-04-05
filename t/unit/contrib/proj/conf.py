from __future__ import absolute_import, unicode_literals

import os
import sys

extensions = ['celery.contrib.sphinx']
autodoc_default_flags = ['members']

sys.path.insert(0, os.path.abspath('.'))
