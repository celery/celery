from __future__ import absolute_import, unicode_literals

import os
import sys

extensions = ['celery.contrib.sphinx', 'sphinx.ext.autodoc']
autodoc_default_flags = ['members']
autosummary_generate = True

sys.path.insert(0, os.path.abspath('.'))
