from __future__ import absolute_import, unicode_literals

import os


def get_file(*args):
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), *args)


def logo():
    return get_file('celery_128.png')
