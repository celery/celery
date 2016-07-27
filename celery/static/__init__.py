from __future__ import absolute_import, unicode_literals

import base64
import codecs
import os


def get_file(*args):
    return os.path.join(os.path.abspath(os.path.dirname(__file__)), *args)


def logo():
    return get_file('celery_128.png')


def logo_as_base64():
    with codecs.open(logo(), mode='rb') as fh:
        return base64.b64encode(fh.read())



