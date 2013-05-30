# -*- coding: utf-8 -*-
from __future__ import absolute_import

from celery.utils.debug import humanbytes


class Data(object):

    def __init__(self, label, data):
        self.label = label
        self.data = data

    def __str__(self):
        return '<Data: {0} ({1})>'.format(
            self.label, humanbytes(len(self.data)),
        )

    def __reduce__(self):
        return Data, (self.label, self.data)
    __unicode__ = __repr__ = __str__

BIG = Data("BIG", 'x' * 2 ** 20 * 8)
SMALL = Data("SMALL", 'e' * 1024)
