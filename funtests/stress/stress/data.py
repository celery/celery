# -*- coding: utf-8 -*-
from __future__ import absolute_import

import json

from celery.utils.debug import humanbytes
from celery.utils.imports import qualname

type_registry = {}


def json_reduce(obj, attrs):
    return {'py/obj': {'type': qualname(obj), 'attrs': attrs}}


def jsonable(cls):
    type_registry[qualname(cls)] = cls.__from_json__
    return cls


@jsonable
class Data(object):

    def __init__(self, label, data):
        self.label = label
        self.data = data

    def __str__(self):
        return '<Data: {0} ({1})>'.format(
            self.label, humanbytes(len(self.data)),
        )

    def __to_json__(self):
        return json_reduce(self, {'label': self.label, 'data': self.data})

    @classmethod
    def __from_json__(cls, label=None, data=None, **kwargs):
        return cls(label, data)

    def __reduce__(self):
        return Data, (self.label, self.data)
    __unicode__ = __repr__ = __str__

BIG = Data('BIG', 'x' * 2 ** 20 * 8)
SMALL = Data('SMALL', 'e' * 1024)


class JSONEncoder(json.JSONEncoder):

    def default(self, obj):
        try:
            return super(JSONEncoder, self).default(obj)
        except TypeError:
            reducer = getattr(obj, '__to_json__', None)
            if reducer:
                return reducer()
            raise


def decode_hook(d):
    try:
        d = d['py/obj']
    except KeyError:
        return d
    type_registry[d['type']](**d['attrs'])


def install_json():
    json._default_encoder = JSONEncoder()
    json._default_decoder.object_hook = decode_hook
install_json()  # ugh, ugly but it's a test suite after all
