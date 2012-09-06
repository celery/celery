# -*- coding: utf-8 -*-
"""
    celery.utils.compat
    ~~~~~~~~~~~~~~~~~~~

    Compatibility implementations of features
    only available in newer Python versions.


"""
from __future__ import absolute_import

############## py3k #########################################################
import sys
is_py3k = sys.version_info[0] == 3

try:
    reload = reload                         # noqa
except NameError:                           # pragma: no cover
    from imp import reload                  # noqa

try:
    from UserList import UserList           # noqa
except ImportError:                         # pragma: no cover
    from collections import UserList        # noqa

try:
    from UserDict import UserDict           # noqa
except ImportError:                         # pragma: no cover
    from collections import UserDict        # noqa

if is_py3k:                                 # pragma: no cover
    from io import StringIO, BytesIO
    from .encoding import bytes_to_str

    class WhateverIO(StringIO):

        def write(self, data):
            StringIO.write(self, bytes_to_str(data))
else:
    from StringIO import StringIO           # noqa
    BytesIO = WhateverIO = StringIO         # noqa


############## collections.OrderedDict ######################################
try:
    from collections import OrderedDict
except ImportError:                         # pragma: no cover
    from ordereddict import OrderedDict     # noqa

############## format(int, ',d') ##########################

if sys.version_info >= (2, 7):  # pragma: no cover
    def format_d(i):
        return format(i, ',d')
else:  # pragma: no cover
    def format_d(i):  # noqa
        s = '%d' % i
        groups = []
        while s and s[-1].isdigit():
            groups.append(s[-3:])
            s = s[:-3]
        return s + ','.join(reversed(groups))
