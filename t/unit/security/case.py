from __future__ import absolute_import, unicode_literals

from case import skip


@skip.unless_module('cryptography')
class SecurityCase:
    pass
