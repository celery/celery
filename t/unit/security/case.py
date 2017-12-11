<<<<<<< HEAD
from __future__ import absolute_import, unicode_literals

=======
>>>>>>> 7ee75fa9882545bea799db97a40cc7879d35e726
from case import skip


@skip.unless_module('OpenSSL.crypto', name='pyOpenSSL')
class SecurityCase:
    pass
