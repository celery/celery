from case import skip


@skip.unless_module('OpenSSL.crypto', name='pyOpenSSL')
class SecurityCase:
    pass
