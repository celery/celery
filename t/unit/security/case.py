from case import skip


@skip.unless_module('cryptography')
class SecurityCase:
    pass
