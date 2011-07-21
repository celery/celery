class SecurityError(Exception):
    """Security related exceptions"""
    def __init__(self, msg, exc=None, *args, **kwargs):
        Exception.__init__(self, msg, exc, *args, **kwargs)

