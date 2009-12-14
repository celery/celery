from celery.datastructures import LimitedSet

REVOKES_MAX = 10000
REVOKE_EXPIRES = 3600 # One hour.

"""

.. data:: revoked

    A :class:`celery.datastructures.LimitedSet` containing revoked task ids.

    Items expire after one hour, and the structure can only hold
    10000 expired items at a time (about 300kb).

"""
revoked = LimitedSet(maxlen=REVOKES_MAX, expires=REVOKE_EXPIRES)
