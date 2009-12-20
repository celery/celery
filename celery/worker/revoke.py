from celery.datastructures import LimitedSet

REVOKES_MAX = 10000
REVOKE_EXPIRES = 3600 # One hour.

revoked = LimitedSet(maxlen=REVOKES_MAX, expires=REVOKE_EXPIRES)
