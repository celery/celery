from celery.datastructures import LimitedSet

REVOKES_MAX = 1000
REVOKE_EXPIRES = 60 * 60 # one hour.

revoked = LimitedSet(maxlen=REVOKES_MAX, expires=REVOKE_EXPIRES)
