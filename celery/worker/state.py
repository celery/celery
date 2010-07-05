from celery.utils.compat import defaultdict
from celery.datastructures import LimitedSet

REVOKES_MAX = 10000
REVOKE_EXPIRES = 3600 # One hour.

active = defaultdict(lambda: 0)
total = defaultdict(lambda: 0)
revoked = LimitedSet(maxlen=REVOKES_MAX, expires=REVOKE_EXPIRES)


def task_accepted(task_name):
    active[task_name] += 1
    total[task_name] += 1


def task_ready(task_name):
    active[task_name] -= 1
    if active[task_name] == 0:
        active.pop(task_name, None)
