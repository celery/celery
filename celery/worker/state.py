import shelve

from celery.utils.compat import defaultdict
from celery.datastructures import LimitedSet

REVOKES_MAX = 10000
REVOKE_EXPIRES = 3600 # One hour.

active_requests = set()
total_count = defaultdict(lambda: 0)
revoked = LimitedSet(maxlen=REVOKES_MAX, expires=REVOKE_EXPIRES)


def task_accepted(request):
    active_requests.add(request)
    total_count[request.task_name] += 1


def task_ready(request):
    try:
        active_requests.remove(request)
    except KeyError:
        pass


class Persistent(object):
    _open = None

    def __init__(self, filename):
        self.filename = filename
        self._load()

    def _load(self):
        self.merge(self.db)
        self.close()

    def save(self):
        self.sync(self.db).sync()
        self.close()

    def merge(self, d):
        revoked.update(d.get("revoked") or {})
        return d

    def sync(self, d):
        prev = d.get("revoked") or {}
        prev.update(revoked.as_dict())
        d["revoked"] = prev
        return d

    def open(self):
        return shelve.open(self.filename)

    def close(self):
        if self._open:
            self._open.close()
            self._open = None

    @property
    def db(self):
        if self._open is None:
            self._open = self.open()
        return self._open
