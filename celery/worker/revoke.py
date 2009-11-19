import time
from UserDict import UserDict

from carrot.connection import DjangoBrokerConnection

from celery.messaging import BroadcastPublisher
from celery.utils import noop

REVOKES_MAX = 1000
REVOKE_EXPIRES = 60 * 60 # one hour.


class RevokeRegistry(UserDict):

    def __init__(self, maxlen=REVOKES_MAX, expires=REVOKE_EXPIRES):
        self.maxlen = maxlen
        self.expires = expires
        self.data = {}

    def add(self, uuid):
        self._expire_item()
        self[uuid] = time.time()

    def _expire_item(self):
        while 1:
            if len(self) > self.maxlen:
                uuid, when = self.oldest
                if time.time() > when + self.expires:
                    try:
                        self.pop(uuid, None)
                    except TypeError:
                        continue
            break

    @property
    def oldest(self):
        return sorted(self.items(), key=lambda (uuid, when): when)[0]


def revoke(uuid, connection=None):
    conn = connection or DjangoBrokerConnection()
    close_connection = not connection and conn.close or noop

    broadcast = BroadcastPublisher(conn)
    try:
        broadcast.send({"revoke": uuid})
    finally:
        broadcast.close()
        close_connection()

revoked = RevokeRegistry()


