from __future__ import absolute_import

from time import sleep

from kombu.utils.eventio import poll, POLL_READ, POLL_ERR


class Hub(object):
    eventflags = POLL_READ | POLL_ERR

    def __init__(self):
        self.fdmap = {}
        self.poller = poll()

    def __enter__(self):
        return self

    def __exit__(self, *exc_info):
        return self.close()

    def add(self, f, callback, flags=None):
        flags = self.eventflags if flags is None else flags
        fd = f.fileno()
        self.poller.register(fd, flags)
        self.fdmap[fd] = callback

    def update(self, *maps):
        [self.add(*x) for row in maps for x in row.iteritems()]

    def remove(self, fd):
        try:
            self.poller.unregister(fd)
        except (KeyError, OSError):
            pass

    def tick(self, timeout=1.0):
        if not self.fdmap:
            return sleep(0.1)
        for fileno, event in self.poller.poll(timeout) or ():
            try:
                self.fdmap[fileno]()
            except socket.timeout:
                pass
            except socket.error, exc:
                if exc.errno != errno.EAGAIN:
                    raise

    def close(self):
        [self.remove(fd) for fd in self.fdmap.keys()]
