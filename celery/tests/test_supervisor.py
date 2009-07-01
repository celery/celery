import unittest
from celery.supervisor import raise_ping_timeout, OFASupervisor
from celery.supervisor import TimeoutError, MaxRestartsExceededError


def target_one(x, y, z):
    return x * y * z


class MockProcess(object):
    _started = False
    _stopped = False
    _terminated = False
    _joined = False
    alive = True
    timeout_on_is_alive = False

    def __init__(self, target, args, kwargs):
        self.target = target
        self.args = args
        self.kwargs = kwargs

    def start(self):
        self._stopped = False
        self._started = True

    def stop(self):
        self._stopped = True
        self._started = False

    def terminate(self):
        self._terminated = False

    def is_alive(self):
        if self._started and self.alive:
            if self.timeout_on_is_alive:
                raise TimeoutError("Supervised: timed out.")
            return True
        return False

    def join(self, timeout=None):
        self._joined = True


class TestDiv(unittest.TestCase):

    def test_raise_ping_timeout(self):
        self.assertRaises(TimeoutError, raise_ping_timeout, "timed out")


class TestOFASupervisor(unittest.TestCase):

    def test_init(self):
        s = OFASupervisor(target=target_one, args=[2, 4, 8], kwargs={})
        s.Process = MockProcess

    def test__is_alive(self):
        s = OFASupervisor(target=target_one, args=[2, 4, 8], kwargs={})
        s.Process = MockProcess
        proc = MockProcess(target_one, [2, 4, 8], {})
        proc.start()
        self.assertTrue(s._is_alive(proc))
        proc.alive = False
        self.assertFalse(s._is_alive(proc))

    def test_start(self):
        MockProcess.alive = False
        s = OFASupervisor(target=target_one, args=[2, 4, 8], kwargs={},
                          max_restart_freq=0, max_restart_freq_time=0)
        s.Process = MockProcess
        self.assertRaises(MaxRestartsExceededError, s.start)
        MockProcess.alive = True

    def test_start_is_alive_timeout(self):
        MockProcess.alive = True
        MockProcess.timeout_on_is_alive = True
        s = OFASupervisor(target=target_one, args=[2, 4, 8], kwargs={},
                          max_restart_freq=0, max_restart_freq_time=0)
        s.Process = MockProcess
        self.assertRaises(MaxRestartsExceededError, s.start)
        MockProcess.timeout_on_is_alive = False
