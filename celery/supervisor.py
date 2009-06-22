from multiprocessing import Process, TimeoutError
import threading
import time

PING_TIMEOUT = 30 # seconds
JOIN_TIMEOUT = 2 
CHECK_INTERVAL = 2
MAX_RESTART_FREQ = 3
MAX_RESTART_FREQ_TIME = 10


def raise_ping_timeout():
    raise TimeoutError("Supervised: Timed out while pinging process.")


class OFASupervisor(object):
    """Process supervisor using the `one_for_all`_ strategy.
   
    .. _`one_for_all`:
        http://erlang.org/doc/design_principles/sup_princ.html#5.3.2
    
    """

    def __init__(self, target, args=None, kwargs=None,
            ping_timeout=PING_TIMEOUT, join_timeout=JOIN_TIMEOUT,
            max_restart_freq_time=MAX_RESTART_FREQ_TIME,
            max_restart_freq = MAX_RESTART_FREQ,
            check_interval=CHECK_INTERVAL):
        self.target = target
        self.args = args or []
        self.kwargs = kwargs or {}
        self.ping_timeout = ping_timeout
        self.join_timeout = join_timeout
        self.check_interval = check_interval
        self.max_restart_freq = max_restart_freq
        self.max_restart_freq_time = max_restart_freq_time
        self.restarts_in_frame = 0

    def start(self):
        target = self.target
    
        def _start_supervised_process():
            process = Process(target=target,
                              args=self.args, kwargs=self.kwargs)
            process.start()
            return process
    
        def _restart(self, process):
            process.join(timeout=self.join_timeout)
            process.terminate()
            self.restarts_in_frame += 1
            process = _start_supervised_process()

        try:
            process = _start_supervised_process()
            restart_frame = 0
            while True:
                if restart_frame > self.max_restart_freq_time:
                    if self.restarts_in_frame >= self.max_restart_freq:
                        raise Exception(
                                "Supervised: Max restart frequency reached")
                restart_frame = 0
                self.restarts_in_frame = 0
                    
                try:
                    proc_is_alive = self.is_alive(process)
                except TimeoutError:
                    proc_is_alive = False

                if not proc_is_alive:
                    self._restart()

                time.sleep(self.check_interval)
                restart_frame += self.check_interval
        finally:
            process.join()
        
    def _is_alive(self, process):
        timeout_timer = threading.Timer(self.ping_timeout, raise_ping_timeout)
        try:
            alive = process.is_alive()
        finally:
            timeout_timer.cancel()
        return alive
