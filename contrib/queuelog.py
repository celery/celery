import sys
from multiprocessing.queues import SimpleQueue
from multiprocessing.process import Process
from multiprocessing.pool import Pool


class Logwriter(Process):

    def start(self, log_queue, logfile="process.log"):
        self.log_queue = log_queue
        self.logfile = logfile
        super(Logwriter, self).start()

    def run(self):
        self.process_logs(self.log_queue, self.logfile)

    def process_logs(self, log_queue, logfile):
        need_to_close_fh = False
        logfh = logfile
        if isinstance(logfile, basestring):
            need_to_close_fh = True
            logfh = open(logfile, "a")

        logfh = open(logfile, "a")
        while 1:
            message = log_queue.get()
            if message is None: # received sentinel
                break
            logfh.write(message)

        log_queue.put(None) # cascade sentinel

        if need_to_close_fh:
            logfh.close()


class QueueLogger(object):

    def __init__(self, log_queue, log_process):
        self.log_queue = log_queue
        self.log_process = log_process

    @classmethod
    def start(cls):
        log_queue = SimpleQueue()
        log_process = Logwriter()
        log_process.start(log_queue)
        return cls(log_queue, log_process)

    def write(self, message):
        self.log_queue.put(message)

    def stop(self):
        self.log_queue.put(None) # send sentinel

    def flush(self):
        pass


def some_process_body():
    sys.stderr.write("Vandelay industries!\n")


def setup_redirection():
    queue_logger = QueueLogger.start()
    sys.stderr = queue_logger
    return queue_logger


def main():
    queue_logger = setup_redirection()
    queue_logger.write("ABCDEF\n")
    try:
        p = Pool(10)
        results = [p.apply_async(some_process_body) for i in xrange(20)]
        [result.get() for result in results]
        p.close()
    finally:
        queue_logger.stop()

if __name__ == "__main__":
    main()
