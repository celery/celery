import signal as _signal

from celery.concurrency import processes
from celery.concurrency.processes.processlist import kill_processtree

class TaskPool(processes.TaskPool):
    """Same as celery.concurrency.processes.TaskPool but it forces a kill
       of the entire process tree when finishing a job. Useful because in
       Windows you cannot capture TerminateProcess to handle graceful
       termination of a process tree.
    """
    def terminate_job(self, pid, signal=None):
        kill_processtree(pid, signal or _signal.SIGTERM)

