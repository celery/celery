"""Signal Handlers for the smoke test."""

from celery.signals import worker_init, worker_process_init, worker_process_shutdown, worker_ready, worker_shutdown


@worker_init.connect
def worker_init_handler(sender, **kwargs):
    print("worker_init_handler")


@worker_process_init.connect
def worker_process_init_handler(sender, **kwargs):
    print("worker_process_init_handler")


@worker_process_shutdown.connect
def worker_process_shutdown_handler(sender, pid, exitcode, **kwargs):
    print("worker_process_shutdown_handler")


@worker_ready.connect
def worker_ready_handler(sender, **kwargs):
    print("worker_ready_handler")


@worker_shutdown.connect
def worker_shutdown_handler(sender, **kwargs):
    print("worker_shutdown_handler")
