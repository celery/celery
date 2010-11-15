import logging
import os
import sys

config = os.environ.setdefault("CELERY_TEST_CONFIG_MODULE",
                               "celery.tests.config")

os.environ["CELERY_CONFIG_MODULE"] = config
os.environ["CELERY_LOADER"] = "default"
os.environ["TIMER2_TRACE_THREAD"] = "yes"


def teardown():
    # Don't want SUBDEBUG log messages at finalization.
    from multiprocessing.util import get_logger
    get_logger().setLevel(logging.WARNING)
    import threading
    import os
    if os.path.exists("test.db"):
        os.remove("test.db")
    remaining_threads = [thread for thread in threading.enumerate()
                            if thread.name != "MainThread"]
    if remaining_threads:
        sys.stderr.write(
            "\n\n**WARNING**: Remaning threads at teardown: %r...\n" % (
                remaining_threads))
        for thread in remaining_threads:
            try:
                started_by = thread._started_by[thread.ident]
            except (AttributeError, KeyError):
                pass
            else:
                sys.stderr.write("THREAD %r STARTED BY:\n%r\n" % (
                    thread, started_by))
