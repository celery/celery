import multiprocessing
import os
import time
import logging
from celery.conf import LOG_FORMAT, DAEMON_LOG_LEVEL

__all__ = ["setup_logger", "emergency_error"]


def setup_logger(loglevel=DAEMON_LOG_LEVEL, logfile=None, format=LOG_FORMAT):
    """Setup the ``multiprocessing`` logger. If ``logfile`` is not specified,
    ``stderr`` is used.
    
    Returns logger object.
    """
    logger = multiprocessing.get_logger()
    if logfile:
        log_file_handler = logging.FileHandler(logfile)
        formatter = logging.Formatter(format)
        log_file_handler.setFormatter(formatter)
        logger.addHandler(log_file_handler)
    else:
        multiprocessing.log_to_stderr()
    logger.setLevel(loglevel)
    return logger


def emergency_error(logfile, message):
    logfh = open(logfile, "a")
    logfh.write("[%(asctime)s: FATAL/%(pid)d]: %(message)s\n" % {
                    "asctime": time.asctime(),
                    "pid": os.getpid(),
                    "message": message})
    logfh.close()
