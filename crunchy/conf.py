from django.conf import settings
import logging

# The number of processes to work simultaneously at processing the queue.
DEFAULT_DAEMON_CONCURRENCY = 10

# If the queue is empty, this is the time *in seconds* the daemon sleeps
# until it wakes up to check if there's any new messages on the queue.
DEFAULT_QUEUE_WAKEUP_AFTER = 0.3 

# As long as the queue is empty, the daemon logs a "Queue is empty" message
# every ``EMPTY_MSG_EMIT_EVERY`` *seconds*.
DEFAULT_EMPTY_MSG_EMIT_EVERY = 5 

DEFAULT_DAEMON_PID_FILE = "crunchd.pid"

# The format we log messages in.
DEFAULT_LOG_FMT = '[%(asctime)s: %(levelname)s/%(processName)s] %(message)s'

# Default log level [DEBUG|INFO|WARNING|ERROR|CRITICAL|FATAL]
DEFAULT_DAEMON_LOG_LEVEL = "INFO"

# Default log file
DEFAULT_DAEMON_LOG_FILE = "refreshd.log"

# Table of loglevels to constants for use in settings.py.
LOG_LEVELS = {
    "DEBUG": logging.DEBUG,
    "INFO": logging.INFO,
    "WARNING": logging.WARNING,
    "WARN": logging.WARNING,
    "ERROR": logging.ERROR,
    "CRITICAL": logging.CRITICAL,
    "FATAL": logging.FATAL,
}

LOG_FORMAT = getattr(settings, "CRUNCHD_DAEMON_LOG_FORMAT",
                            DEFAULT_LOG_FMT)
DAEMON_LOG_FILE = getattr(settings, "CRUNCHD_LOG_FILE",
                            DEFAULT_DAEMON_LOG_FILE)
DAEMON_LOG_LEVEL = LOG_LEVELS[getattr(settings, "CRUNCHD_DAEMON_LOG_LEVEL",
                               DEFAULT_DAEMON_LOG_LEVEL).upper()]

QUEUE_WAKEUP_AFTER = getattr(settings, "CRUNCHD_QUEUE_WAKEUP_AFTER",
                                DEFAULT_QUEUE_WAKEUP_AFTER)
EMPTY_MSG_EMIT_EVERY = getattr(settings, "CRUNCHD_EMPTY_MSG_EMIT_EVERY",
                                DEFAULT_EMPTY_MSG_EMIT_EVERY)
DAEMON_PID_FILE = getattr("settings", "CRUNCHD_PID_FILE",
                            DEFAULT_DAEMON_PID_FILE)
DAEMON_CONCURRENCY = getattr("settings", "CRUNCHD_CONCURRENCY",
                                DEFAULT_DAEMON_CONCURRENCY)
