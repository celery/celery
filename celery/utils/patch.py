import logging

_process_aware = False


def _patch_logger_class():
    """Make sure process name is recorded when loggers are used."""

    try:
        from multiprocessing.process import current_process
    except ImportError:
        current_process = None

    logging._acquireLock()
    try:
        OldLoggerClass = logging.getLoggerClass()
        if not getattr(OldLoggerClass, '_process_aware', False):

            class ProcessAwareLogger(OldLoggerClass):
                _process_aware = True

                def makeRecord(self, *args, **kwds):
                    record = OldLoggerClass.makeRecord(self, *args, **kwds)
                    if current_process:
                        record.processName = current_process()._name
                    else:
                        record.processName = ""
                    return record
            logging.setLoggerClass(ProcessAwareLogger)
    finally:
        logging._releaseLock()


def ensure_process_aware_logger():
    global _process_aware

    if not _process_aware:
        _patch_logger_class()
        _process_aware = True
