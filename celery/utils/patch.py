import logging

_process_aware = False


def _patch_logger_class():
    """Make sure process name is recorded when loggers are used."""

    from multiprocessing.process import current_process
    logging._acquireLock()
    did_exc = None
    try:
        OldLoggerClass = logging.getLoggerClass()
        if not getattr(OldLoggerClass, '_process_aware', False):

            class ProcessAwareLogger(OldLoggerClass):
                _process_aware = True

                def makeRecord(self, *args, **kwds):
                    record = OldLoggerClass.makeRecord(self, *args, **kwds)
                    record.processName = current_process()._name
                    return record
            logging.setLoggerClass(ProcessAwareLogger)
    except Exception, e:
        did_exc = e

    logging._releaseLock()

    if did_exc:
        raise did_exc
        

def ensure_process_aware_logger():
    global _process_aware

    if not _process_aware:
        _patch_logger_class()
        _process_aware = True
