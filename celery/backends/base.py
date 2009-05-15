import time


class TimeOutError(Exception):
    """The operation has timed out."""


class BaseBackend(object):

    TimeOutError = TimeOutError
    
    def __init__(self):
        pass

    def mark_as_done(self, task_id, result):
        raise NotImplementedError(
                "Backends must implement the mark_as_done method")

    def get_status(self, task_id):
        raise NotImplementedError(
                "Backends must implement the get_status method")

    def prepare_result(self, result):
        if result is None:
            return True
        return result
        
    def get_result(self, task_id):
        raise NotImplementedError(
                "Backends must implement the get_result method")

    def is_done(self, task_id):
        return self.get_status(task_id) == "DONE"

    def cleanup(self):
        pass

    def wait_for(self, task_id, timeout=None):
        time_start = time.time()
        while True:
            status = self.get_status(task_id)
            if status == "DONE":
                return self.get_result(task_id)
            if timeout and time.time() > time_start + timeout:
                raise self.TimeOutError(
                        "Timed out while waiting for task %s" % (task_id))
