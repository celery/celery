from celery.timer import TimeoutTimer


class BaseBackend(object):

    def __init__(self):
        pass

    def store_result(self, task_id, result, status):
        raise NotImplementedError(
                "Backends must implement the store_result method")

    def mark_as_done(self, task_id, result):
        return self.store_result(task_id, result, status="DONE")
    
    def mark_as_failure(self, task_id, exc):
        return self.store_result(task_id, exc, status="FAILURE")

    def mark_as_retry(self, task_id, exc):
        return self.store_result(task_id, exc, status="RETRY")

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
        timeout_timer = TimeoutTimer(timeout)
        while True:
            status = self.get_status(task_id)
            if status == "DONE":
                return self.get_result(task_id)
            elif status == "FAILURE":
                raise self.get_result(task_id)
            timeout_timer.tick()
