from UserList import UserList
from celery.task import mark_as_done


class ProcessQueue(UserList):
    """Queue of running child processes, which starts waiting for the
    processes to finish when the queue limit is reached."""

    def __init__(self, limit, logger=None, done_msg=None):
        self.limit = limit
        self.logger = logger
        self.done_msg = done_msg
        self.data = []
        
    def add(self, result, task_name, task_id):
        self.data.append([result, task_name, task_id])

        if self.data and len(self.data) >= self.limit:
            for result, task_name, task_id in self.data:
                ret_value = result.get()
                if self.done_msg and self.logger:
                    self.logger.info(self.done_msg % {
                        "name": task_name,
                        "id": task_id,
                        "return_value": ret_value})
                    mark_as_done(task_id)
            self.data = []
