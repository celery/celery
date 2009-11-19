from functools import wraps

import simplejson
from tornado.web import RequestHandler

from celery.task import revoke
from celery.monitoring.state import monitor_state


def JSON(fun):

    @wraps(fun)
    def _write_json(self, *args, **kwargs):
        content = fun(self, *args, **kwargs)
        self.write(simplejson.dumps(content))

    return _write_json


class APIHandler(RequestHandler):

    def __init__(self, *args, **kwargs):
        super(APIHandler, self).__init__(*args, **kwargs)
        self.set_header("Content-Type", "application/javascript")


class TaskStateHandler(APIHandler):

    @JSON
    def get(self, task_id):
        return monitor_state.task_events[task_id]


class ListTasksHandler(APIHandler):

    @JSON
    def get(self):
        return monitor_state.tasks_by_time()


class ListTasksByNameHandler(APIHandler):

    @JSON
    def get(self, name):
        return monitor_state.tasks_by_type()[name]


class ListAllTasksByNameHandler(APIHandler):

    @JSON
    def get(self):
        return monitor_state.tasks_by_type()


class RevokeTaskHandler(APIHandler):

    @JSON
    def get(self, task_id):
        revoke(task_id)
        return {"ok": True}
