from functools import wraps

import simplejson
from tornado.web import RequestHandler, Application

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


def api_handler(fun):

    @JSON
    def get(self, *args, **kwargs):
        return fun(self, *args, **kwargs)

    return type(fun.__name__, (APIHandler, ), {"get": get})


@api_handler
def task_state(request, task_id):
    return monitor_state.get_task_info(task_id)


@api_handler
def list_tasks(request):
    return monitor_state.tasks_by_time()


@api_handler
def list_tasks_by_name(request, name):
    return monitor_state.tasks_by_type()[name]


@api_handler
def list_task_types(request):
    return monitor_state.tasks_by_type()


class RevokeTaskHandler(APIHandler):

    SUPPORTED_METHODS = ["POST"]

    @JSON
    def post(self):
        task_id = self.get_argument("task_id")
        revoke(task_id)
        return {"ok": True}


API = [
       (r"/task/name/$", list_task_types),
       (r"/task/name/(.+?)", list_tasks_by_name),
       (r"/task/$", list_tasks),
       (r"/revoke/task/", RevokeTaskHandler),
       (r"/task/(.+)", task_state),
]
