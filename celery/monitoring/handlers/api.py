from tornado.web import RequestHandler
import simplejson

from celery.monitoring.state import monitor_state

class APIHandler(RequestHandler):

    def __init__(self, *args, **kwargs):
        super(APIHandler, self).__init__(*args, **kwargs)
        self.set_header("Content-Type", "application/javascript")


class TaskStateHandler(APIHandler):

    def get(self, task_id):
        events = simplejson.dumps(monitor_state.task_events[task_id])
        self.write(events)


class ListTasksHandler(APIHandler):

    def get(self):
        tasks = simplejson.dumps(monitor_state.tasks_by_time())
        self.write(tasks)


class ListTasksByNameHandler(APIHandler):

    def get(self, name):
        tasks = simplejson.dumps(monitor_state.tasks_by_type()[name])
        self.write(tasks)


class ListAllTasksByNameHandler(APIHandler):
    def get(self):
        tasks = simplejson.dumps(monitor_state.tasks_by_type())
        self.write(tasks)




