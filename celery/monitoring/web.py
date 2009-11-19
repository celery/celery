import threading

from tornado import httpserver
from tornado import ioloop
from tornado.web import Application

from celery.monitoring.handlers import api


class WebServerThread(threading.Thread):

    def __init__(self):
        super(WebServerThread, self).__init__()
        self.setDaemon(True)

    def run(self):
        application = Application([
            (r"/api/task/name/$", api.ListAllTasksByNameHandler),
            (r"/api/task/name/(.+?)", api.ListTasksByNameHandler),
            (r"/api/task/$", api.ListTasksHandler),
            (r"/api/task/(.+)", api.TaskStateHandler),
        ])
        http_server = httpserver.HTTPServer(application)
        http_server.listen(8989)
        ioloop.IOLoop.instance().start()
