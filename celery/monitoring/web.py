import threading

from tornado import httpserver
from tornado import ioloop
from tornado.web import Application

from celery.monitoring.handlers import api


class Site(Application):

    def __init__(self, applications, *args, **kwargs):
        handlers = []
        for urlprefix, application in applications:
            for urlmatch, handler in application:
                handlers.append((urlprefix + urlmatch, handler))
        kwargs["handlers"] = handlers
        super(Site, self).__init__(*args, **kwargs)



class WebServerThread(threading.Thread):

    def __init__(self):
        super(WebServerThread, self).__init__()
        self.setDaemon(True)

    def run(self):
        site = Site([
            (r"/api", api.API),
        ])
        http_server = httpserver.HTTPServer(site)
        http_server.listen(8989)
        ioloop.IOLoop.instance().start()
