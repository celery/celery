import threading

from tornado import httpserver
from tornado import ioloop
from tornado.web import Application

from celerymon.handlers import api


class Site(Application):
    """Tornado Website with multiple :class:`Application`'s."""

    def __init__(self, applications, *args, **kwargs):
        handlers = []
        for urlprefix, application in applications:
            for urlmatch, handler in application:
                handlers.append((urlprefix + urlmatch, handler))
        kwargs["handlers"] = handlers
        super(Site, self).__init__(*args, **kwargs)


class WebServerThread(threading.Thread):

    def __init__(self, port=8989):
        super(WebServerThread, self).__init__()
        self.port = port
        self.setDaemon(True)

    def run(self):
        site = Site([
            (r"/api", api.API),
        ])
        http_server = httpserver.HTTPServer(site)
        http_server.listen(self.port)
        ioloop.IOLoop.instance().start()
