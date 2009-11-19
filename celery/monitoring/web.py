import threading

from tornado import httpserver
from tornado import ioloop



def handle_request(request):
    message = "You requested %s\n" % request.uri
    request.write("HTTP/1.1 200 OK\r\nContent-Length: %d\r\n\r\n%s" % (
                    len(message), message))
    request.finish()


class WebServerThread(threading.Thread):

    def __init__(self):
        super(WebServerThread, self).__init__()
        self.setDaemon(True)

    def run(self):
        http_server = httpserver.HTTPServer(handle_request)
        http_server.listen(8888)
        ioloop.IOLoop.instance().start()
