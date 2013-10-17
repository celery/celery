"""
    celery.backends.rest_server
    ~~~~~~~~~~~~~~~~~~~~~~~~~~~

    REST reference server.

"""
from __future__ import absolute_import

__author__ = """\
Tal Liron <tal.liron@threecrickets.com>
"""

from BaseHTTPServer import BaseHTTPRequestHandler,HTTPServer
from SocketServer import ThreadingMixIn
import argparse, re, urlparse, json, traceback, Queue


class MemoryBackend(object):
    def __init__(self):
        self.message_queues = {}
        self.routes = {}
        self.task_results = {}

    def pop_message_from_queue(self, queue):
        message_queue = self._get_message_queue(queue)
        try:
            return message_queue.get(block=False)
        except Queue.Empty:
            return None

    def add_message_to_queue(self, queue, message):
        message_queue = self._get_message_queue(queue)
        message_queue.put(message)
    
    def add_message_to_group(self, group, message):
        routes = self._get_routes(group)
        for route in routes:
            if 'queue' in route:
                queue = route['queue']
                message_queue = self._get_message_queue(queue)
                message_queue.put(message)
    
    def get_message_count(self, queue):
        message_queue = self._get_message_queue(queue)
        return {'count': message_queue.qsize()}
    
    def delete_queue(self, queue):
        try:
            del self.message_queues[queue]
        except KeyError:
            return None
        for group, route in self.routes:
            if 'queue' in route:
                if queue == route['queue']:
                    del self.routes[group]
    
    def get_routes(self, group):
        return self._get_routes(group)
    
    def add_route(self, group, route):
        routes = self._get_routes(group)
        routes.append(route)
    
    def delete_routes(self, group):
        try:
            del self.routes[group]
        except KeyError:
            pass
    
    def get_task_result(self, task, format):
        try:
            return self.task_results[task]
        except KeyError:
            return None
    
    def set_task_result(self, task, result, format):
        self.task_results[task] = result
    
    def delete_task_result(self, task):
        try:
            del self.task_results[task]
        except KeyError:
            return None

    def _get_message_queue(self, name):
        try:
            message_queue = self.message_queues[name]
        except KeyError:
            message_queue = self.message_queues[name] = Queue.Queue()
        return message_queue

    def _get_routes(self, group):
        try:
            routes = self.routes[group]
        except KeyError:
            routes = self.routes[group] = []
        return routes

class QueueResource(object):
    url = re.compile(r'/messaging/queue/(?P<queue>[^/]+)/$')
    
    def __init__(self, backend):
        self.backend = backend

    def delete(self, segments, query):
        self.backend.delete_queue(queue)

class QueueMessageResource(object):
    url = re.compile(r'/messaging/queue/(?P<queue>[^/]+)/message/$')

    def __init__(self, backend):
        self.backend = backend

    def get(self, segments, query):
        queue = segments['queue']
        return self.backend.pop_message_from_queue(queue)

    def post(self, segments, query, payload):
        queue = segments['queue']
        self.backend.add_message_to_queue(queue, payload)
        return payload
    
class QueueCountResource(object):
    url = re.compile(r'/messaging/queue/(?P<queue>[^/]+)/count/$')
    
    def __init__(self, backend):
        self.backend = backend

    def get(self, segments, query):
        queue = segments['queue']
        return self.backend.get_message_count(queue)

class QueuesMessageResource(object):
    url = re.compile(r'/messaging/queues/(?P<group>[^/]+)/message/$')
    
    def __init__(self, backend):
        self.backend = backend

    def post(self, segments, query, payload):
        group = segments['group']
        self.backend.add_message_to_group(group, payload)
        return payload

class RoutesResource(object):
    url = re.compile(r'/messaging/routes/(?P<group>[^/]+)/$')
    
    def __init__(self, backend):
        self.backend = backend

    def get(self, segments, query):
        group = segments['group']
        return {'routes': self.backend.get_routes(group)}

    def post(self, segments, query, payload):
        group = segments['group']
        self.backend.add_route(group, payload)
        return payload

class TaskResultResource(object):
    url = re.compile(r'/messaging/task/(?P<task>[^/]+)/result/$')
    
    def __init__(self, backend):
        self.backend = backend

    def get(self, segments, query):
        task = segments['task']
        format = query['format']
        return {'result': self.backend.get_task_result(task, format)}

    def post(self, segments, query, payload):
        if 'result' not in payload:
            return
        result = payload['result']
        task = segments['task']
        format = query['format']
        self.backend.set_task_result(task, result, format)
        return {'result': payload}

BACKEND = MemoryBackend()

RESOURCES = (
    QueueResource(BACKEND),
    QueueMessageResource(BACKEND),
    QueueCountResource(BACKEND),
    QueuesMessageResource(BACKEND),
    RoutesResource(BACKEND),
    TaskResultResource(BACKEND)
)

def get_resource(path):
    url = urlparse.urlparse(path)
    path = url.path
    query = urlparse.parse_qs(url.query)
    for resource in RESOURCES:
        matches = resource.url.match(path)
        if matches:
            segments = matches.groupdict()
            return resource, segments, query
    return None, None, None


class RESTRequestHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        try:
            resource, segments, query = get_resource(self.path)
            if resource:
                if not hasattr(resource, 'get'):
                    self.send_error(405, 'Resource %s does not support GET' % resource.__class__.__name__)
                    return
                result = resource.get(segments, query)
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                if result is not None:
                    self.wfile.write(json.dumps(result))
            else:
                self.send_error(404, 'Resource not found')
        except Exception, x:
            print traceback.format_exc()
            self.send_error(500, x.message)

    def do_POST(self):
        try:
            resource, segments, query = get_resource(self.path)
            if resource:
                if not hasattr(resource, 'post'):
                    self.send_error(405, 'Resource %s does not support POST' % resource.__class__.__name__)
                    return
                length = int(self.headers.getheader('content-length'))
                payload = self.rfile.read(length)
                try:
                    payload = json.loads(payload)
                except:
                    self.send_error(400, 'Must send JSON')
                    return
                result = resource.post(segments, query, payload)
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.end_headers()
                if result is not None:
                    self.wfile.write(json.dumps(result))
            else:
                self.send_error(404, 'Resource not found')
        except Exception, x:
            print traceback.format_exc()
            self.send_error(500, x.message)

    def do_DELETE(self):
        try:
            resource, segments, query = get_resource(self.path)
            if resource:
                if not hasattr(resource, 'delete'):
                    self.send_error(405, 'Resource %s does not support DELETE' % resource.__class__.__name__)
                    return
                result = resource.delete(segments, query)
                self.send_response(200)
            else:
                self.send_error(404, 'Resource not found')
        except Exception, x:
            print traceback.format_exc()
            self.send_error(500, x.message)

class ThreadedHTTPServer(ThreadingMixIn, HTTPServer):
    allow_reuse_address = True
 
    def shutdown(self):
        self.socket.close()
        HTTPServer.shutdown(self)
 
class RESTServer():
    def __init__(self, ip, port):
        self.server = ThreadedHTTPServer((ip, port), RESTRequestHandler)
 
    def start(self):
        try:
            self.server.serve_forever()
        except KeyboardInterrupt, x:
            self.server.shutdown()

if __name__=='__main__':
    parser = argparse.ArgumentParser(description='Celery/Kombu REST Server')
    parser.add_argument('--port', type=int, help='Listening port for HTTP server (defaults to 8080)', default=8080)
    parser.add_argument('--ip', help='IP addess to bind HTTP server (defaults to 127.0.0.1)', default='127.0.0.1')
    args = parser.parse_args()
 
    server = RESTServer(args.ip, args.port)
    print 'Celery/Kombu REST server running on %s:%s...' % (args.ip, args.port)
    server.start()
