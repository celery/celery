import time
from collections import defaultdict

from carrot.connection import DjangoBrokerConnection

from celery.events import EventReceiver
from celery.monitoring.web import WebServerThread

HEARTBEAT_EXPIRE = 120 # Heartbeats must be at most 2 minutes apart.


class MonitorState(object):

    def __init__(self):
        self.hearts = {}
        self.tasks = defaultdict(lambda: [])
        self.workers = defaultdict(lambda: [])


    def tasks_by_type(self):
        types = {}
        for id, task in self.tasks:
            types[task["name"]] = task
        return types

    def receive_task_event(self, event):
        event["state"] = event.pop("type")
        self.tasks[event["uuid"]].append(event)

    def receive_heartbeat(self, event):
        self.hearts[event["hostname"]] = event["timestamp"]

    def receive_worker_event(self, event):
        self.workers[event["hostname"]].append(event["type"])

    def worker_is_alive(self, hostname):
        last_worker_event = self.workers[hostname][-1]
        if last_worker_event and last_worker_event == "worker-online":
            time_of_last_heartbeat = self.hearts[hostname]
            if time.time() < time_of_last_heartbeat + HEARTBEAT_EXPIRE:
                return True
        return False

    def tasks_by_time(self):
        return sorted(self.tasks.values(), key=lambda events: events[-1].timestamp)

    def tasks_by_last_state(self):
        return [events[-1] for event in self.tasks_by_time()]


class MonitorListener(object):

    def __init__(self, state):
        self.connection = DjangoBrokerConnection()
        self.receiver = EventReceiver(self.connection, handlers={
            "worker-heartbeat": state.receive_heartbeat,
            "worker-online": state.receive_worker_event,
            "worker-offline": state.receive_worker_event,
            "task-received": state.receive_task_event,
            "task-accepted": state.receive_task_event,
            "task-succeeded": state.receive_task_event,
            "task-failed": state.receive_task_event,
            "task-retried": state.receive_task_event
        })

    def start(self):
        self.receiver.consume()


class MonitorService(object):

    def __init__(self, logger, is_detached=False):
        self.logger = logger
        self.is_detached = is_detached

    def start(self):
        state = MonitorState()
        listener = MonitorListener(state)
        webthread = WebServerThread()
        webthread.start()

        listener.start()
