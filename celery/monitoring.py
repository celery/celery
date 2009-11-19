import time
from collections import defaultdict

from carrot.connection import DjangoBrokerConnection

from celery.events import EventReceiver

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
        self.receiver = EventReceiver(connection, handlers={
            "worker-heartbeat": self.state.receive_heartbeat,
            "worker-online": self.state.receive_worker_event,
            "worker-offline": self.state.receive_worker_event,
            "task-received": self.state.receive_task_event,
            "task-accepted": self.state.receive_task_event,
            "task-succeeded": self.state.receive_task_event,
            "task-failed": self.state.receive_task_event,
            "task-retried": self.state.receive_task_event
        })

    def start(self):
        self.receiver.consume()


class MonitorService(object):

    def start():
        state = MonitorState()
        listener = MonitorListener(state)

        listener.start()
