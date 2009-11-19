import time
from collections import defaultdict

HEARTBEAT_EXPIRE = 120 # Heartbeats must be at most 2 minutes apart.


class MonitorState(object):

    def __init__(self):
        self.hearts = {}
        self.tasks = {}
        self.task_events = defaultdict(lambda: [])
        self.workers = defaultdict(lambda: [])

    def tasks_by_type(self):
        t = defaultdict(lambda: [])
        for id, events in self.task_events.items():
            try:
                task_type = self.tasks[id]["name"]
            except KeyError:
                pass
            else:
                t[task_type].append(id)
        return t

    def receive_task_event(self, event):
        event["state"] = event.pop("type")
        self.task_events[event["uuid"]].append(event)

    def receive_heartbeat(self, event):
        self.hearts[event["hostname"]] = event["timestamp"]

    def receive_task_received(self, event):
        self.tasks[event["uuid"]] = event
        self.task_events[event["uuid"]].append(event)

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
        return dict(sorted(self.task_events.items(),
                        key=lambda (uuid, events): events[-1]["timestamp"]))

    def tasks_by_last_state(self):
        return [events[-1] for event in self.task_by_time()]

monitor_state = MonitorState()
