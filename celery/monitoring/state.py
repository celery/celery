import time
from collections import defaultdict
from datetime import datetime

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

    def get_task_info(self, task_id):
        task_info = dict(self.tasks[task_id])

        task_events = []
        for event in self.task_events[task_id]:
            if event["state"] in ("task-failed", "task-retried"):
                task_info["exception"] = event["exception"]
                task_info["traceback"] = event["traceback"]
            elif event["state"] == "task-succeeded":
                task_info["result"] = event["result"]
            task_events.append({event["state"]: event["when"]})
        task_info["events"] = task_events

        return task_info

    def receive_task_event(self, event):
        event["state"] = event.pop("type")
        event["when"] = self.timestamp_to_isoformat(event["timestamp"])
        self.task_events[event["uuid"]].append(event)

    def timestamp_to_isoformat(self, timestamp):
        return datetime.fromtimestamp(timestamp).isoformat()

    def receive_heartbeat(self, event):
        self.hearts[event["hostname"]] = event["timestamp"]

    def receive_task_received(self, event):
        task_info = dict(event)
        event = dict(event)
        task_info.pop("type")
        event["state"] = event.pop("type")
        event["when"] = self.timestamp_to_isoformat(event["timestamp"])
        self.tasks[task_info["uuid"]] = task_info
        self.task_events[event["uuid"]].append(event)

    def list_workers(self):
        alive_workers = []
        for hostname, events in self.workers.items():
            if events[-1]["state"] == "worker-online":
                alive_workers.append({hostname: events[-1]["when"]})
        return alive_workers

    def receive_worker_event(self, event):
        event["state"] = event.pop("type")
        event["when"] = self.timestamp_to_isoformat(event["timestamp"])
        self.workers[event["hostname"]].append(event)

    def worker_is_alive(self, hostname):
        last_worker_event = self.workers[hostname][-1]
        if last_worker_event and last_worker_event == "worker-online":
            time_of_last_heartbeat = self.hearts[hostname]
            if time.time() < time_of_last_heartbeat + HEARTBEAT_EXPIRE:
                return True
        return False

    def tasks_by_time(self):
        return dict(sorted(self.task_events.items(),
                        key=lambda uuid__events: uuid__events[1][-1]["timestamp"]))

    def tasks_by_last_state(self):
        return [events[-1] for event in self.task_by_time()]

monitor_state = MonitorState()
