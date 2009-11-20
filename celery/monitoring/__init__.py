from carrot.connection import DjangoBrokerConnection

from celery.events import EventReceiver
from celery.monitoring.state import monitor_state
from celery.monitoring.web import WebServerThread


class MonitorListener(object):

    def __init__(self, state):
        self.connection = DjangoBrokerConnection()
        self.receiver = EventReceiver(self.connection, handlers={
            "worker-heartbeat": state.receive_heartbeat,
            "worker-online": state.receive_worker_event,
            "worker-offline": state.receive_worker_event,
            "task-received": state.receive_task_received,
            "task-accepted": state.receive_task_event,
            "task-succeeded": state.receive_task_event,
            "task-failed": state.receive_task_event,
            "task-retried": state.receive_task_event
        })

    def start(self):
        self.receiver.consume()


class MonitorService(object):

    def __init__(self, logger, is_detached=False, http_port=8989):
        self.logger = logger
        self.is_detached = is_detached
        self.http_port = http_port

    def start(self):
        listener = MonitorListener(monitor_state)
        webthread = WebServerThread(port=self.http_port)
        webthread.start()

        listener.start()
