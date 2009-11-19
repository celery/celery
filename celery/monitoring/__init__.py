from carrot.connection import DjangoBrokerConnection

from celery.events import EventReceiver
from celery.monitoring.state import MonitorState
from celery.monitoring.web import WebServerThread


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
