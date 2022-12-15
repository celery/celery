from uuid import uuid4

from celery.canvas import StampingVisitor


class MyStampingVisitor(StampingVisitor):
    def on_signature(self, sig, **headers) -> dict:
        return {'mystamp': 'I am a stamp!'}


class MonitoringIdStampingVisitor(StampingVisitor):

    def on_signature(self, sig, **headers) -> dict:
        return {'monitoring_id': str(uuid4())}
