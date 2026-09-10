from uuid import uuid4

from celery.canvas import Signature, StampingVisitor
from celery.utils.log import get_task_logger

logger = get_task_logger(__name__)


class MyStampingVisitor(StampingVisitor):
    def on_signature(self, sig: Signature, **headers) -> dict:
        logger.critical(f"Visitor: Sig '{sig}' is stamped with: mystamp")
        return {"mystamp": "I am a stamp!"}


class MonitoringIdStampingVisitor(StampingVisitor):
    def on_signature(self, sig: Signature, **headers) -> dict:
        mtask_id = str(uuid4())
        logger.critical(f"Visitor: Sig '{sig}' is stamped with: {mtask_id}")
        return {"mtask_id": mtask_id}


class FullVisitor(StampingVisitor):
    def on_signature(self, sig: Signature, **headers) -> dict:
        logger.critical(f"Visitor: Sig '{sig}' is stamped with: on_signature")
        return {
            "on_signature": "FullVisitor.on_signature()",
        }

    def on_callback(self, sig, **headers) -> dict:
        logger.critical(f"Visitor: Sig '{sig}' is stamped with: on_callback")
        return {
            "on_callback": "FullVisitor.on_callback()",
        }

    def on_errback(self, sig, **headers) -> dict:
        logger.critical(f"Visitor: Sig '{sig}' is stamped with: on_errback")
        return {
            "on_errback": "FullVisitor.on_errback()",
        }

    def on_chain_start(self, sig: Signature, **headers) -> dict:
        logger.critical(f"Visitor: Sig '{sig}' is stamped with: on_chain_start")
        return {
            "on_chain_start": "FullVisitor.on_chain_start()",
        }

    def on_group_start(self, sig: Signature, **headers) -> dict:
        logger.critical(f"Visitor: Sig '{sig}' is stamped with: on_group_start")
        return {
            "on_group_start": "FullVisitor.on_group_start()",
        }

    def on_chord_header_start(self, sig: Signature, **headers) -> dict:
        logger.critical(f"Visitor: Sig '{sig}' is stamped with: on_chord_header_start")
        s = super().on_chord_header_start(sig, **headers)
        s.update(
            {
                "on_chord_header_start": "FullVisitor.on_chord_header_start()",
            }
        )
        return s

    def on_chord_body(self, sig: Signature, **headers) -> dict:
        logger.critical(f"Visitor: Sig '{sig}' is stamped with: on_chord_body")
        return {
            "on_chord_body": "FullVisitor.on_chord_body()",
        }
