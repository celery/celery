from uuid import uuid4

from celery.canvas import Signature, StampingVisitor


class MyStampingVisitor(StampingVisitor):
    def on_signature(self, sig: Signature, **headers) -> dict:
        return {"mystamp": "I am a stamp!"}


class MonitoringIdStampingVisitor(StampingVisitor):
    def on_signature(self, sig: Signature, **headers) -> dict:
        return {"monitoring_id": str(uuid4())}


class FullVisitor(StampingVisitor):
    def on_signature(self, sig: Signature, **headers) -> dict:
        return {
            "on_signature": "FullVisitor.on_signature()",
        }

    def on_callback(self, sig, **headers) -> dict:
        return {
            "on_callback": "FullVisitor.on_callback()",
        }

    def on_errback(self, sig, **headers) -> dict:
        return {
            "on_errback": "FullVisitor.on_errback()",
        }

    def on_chain_start(self, sig: Signature, **headers) -> dict:
        return {
            "on_chain_start": "FullVisitor.on_chain_start()",
        }

    def on_group_start(self, sig: Signature, **headers) -> dict:
        return {
            "on_group_start": "FullVisitor.on_group_start()",
        }

    def on_chord_header_start(self, sig: Signature, **headers) -> dict:
        s = super().on_chord_header_start(sig, **headers)
        s.update(
            {
                "on_chord_header_start": "FullVisitor.on_chord_header_start()",
            }
        )
        return s

    def on_chord_body(self, sig: Signature, **headers) -> dict:
        return {
            "on_chord_body": "FullVisitor.on_chord_body()",
        }


class LoggingVisitor(StampingVisitor):
    def on_signature(self, sig: Signature, **headers) -> dict:
        msg = f"Examining signature '{sig.name}' with stamps: {sig.options.get('stamped_headers', 'None')}"
        print(msg)
        print(f"Options: {sig.options}")
        return super().on_signature(sig, **headers)
