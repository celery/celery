from __future__ import annotations

import json

from pytest_celery import RESULT_TIMEOUT, CeleryTestWorker

from celery.canvas import StampingVisitor
from t.integration.tasks import add, identity


class test_stamping_visitor:
    def test_callback(self, dev_worker: CeleryTestWorker):
        on_signature_stamp = {"on_signature_stamp": 4}
        no_visitor_stamp = {"no_visitor_stamp": "Stamp without visitor"}
        on_callback_stamp = {"on_callback_stamp": 2}
        link_stamp = {
            **on_signature_stamp,
            **no_visitor_stamp,
            **on_callback_stamp,
        }

        class CustomStampingVisitor(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return on_signature_stamp.copy()

            def on_callback(self, callback, **header) -> dict:
                return on_callback_stamp.copy()

        stamped_task = identity.si(123).set(queue=dev_worker.worker_queue)
        stamped_task.link(
            add.s(0)
            .stamp(no_visitor_stamp=no_visitor_stamp["no_visitor_stamp"])
            .set(queue=dev_worker.worker_queue)
        )
        stamped_task.stamp(visitor=CustomStampingVisitor())
        stamped_task.delay().get(timeout=RESULT_TIMEOUT)
        assert dev_worker.logs().count(
            json.dumps(on_signature_stamp, indent=4, sort_keys=True)
        )
        assert dev_worker.logs().count(json.dumps(link_stamp, indent=4, sort_keys=True))
