from __future__ import annotations

import os
import socket
import time
from contextlib import closing, suppress
from time import monotonic
from unittest.mock import Mock, patch

from pytest_celery import CeleryTestSetup

from celery import uuid
from celery.events.state import HEARTBEAT_DRIFT_MAX
from celery.utils.objects import Bunch
from celery.utils.time import utcoffset
from celery.worker.consumer.gossip import Gossip


class test_gossip:
    def test_worker_event_across_timezones_causes_no_drift_warning(
        self,
        celery_setup: CeleryTestSetup,
        monkeypatch,
    ):
        try:
            with monkeypatch.context() as m:
                m.setenv("TZ", "Asia/Ho_Chi_Minh")
                time.tzset()

                with closing(
                    celery_setup.app.connection_for_read()
                ) as receiver_conn:
                    receiver_conn.connect()

                    consumer = Bunch(
                        app=celery_setup.app,
                        hostname=f"receiver-{uuid()}@x",
                        pid=os.getpid(),
                        timer=Mock(name="timer"),
                        hub=None,
                        connection=receiver_conn,
                        event_dispatcher=None,
                    )
                    gossip = Gossip(consumer)
                    sender = celery_setup.worker.hostname()

                    with patch(
                        "celery.events.state._warn_drift"
                    ) as warn_drift:
                        try:
                            gossip.start(consumer)
                            deadline = monotonic() + 10

                            while sender not in gossip.state.workers:
                                assert monotonic() < deadline, "worker event never reached gossip"
                                with suppress(socket.timeout):
                                    receiver_conn.drain_events(timeout=1)

                            worker = gossip.state.workers[sender]

                            # Prove that the sender and receiver really are
                            # running with different timezone offsets.
                            assert worker.utcoffset != utcoffset()

                            # Prove that Gossip retained the absolute epoch
                            # instead of merely suppressing the warning.
                            drift = abs(
                                int(worker.local_received)
                                - int(worker.timestamp)
                            )
                            assert drift <= HEARTBEAT_DRIFT_MAX

                            warn_drift.assert_not_called()
                        finally:
                            gossip.stop(consumer)
        finally:
            # monkeypatch.context() restores TZ before this runs.
            time.tzset()
