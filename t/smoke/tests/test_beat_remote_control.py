from __future__ import annotations

import time

import pytest
from pytest_celery import CeleryTestSetup
from tenacity import retry, stop_after_delay, wait_fixed

from celery import beat


@pytest.fixture
def beat_pidbox(celery_setup: CeleryTestSetup) -> beat.BeatPidbox:
    """A beat control node bound to the suite's real broker.

    Unit tests mock the connection away, so they cannot show that the
    pidbox queue is declared correctly on a real broker, nor that beat
    and a worker coexist on the ``celery.pidbox`` exchange.
    """
    app = celery_setup.app
    app.conf.beat_enable_remote_control = True
    service = beat.Service(app=app, scheduler_cls='celery.beat:Scheduler')
    service._last_tick = time.monotonic()
    pidbox = beat.BeatPidbox(service)
    pidbox.start()

    @retry(stop=stop_after_delay(30), wait=wait_fixed(0.5), reraise=True)
    def wait_until_bound() -> None:
        assert app.control.ping(
            destination=[pidbox.hostname], timeout=3,
        ), 'beat never joined the control exchange'

    try:
        wait_until_bound()
        yield pidbox
    finally:
        pidbox.stop()


class test_beat_remote_control:
    def test_answers_ping_like_a_worker(
        self, celery_setup: CeleryTestSetup, beat_pidbox: beat.BeatPidbox,
    ):
        replies = celery_setup.app.control.ping(
            destination=[beat_pidbox.hostname], timeout=5)
        assert replies == [{beat_pidbox.hostname: {'ok': 'pong'}}]

    def test_coexists_with_a_worker_on_the_exchange(
        self, celery_setup: CeleryTestSetup, beat_pidbox: beat.BeatPidbox,
    ):
        replying = set()
        for reply in celery_setup.app.control.ping(timeout=5):
            replying.update(reply)
        assert beat_pidbox.hostname in replying
        assert celery_setup.worker.hostname() in replying

    def test_ignores_worker_only_commands(
        self, celery_setup: CeleryTestSetup, beat_pidbox: beat.BeatPidbox,
    ):
        # Beat must stay out of worker-only output rather than replying
        # with an error row.
        replies = celery_setup.app.control.inspect(
            destination=[beat_pidbox.hostname], timeout=5).active()
        assert replies is None

    def test_stops_replying_once_the_scheduler_stalls(
        self, celery_setup: CeleryTestSetup, beat_pidbox: beat.BeatPidbox,
    ):
        celery_setup.app.conf.beat_remote_control_max_tick_age = 5.0
        service = beat_pidbox.service

        service._last_tick = time.monotonic() - 60.0
        assert celery_setup.app.control.ping(
            destination=[beat_pidbox.hostname], timeout=5) == []

        service._last_tick = time.monotonic()
        assert celery_setup.app.control.ping(
            destination=[beat_pidbox.hostname], timeout=5) == [
                {beat_pidbox.hostname: {'ok': 'pong'}}]
