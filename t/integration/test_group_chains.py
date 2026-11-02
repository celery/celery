import json
from uuid import uuid4

import pytest

from celery import chain, chord, group, signature
from celery.signals import before_task_publish
from t.integration.tasks import (ExpectedException, fail, get_redis_connection, identity, redis_echo, sum_and_record,
                                 xsum)


class test_group_chains:
    @pytest.fixture(autouse=True)
    def require_redis(self, app):
        if not app.conf.result_backend.startswith('redis'):
            pytest.skip('Requires Redis result backend.')

    @pytest.mark.parametrize('task_protocol', [1, 2])
    @pytest.mark.parametrize('serialized', [False, True])
    @pytest.mark.parametrize('frozen', [False, True])
    @pytest.mark.parametrize('as_chord_body', [False, True])
    def test_group_results_reach_the_next_group(
        self, app, manager, task_protocol, serialized, frozen, as_chord_body,
    ):
        app.conf.task_protocol = task_protocol
        redis_key = f'group-chain-{uuid4()}'
        width, levels = 3, 4
        steps = [group(sum_and_record.si([1] * width, redis_key) for _ in range(width))]
        steps.extend(
            group(sum_and_record.s(redis_key) for _ in range(width))
            for _ in range(levels - 1)
        )
        workflow = chain(*steps, xsum.s(), app=app)
        if as_chord_body:
            workflow = chord([identity.si(0), identity.si(1)], workflow)
        if frozen:
            workflow.freeze()
        if serialized:
            workflow = signature(json.loads(json.dumps(workflow)), app=app)

        with get_redis_connection() as connection:
            try:
                result = workflow.apply_async()
                assert result.get(timeout=30) == width ** (levels + 1)
                executed = connection.lrange(redis_key, 0, -1)
                assert len(executed) == len(set(executed)) == width * levels
            finally:
                connection.delete(redis_key)

    @pytest.mark.parametrize('task_protocol', [1, 2])
    @pytest.mark.parametrize('failed_level', [0, 2])
    def test_pending_chord_keeps_header_errback(self, app, manager, task_protocol, failed_level):
        app.conf.task_protocol = task_protocol
        prefix = f'group-chain-{uuid4()}'
        keys = [f'{prefix}:{level}' for level in range(4)]
        errback_key = f'{prefix}:errback'
        failing = fail.si().on_error(redis_echo.si('failed', redis_key=errback_key))
        failed_id = failing.freeze().id
        steps = []
        for level, key in enumerate(keys):
            tasks = [sum_and_record.si([1, 1, 1], key) for _ in range(3)]
            if level == failed_level:
                tasks[-1] = failing
            steps.append(group(tasks))
        workflow = chain(*steps, xsum.s(), app=app)

        with get_redis_connection() as connection:
            try:
                workflow.apply_async()
                assert connection.blpop(errback_key, timeout=30) == (errback_key.encode(), b'failed')
                failed_result = app.AsyncResult(failed_id)
                assert failed_result.failed()
                assert isinstance(failed_result.result, ExpectedException)
                manager.wait_until_idle()
                assert [connection.llen(key) for key in keys] == [
                    3 if level < failed_level else 2 if level == failed_level else 0
                    for level in range(4)
                ]
            finally:
                connection.delete(*keys, errback_key)

    def test_large_group_pipeline(self, app, manager):
        redis_key = f'group-chain-{uuid4()}'
        width, levels = 25, 10
        steps = [group(sum_and_record.si([1] * width, redis_key) for _ in range(width))]
        steps.extend(
            group(sum_and_record.s(redis_key) for _ in range(width))
            for _ in range(levels - 1)
        )
        workflow = chain(*steps, xsum.s(), app=app)
        sizes = []

        def capture(sender=None, body=None, **kwargs):
            if sender == sum_and_record.name:
                sizes.append(len(json.dumps(body)))

        before_task_publish.connect(capture, weak=False)
        try:
            result = workflow.apply_async()
        finally:
            before_task_publish.disconnect(capture)
        with get_redis_connection() as connection:
            try:
                assert result.get(timeout=60) == width ** (levels + 1)
                executed = connection.lrange(redis_key, 0, -1)
                assert len(executed) == len(set(executed)) == width * levels
                assert len(sizes) == width
                assert max(sizes) < 128 * 1024
            finally:
                connection.delete(redis_key)
