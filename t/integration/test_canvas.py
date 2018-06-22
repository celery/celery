from __future__ import absolute_import, unicode_literals

from datetime import datetime, timedelta

import pytest

from celery import chain, chord, group
from celery.exceptions import TimeoutError
from celery.result import AsyncResult, GroupResult, ResultSet

from .conftest import flaky, get_active_redis_channels, get_redis_connection
from .tasks import (add, add_chord_to_chord, add_replaced, add_to_all,
                    add_to_all_to_chord, build_chain_inside_task, collect_ids,
                    delayed_sum, delayed_sum_with_soft_guard, identity, ids,
                    print_unicode, redis_echo, second_order_replace1, tsum)

TIMEOUT = 120


class test_chain:

    @flaky
    def test_simple_chain(self, manager):
        c = add.s(4, 4) | add.s(8) | add.s(16)
        assert c().get(timeout=TIMEOUT) == 32

    @flaky
    def test_single_chain(self, manager):
        c = chain(add.s(3, 4))()
        assert c.get(timeout=TIMEOUT) == 7

    @flaky
    def test_complex_chain(self, manager):
        c = (
            add.s(2, 2) | (
                add.s(4) | add_replaced.s(8) | add.s(16) | add.s(32)
            ) |
            group(add.s(i) for i in range(4))
        )
        res = c()
        assert res.get(timeout=TIMEOUT) == [64, 65, 66, 67]

    @flaky
    def test_group_results_in_chain(self, manager):
        # This adds in an explicit test for the special case added in commit
        # 1e3fcaa969de6ad32b52a3ed8e74281e5e5360e6
        c = (
            group(
                add.s(1, 2) | group(
                    add.s(1), add.s(2)
                )
            )
        )
        res = c()
        assert res.get(timeout=TIMEOUT) == [4, 5]

    @flaky
    def test_chain_inside_group_receives_arguments(self, manager):
        c = (
            add.s(5, 6) |
            group((add.s(1) | add.s(2), add.s(3)))
        )
        res = c()
        assert res.get(timeout=TIMEOUT) == [14, 14]

    @flaky
    def test_eager_chain_inside_task(self, manager):
        from .tasks import chain_add

        prev = chain_add.app.conf.task_always_eager
        chain_add.app.conf.task_always_eager = True

        chain_add.apply_async(args=(4, 8), throw=True).get()

        chain_add.app.conf.task_always_eager = prev

    @flaky
    def test_group_chord_group_chain(self, manager):
        from celery.five import bytes_if_py2

        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')
        redis_connection = get_redis_connection()
        redis_connection.delete('redis-echo')
        before = group(redis_echo.si('before {}'.format(i)) for i in range(3))
        connect = redis_echo.si('connect')
        after = group(redis_echo.si('after {}'.format(i)) for i in range(2))

        result = (before | connect | after).delay()
        result.get(timeout=TIMEOUT)
        redis_messages = list(map(
            bytes_if_py2,
            redis_connection.lrange('redis-echo', 0, -1)
        ))
        before_items = \
            set(map(bytes_if_py2, (b'before 0', b'before 1', b'before 2')))
        after_items = set(map(bytes_if_py2, (b'after 0', b'after 1')))

        assert set(redis_messages[:3]) == before_items
        assert redis_messages[3] == b'connect'
        assert set(redis_messages[4:]) == after_items
        redis_connection.delete('redis-echo')

    @flaky
    def test_second_order_replace(self, manager):
        from celery.five import bytes_if_py2

        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        redis_connection = get_redis_connection()
        redis_connection.delete('redis-echo')

        result = second_order_replace1.delay()
        result.get(timeout=TIMEOUT)
        redis_messages = list(map(
            bytes_if_py2,
            redis_connection.lrange('redis-echo', 0, -1)
        ))

        expected_messages = [b'In A', b'In B', b'In/Out C', b'Out B', b'Out A']
        assert redis_messages == expected_messages

    @flaky
    def test_parent_ids(self, manager, num=10):
        assert manager.inspect().ping()
        c = chain(ids.si(i=i) for i in range(num))
        c.freeze()
        res = c()
        try:
            res.get(timeout=TIMEOUT)
        except TimeoutError:
            print(manager.inspect.active())
            print(manager.inspect.reserved())
            print(manager.inspect.stats())
            raise
        self.assert_ids(res, num - 1)

    def assert_ids(self, res, size):
        i, root = size, res
        while root.parent:
            root = root.parent
        node = res
        while node:
            root_id, parent_id, value = node.get(timeout=30)
            assert value == i
            if node.parent:
                assert parent_id == node.parent.id
            assert root_id == root.id
            node = node.parent
            i -= 1

    def test_chord_soft_timeout_recuperation(self, manager):
        """Test that if soft timeout happens in task but is managed by task,
        chord still get results normally
        """
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        c = chord([
            # return 3
            add.s(1, 2),
            # return 0 after managing soft timeout
            delayed_sum_with_soft_guard.s(
                [100], pause_time=2
            ).set(
                soft_time_limit=1
            ),
        ])
        result = c(delayed_sum.s(pause_time=0)).get()
        assert result == 3

    def test_chain_error_handler_with_eta(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        eta = datetime.utcnow() + timedelta(seconds=10)
        c = chain(
            group(
                add.s(1, 2),
                add.s(3, 4),
            ),
            tsum.s()
        ).on_error(print_unicode.s()).apply_async(eta=eta)

        result = c.get()
        assert result == 10

    @flaky
    def test_groupresult_serialization(self, manager):
        """Test GroupResult is correctly serialized
        to save in the result backend"""
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        async_result = build_chain_inside_task.delay()
        result = async_result.get()
        assert len(result) == 2
        assert isinstance(result[0][1], list)


class test_result_set:

    @flaky
    def test_result_set(self, manager):
        assert manager.inspect().ping()

        rs = ResultSet([add.delay(1, 1), add.delay(2, 2)])
        assert rs.get(timeout=TIMEOUT) == [2, 4]


class test_group:

    @flaky
    def test_empty_group_result(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        task = group([])
        result = task.apply_async()

        GroupResult.save(result)
        task = GroupResult.restore(result.id)
        assert task.results == []

    @flaky
    def test_parent_ids(self, manager):
        assert manager.inspect().ping()
        g = (
            ids.si(i=1) |
            ids.si(i=2) |
            group(ids.si(i=i) for i in range(2, 50))
        )
        res = g()
        expected_root_id = res.parent.parent.id
        expected_parent_id = res.parent.id
        values = res.get(timeout=TIMEOUT)

        for i, r in enumerate(values):
            root_id, parent_id, value = r
            assert root_id == expected_root_id
            assert parent_id == expected_parent_id
            assert value == i + 2

    @flaky
    def test_nested_group(self, manager):
        assert manager.inspect().ping()

        c = group(
            add.si(1, 10),
            group(
                add.si(1, 100),
                group(
                    add.si(1, 1000),
                    add.si(1, 2000),
                ),
            ),
        )
        res = c()

        assert res.get(timeout=TIMEOUT) == [11, 101, 1001, 2001]


def assert_ids(r, expected_value, expected_root_id, expected_parent_id):
    root_id, parent_id, value = r.get(timeout=TIMEOUT)
    assert expected_value == value
    assert root_id == expected_root_id
    assert parent_id == expected_parent_id


class test_chord:

    @flaky
    def test_redis_subscribed_channels_leak(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        manager.app.backend.result_consumer.on_after_fork()
        initial_channels = get_active_redis_channels()
        initial_channels_count = len(initial_channels)

        total_chords = 10
        async_results = [
            chord([add.s(5, 6), add.s(6, 7)])(delayed_sum.s())
            for _ in range(total_chords)
        ]

        manager.assert_result_tasks_in_progress_or_completed(async_results)

        channels_before = get_active_redis_channels()
        channels_before_count = len(channels_before)

        assert set(channels_before) != set(initial_channels)
        assert channels_before_count > initial_channels_count

        # The total number of active Redis channels at this point
        # is the number of chord header tasks multiplied by the
        # total chord tasks, plus the initial channels
        # (existing from previous tests).
        chord_header_task_count = 2
        assert channels_before_count <= \
            chord_header_task_count * total_chords + initial_channels_count

        result_values = [
            result.get(timeout=TIMEOUT)
            for result in async_results
        ]
        assert result_values == [24] * total_chords

        channels_after = get_active_redis_channels()
        channels_after_count = len(channels_after)

        assert channels_after_count == initial_channels_count
        assert set(channels_after) == set(initial_channels)

    @flaky
    def test_replaced_nested_chord(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c1 = chord([
            chord(
                [add.s(1, 2), add_replaced.s(3, 4)],
                add_to_all.s(5),
            ) | tsum.s(),
            chord(
                [add_replaced.s(6, 7), add.s(0, 0)],
                add_to_all.s(8),
            ) | tsum.s(),
        ], add_to_all.s(9))
        res1 = c1()
        assert res1.get(timeout=TIMEOUT) == [29, 38]

    @flaky
    def test_add_to_chord(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        c = group([add_to_all_to_chord.s([1, 2, 3], 4)]) | identity.s()
        res = c()
        assert res.get() == [0, 5, 6, 7]

    @flaky
    def test_add_chord_to_chord(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        c = group([add_chord_to_chord.s([1, 2, 3], 4)]) | identity.s()
        res = c()
        assert res.get() == [0, 5 + 6 + 7]

    @flaky
    def test_group_chain(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')
        c = (
            add.s(2, 2) |
            group(add.s(i) for i in range(4)) |
            add_to_all.s(8)
        )
        res = c()
        assert res.get(timeout=TIMEOUT) == [12, 13, 14, 15]

    @flaky
    def test_nested_group_chain(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        if not manager.app.backend.supports_native_join:
            raise pytest.skip('Requires native join support.')
        c = chain(
            add.si(1, 0),
            group(
                add.si(1, 100),
                chain(
                    add.si(1, 200),
                    group(
                        add.si(1, 1000),
                        add.si(1, 2000),
                    ),
                ),
            ),
            add.si(1, 10),
        )
        res = c()
        assert res.get(timeout=TIMEOUT) == 11

    @flaky
    def test_single_task_header(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c1 = chord([add.s(2, 5)], body=add_to_all.s(9))
        res1 = c1()
        assert res1.get(timeout=TIMEOUT) == [16]

        c2 = group([add.s(2, 5)]) | add_to_all.s(9)
        res2 = c2()
        assert res2.get(timeout=TIMEOUT) == [16]

    def test_empty_header_chord(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c1 = chord([], body=add_to_all.s(9))
        res1 = c1()
        assert res1.get(timeout=TIMEOUT) == []

        c2 = group([]) | add_to_all.s(9)
        res2 = c2()
        assert res2.get(timeout=TIMEOUT) == []

    @flaky
    def test_nested_chord(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c1 = chord([
            chord([add.s(1, 2), add.s(3, 4)], add.s([5])),
            chord([add.s(6, 7)], add.s([10]))
        ], add_to_all.s(['A']))
        res1 = c1()
        assert res1.get(timeout=TIMEOUT) == [[3, 7, 5, 'A'], [13, 10, 'A']]

        c2 = group([
            group([add.s(1, 2), add.s(3, 4)]) | add.s([5]),
            group([add.s(6, 7)]) | add.s([10]),
        ]) | add_to_all.s(['A'])
        res2 = c2()
        assert res2.get(timeout=TIMEOUT) == [[3, 7, 5, 'A'], [13, 10, 'A']]

        c = group([
            group([
                group([
                    group([
                        add.s(1, 2)
                    ]) | add.s([3])
                ]) | add.s([4])
            ]) | add.s([5])
        ]) | add.s([6])

        res = c()
        assert [[[[3, 3], 4], 5], 6] == res.get(timeout=TIMEOUT)

    @flaky
    def test_parent_ids(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')
        root = ids.si(i=1)
        expected_root_id = root.freeze().id
        g = chain(
            root, ids.si(i=2),
            chord(
                group(ids.si(i=i) for i in range(3, 50)),
                chain(collect_ids.s(i=50) | ids.si(i=51)),
            ),
        )
        self.assert_parentids_chord(g(), expected_root_id)

    @flaky
    def test_parent_ids__OR(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')
        root = ids.si(i=1)
        expected_root_id = root.freeze().id
        g = (
            root |
            ids.si(i=2) |
            group(ids.si(i=i) for i in range(3, 50)) |
            collect_ids.s(i=50) |
            ids.si(i=51)
        )
        self.assert_parentids_chord(g(), expected_root_id)

    def assert_parentids_chord(self, res, expected_root_id):
        assert isinstance(res, AsyncResult)
        assert isinstance(res.parent, AsyncResult)
        assert isinstance(res.parent.parent, GroupResult)
        assert isinstance(res.parent.parent.parent, AsyncResult)
        assert isinstance(res.parent.parent.parent.parent, AsyncResult)

        # first we check the last task
        assert_ids(res, 51, expected_root_id, res.parent.id)

        # then the chord callback
        prev, (root_id, parent_id, value) = res.parent.get(timeout=30)
        assert value == 50
        assert root_id == expected_root_id
        # started by one of the chord header tasks.
        assert parent_id in res.parent.parent.results

        # check what the chord callback recorded
        for i, p in enumerate(prev):
            root_id, parent_id, value = p
            assert root_id == expected_root_id
            assert parent_id == res.parent.parent.parent.id

        # ids(i=2)
        root_id, parent_id, value = res.parent.parent.parent.get(timeout=30)
        assert value == 2
        assert parent_id == res.parent.parent.parent.parent.id
        assert root_id == expected_root_id

        # ids(i=1)
        root_id, parent_id, value = res.parent.parent.parent.parent.get(
            timeout=30)
        assert value == 1
        assert root_id == expected_root_id
        assert parent_id is None
