import collections
import re
import tempfile
import uuid
from datetime import datetime, timedelta
from time import monotonic, sleep

import pytest
import pytest_subtests  # noqa

from celery import chain, chord, group, signature
from celery.backends.base import BaseKeyValueStoreBackend
from celery.canvas import StampingVisitor
from celery.exceptions import ImproperlyConfigured, TimeoutError
from celery.result import AsyncResult, GroupResult, ResultSet
from celery.signals import before_task_publish, task_received

from . import tasks
from .conftest import TEST_BACKEND, get_active_redis_channels, get_redis_connection
from .tasks import (ExpectedException, StampOnReplace, add, add_chord_to_chord, add_replaced, add_to_all,
                    add_to_all_to_chord, build_chain_inside_task, collect_ids, delayed_sum,
                    delayed_sum_with_soft_guard, errback_new_style, errback_old_style, fail, fail_replaced, identity,
                    ids, mul, print_unicode, raise_error, redis_count, redis_echo, redis_echo_group_id,
                    replace_with_chain, replace_with_chain_which_raises, replace_with_empty_chain,
                    replace_with_stamped_task, retry_once, return_exception, return_priority, second_order_replace1,
                    tsum, write_to_file_and_return_int, xsum)

RETRYABLE_EXCEPTIONS = (OSError, ConnectionError, TimeoutError)


def is_retryable_exception(exc):
    return isinstance(exc, RETRYABLE_EXCEPTIONS)


TIMEOUT = 60

_flaky = pytest.mark.flaky(reruns=5, reruns_delay=1, cause=is_retryable_exception)
_timeout = pytest.mark.timeout(timeout=300)


def flaky(fn):
    return _timeout(_flaky(fn))


def await_redis_echo(expected_msgs, redis_key="redis-echo", timeout=TIMEOUT):
    """
    Helper to wait for a specified or well-known redis key to contain a string.
    """
    redis_connection = get_redis_connection()

    if isinstance(expected_msgs, (str, bytes, bytearray)):
        expected_msgs = (expected_msgs,)
    expected_msgs = collections.Counter(
        e if not isinstance(e, str) else e.encode("utf-8")
        for e in expected_msgs
    )

    # This can technically wait for `len(expected_msg_or_msgs) * timeout` :/
    while +expected_msgs:
        maybe_key_msg = redis_connection.blpop(redis_key, timeout)
        if maybe_key_msg is None:
            raise TimeoutError(
                "Fetching from {!r} timed out - still awaiting {!r}"
                .format(redis_key, dict(+expected_msgs))
            )
        retrieved_key, msg = maybe_key_msg
        assert retrieved_key.decode("utf-8") == redis_key
        expected_msgs[msg] -= 1  # silently accepts unexpected messages

    # There should be no more elements - block momentarily
    assert redis_connection.blpop(redis_key, min(1, timeout)) is None


def await_redis_list_message_length(expected_length, redis_key="redis-group-ids", timeout=TIMEOUT):
    """
    Helper to wait for a specified or well-known redis key to contain a string.
    """
    sleep(1)
    redis_connection = get_redis_connection()

    check_interval = 0.1
    check_max = int(timeout / check_interval)

    for i in range(check_max + 1):
        length = redis_connection.llen(redis_key)

        if length == expected_length:
            break

        sleep(check_interval)
    else:
        raise TimeoutError(f'{redis_key!r} has length of {length}, but expected to be of length {expected_length}')

    sleep(min(1, timeout))
    assert redis_connection.llen(redis_key) == expected_length


def await_redis_count(expected_count, redis_key="redis-count", timeout=TIMEOUT):
    """
    Helper to wait for a specified or well-known redis key to count to a value.
    """
    redis_connection = get_redis_connection()

    check_interval = 0.1
    check_max = int(timeout / check_interval)
    for i in range(check_max + 1):
        maybe_count = redis_connection.get(redis_key)
        # It's either `None` or a base-10 integer
        if maybe_count is not None:
            count = int(maybe_count)
            if count == expected_count:
                break
            elif i >= check_max:
                assert count == expected_count
        # try again later
        sleep(check_interval)
    else:
        raise TimeoutError(f"{redis_key!r} was never incremented")

    # There should be no more increments - block momentarily
    sleep(min(1, timeout))
    assert int(redis_connection.get(redis_key)) == expected_count


def compare_group_ids_in_redis(redis_key='redis-group-ids'):
    redis_connection = get_redis_connection()
    actual = redis_connection.lrange(redis_key, 0, -1)
    assert len(actual) >= 2, 'Expected at least 2 group ids in redis'
    assert actual[0] == actual[1], 'Expected group ids to be equal'


class test_link_error:
    @flaky
    def test_link_error_eager(self):
        exception = ExpectedException("Task expected to fail", "test")
        result = fail.apply(args=("test",), link_error=return_exception.s())
        actual = result.get(timeout=TIMEOUT, propagate=False)
        assert actual == exception

    @flaky
    def test_link_error(self):
        exception = ExpectedException("Task expected to fail", "test")
        result = fail.apply(args=("test",), link_error=return_exception.s())
        actual = result.get(timeout=TIMEOUT, propagate=False)
        assert actual == exception

    @flaky
    def test_link_error_callback_error_callback_retries_eager(self):
        exception = ExpectedException("Task expected to fail", "test")
        result = fail.apply(
            args=("test",),
            link_error=retry_once.s(countdown=None)
        )
        assert result.get(timeout=TIMEOUT, propagate=False) == exception

    @pytest.mark.xfail(raises=TimeoutError, reason="Task is timeout instead of returning exception")
    def test_link_error_callback_retries(self):
        exception = ExpectedException("Task expected to fail", "test")
        result = fail.apply_async(
            args=("test",),
            link_error=retry_once.s(countdown=None)
        )
        assert result.get(timeout=TIMEOUT / 10, propagate=False) == exception

    @flaky
    def test_link_error_using_signature_eager(self):
        fail = signature('t.integration.tasks.fail', args=("test",))
        retrun_exception = signature('t.integration.tasks.return_exception')

        fail.link_error(retrun_exception)

        exception = ExpectedException("Task expected to fail", "test")
        assert (fail.apply().get(timeout=TIMEOUT, propagate=False), True) == (
            exception, True)

    @pytest.mark.xfail(raises=TimeoutError, reason="Task is timeout instead of returning exception")
    def test_link_error_using_signature(self):
        fail = signature('t.integration.tasks.fail', args=("test",))
        retrun_exception = signature('t.integration.tasks.return_exception')

        fail.link_error(retrun_exception)

        exception = ExpectedException("Task expected to fail", "test")
        assert (fail.delay().get(timeout=TIMEOUT / 10, propagate=False), True) == (
            exception, True)


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
        g = group(add.s(i) for i in range(4))
        c = (
            add.s(2, 2) | (
                add.s(4) | add_replaced.s(8) | add.s(16) | add.s(32)
            ) | g
        )
        res = c()
        assert res.get(timeout=TIMEOUT) == [64, 65, 66, 67]

    @pytest.mark.xfail(raises=TimeoutError, reason="Task is timeout")
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
        assert res.get(timeout=TIMEOUT / 10) == [4, 5]

    def test_chain_of_chain_with_a_single_task(self, manager):
        sig = signature('any_taskname', queue='any_q')
        chain([chain(sig)]).apply_async()

    def test_chain_on_error(self, manager):
        from .tasks import ExpectedException

        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        # Run the chord and wait for the error callback to finish.
        c1 = chain(
            add.s(1, 2), fail.s(), add.s(3, 4),
        )
        res = c1()

        with pytest.raises(ExpectedException):
            res.get(propagate=True)

        with pytest.raises(ExpectedException):
            res.parent.get(propagate=True)

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
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')
        redis_connection = get_redis_connection()
        redis_connection.delete('redis-echo')
        before = group(redis_echo.si(f'before {i}') for i in range(3))
        connect = redis_echo.si('connect')
        after = group(redis_echo.si(f'after {i}') for i in range(2))

        result = (before | connect | after).delay()
        result.get(timeout=TIMEOUT)
        redis_messages = list(redis_connection.lrange('redis-echo', 0, -1))
        before_items = {b'before 0', b'before 1', b'before 2'}
        after_items = {b'after 0', b'after 1'}

        assert set(redis_messages[:3]) == before_items
        assert redis_messages[3] == b'connect'
        assert set(redis_messages[4:]) == after_items
        redis_connection.delete('redis-echo')

    @flaky
    def test_group_result_not_has_cache(self, manager):
        t1 = identity.si(1)
        t2 = identity.si(2)
        gt = group([identity.si(3), identity.si(4)])
        ct = chain(identity.si(5), gt)
        task = group(t1, t2, ct)
        result = task.delay()
        assert result.get(timeout=TIMEOUT) == [1, 2, [3, 4]]

    @flaky
    def test_second_order_replace(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        redis_connection = get_redis_connection()
        redis_connection.delete('redis-echo')

        result = second_order_replace1.delay()
        result.get(timeout=TIMEOUT)
        redis_messages = list(redis_connection.lrange('redis-echo', 0, -1))

        expected_messages = [b'In A', b'In B', b'In/Out C', b'Out B',
                             b'Out A']
        assert redis_messages == expected_messages

    @flaky
    def test_parent_ids(self, manager, num=10):
        assert_ping(manager)

        c = chain(ids.si(i=i) for i in range(num))
        c.freeze()
        res = c()
        try:
            res.get(timeout=TIMEOUT)
        except TimeoutError:
            print(manager.inspect().active())
            print(manager.inspect().reserved())
            print(manager.inspect().stats())
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

    @flaky
    def test_chain_of_task_a_group_and_a_chord(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c = add.si(1, 0)
        c = c | group(add.s(1), add.s(1))
        c = c | group(tsum.s(), tsum.s())
        c = c | tsum.s()

        res = c()
        assert res.get(timeout=TIMEOUT) == 8

    @flaky
    def test_chain_of_chords_as_groups_chained_to_a_task_with_two_tasks(self,
                                                                        manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c = add.si(1, 0)
        c = c | group(add.s(1), add.s(1))
        c = c | tsum.s()
        c = c | add.s(1)
        c = c | group(add.s(1), add.s(1))
        c = c | tsum.s()

        res = c()
        assert res.get(timeout=TIMEOUT) == 12

    @flaky
    def test_chain_of_chords_with_two_tasks(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c = add.si(1, 0)
        c = c | group(add.s(1), add.s(1))
        c = c | tsum.s()
        c = c | add.s(1)
        c = c | chord(group(add.s(1), add.s(1)), tsum.s())

        res = c()
        assert res.get(timeout=TIMEOUT) == 12

    @flaky
    def test_chain_of_a_chord_and_a_group_with_two_tasks(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c = add.si(1, 0)
        c = c | group(add.s(1), add.s(1))
        c = c | tsum.s()
        c = c | add.s(1)
        c = c | group(add.s(1), add.s(1))

        res = c()
        assert res.get(timeout=TIMEOUT) == [6, 6]

    @flaky
    def test_chain_of_a_chord_and_a_task_and_a_group(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c = group(add.s(1, 1), add.s(1, 1))
        c = c | tsum.s()
        c = c | add.s(1)
        c = c | group(add.s(1), add.s(1))

        res = c()
        assert res.get(timeout=TIMEOUT) == [6, 6]

    @flaky
    def test_chain_of_a_chord_and_two_tasks_and_a_group(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c = group(add.s(1, 1), add.s(1, 1))
        c = c | tsum.s()
        c = c | add.s(1)
        c = c | add.s(1)
        c = c | group(add.s(1), add.s(1))

        res = c()
        assert res.get(timeout=TIMEOUT) == [7, 7]

    @flaky
    def test_chain_of_a_chord_and_three_tasks_and_a_group(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c = group(add.s(1, 1), add.s(1, 1))
        c = c | tsum.s()
        c = c | add.s(1)
        c = c | add.s(1)
        c = c | add.s(1)
        c = c | group(add.s(1), add.s(1))

        res = c()
        assert res.get(timeout=TIMEOUT) == [8, 8]

    @pytest.mark.xfail(raises=TimeoutError, reason="Task is timeout")
    def test_nested_chain_group_lone(self, manager):
        """
        Test that a lone group in a chain completes.
        """
        sig = chain(
            group(identity.s(42), identity.s(42)),  # [42, 42]
        )
        res = sig.delay()
        assert res.get(timeout=TIMEOUT / 10) == [42, 42]

    def test_nested_chain_group_mid(self, manager):
        """
        Test that a mid-point group in a chain completes.
        """
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        sig = chain(
            identity.s(42),  # 42
            group(identity.s(), identity.s()),  # [42, 42]
            identity.s(),  # [42, 42]
        )
        res = sig.delay()
        assert res.get(timeout=TIMEOUT) == [42, 42]

    def test_nested_chain_group_last(self, manager):
        """
        Test that a final group in a chain with preceding tasks completes.
        """
        sig = chain(
            identity.s(42),  # 42
            group(identity.s(), identity.s()),  # [42, 42]
        )
        res = sig.delay()
        assert res.get(timeout=TIMEOUT) == [42, 42]

    def test_chain_replaced_with_a_chain_and_a_callback(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        redis_connection = get_redis_connection()
        redis_connection.delete('redis-echo')

        link_msg = 'Internal chain callback'
        c = chain(
            identity.s('Hello '),
            # The replacement chain will pass its args though
            replace_with_chain.s(link_msg=link_msg),
            add.s('world'),
        )
        res = c.delay()

        assert res.get(timeout=TIMEOUT) == 'Hello world'
        await_redis_echo({link_msg, })

    def test_chain_replaced_with_a_chain_and_an_error_callback(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        redis_connection = get_redis_connection()
        redis_connection.delete('redis-echo')

        link_msg = 'Internal chain errback'
        c = chain(
            identity.s('Hello '),
            replace_with_chain_which_raises.s(link_msg=link_msg),
            add.s(' will never be seen :(')
        )
        res = c.delay()

        with pytest.raises(ValueError):
            res.get(timeout=TIMEOUT)
        await_redis_echo({link_msg, })

    def test_chain_with_cb_replaced_with_chain_with_cb(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        redis_connection = get_redis_connection()
        redis_connection.delete('redis-echo')

        link_msg = 'Internal chain callback'
        c = chain(
            identity.s('Hello '),
            # The replacement chain will pass its args though
            replace_with_chain.s(link_msg=link_msg),
            add.s('world'),
        )
        c.link(redis_echo.s())
        res = c.delay()

        assert res.get(timeout=TIMEOUT) == 'Hello world'
        await_redis_echo({link_msg, 'Hello world'})

    def test_chain_flattening_keep_links_of_inner_chain(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        redis_connection = get_redis_connection()

        link_b_msg = 'link_b called'
        link_b_key = 'echo_link_b'
        link_b_sig = redis_echo.si(link_b_msg, redis_key=link_b_key)

        def link_chain(sig):
            sig.link(link_b_sig)
            sig.link_error(identity.s('link_ab'))
            return sig

        inner_chain = link_chain(chain(identity.s('a'), add.s('b')))
        flat_chain = chain(inner_chain, add.s('c'))
        redis_connection.delete(link_b_key)
        res = flat_chain.delay()

        assert res.get(timeout=TIMEOUT) == 'abc'
        await_redis_echo((link_b_msg,), redis_key=link_b_key)

    def test_chain_with_eb_replaced_with_chain_with_eb(
        self, manager, subtests
    ):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        redis_connection = get_redis_connection()
        redis_connection.delete('redis-echo')

        inner_link_msg = 'Internal chain errback'
        outer_link_msg = 'External chain errback'
        c = chain(
            identity.s('Hello '),
            # The replacement chain will die and break the encapsulating chain
            replace_with_chain_which_raises.s(link_msg=inner_link_msg),
            add.s('world'),
        )
        c.link_error(redis_echo.si(outer_link_msg))
        res = c.delay()

        with subtests.test(msg="Chain fails due to a child task dying"):
            with pytest.raises(ValueError):
                res.get(timeout=TIMEOUT)
        with subtests.test(msg="Chain and child task callbacks are called"):
            await_redis_echo({inner_link_msg, outer_link_msg})

    def test_replace_chain_with_empty_chain(self, manager):
        r = chain(identity.s(1), replace_with_empty_chain.s()).delay()

        with pytest.raises(ImproperlyConfigured,
                           match="Cannot replace with an empty chain"):
            r.get(timeout=TIMEOUT)

    def test_chain_children_with_callbacks(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        redis_key = str(uuid.uuid4())
        callback = redis_count.si(redis_key=redis_key)

        child_task_count = 42
        child_sig = identity.si(1337)
        child_sig.link(callback)
        chain_sig = chain(child_sig for _ in range(child_task_count))

        redis_connection.delete(redis_key)
        with subtests.test(msg="Chain executes as expected"):
            res_obj = chain_sig()
            assert res_obj.get(timeout=TIMEOUT) == 1337
        with subtests.test(msg="Chain child task callbacks are called"):
            await_redis_count(child_task_count, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_chain_children_with_errbacks(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        redis_key = str(uuid.uuid4())
        errback = redis_count.si(redis_key=redis_key)

        child_task_count = 42
        child_sig = fail.si()
        child_sig.link_error(errback)
        chain_sig = chain(child_sig for _ in range(child_task_count))

        redis_connection.delete(redis_key)
        with subtests.test(msg="Chain fails due to a child task dying"):
            res_obj = chain_sig()
            with pytest.raises(ExpectedException):
                res_obj.get(timeout=TIMEOUT)
        with subtests.test(msg="Chain child task errbacks are called"):
            # Only the first child task gets a change to run and fail
            await_redis_count(1, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_chain_with_callback_child_replaced(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        redis_key = str(uuid.uuid4())
        callback = redis_count.si(redis_key=redis_key)

        chain_sig = chain(add_replaced.si(42, 1337), identity.s())
        chain_sig.link(callback)

        redis_connection.delete(redis_key)
        with subtests.test(msg="Chain executes as expected"):
            res_obj = chain_sig()
            assert res_obj.get(timeout=TIMEOUT) == 42 + 1337
        with subtests.test(msg="Callback is called after chain finishes"):
            await_redis_count(1, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_chain_with_errback_child_replaced(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        redis_key = str(uuid.uuid4())
        errback = redis_count.si(redis_key=redis_key)

        chain_sig = chain(add_replaced.si(42, 1337), fail.s())
        chain_sig.link_error(errback)

        redis_connection.delete(redis_key)
        with subtests.test(msg="Chain executes as expected"):
            res_obj = chain_sig()
            with pytest.raises(ExpectedException):
                res_obj.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after chain finishes"):
            await_redis_count(1, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_chain_child_with_callback_replaced(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        redis_key = str(uuid.uuid4())
        callback = redis_count.si(redis_key=redis_key)

        child_sig = add_replaced.si(42, 1337)
        child_sig.link(callback)
        chain_sig = chain(child_sig, identity.s())

        redis_connection.delete(redis_key)
        with subtests.test(msg="Chain executes as expected"):
            res_obj = chain_sig()
            assert res_obj.get(timeout=TIMEOUT) == 42 + 1337
        with subtests.test(msg="Callback is called after chain finishes"):
            await_redis_count(1, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_chain_child_with_errback_replaced(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        redis_key = str(uuid.uuid4())
        errback = redis_count.si(redis_key=redis_key)

        child_sig = fail_replaced.si()
        child_sig.link_error(errback)
        chain_sig = chain(child_sig, identity.si(42))

        redis_connection.delete(redis_key)
        with subtests.test(msg="Chain executes as expected"):
            res_obj = chain_sig()
            with pytest.raises(ExpectedException):
                res_obj.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after chain finishes"):
            await_redis_count(1, redis_key=redis_key)
        redis_connection.delete(redis_key)

    @pytest.mark.xfail(raises=TimeoutError,
                       reason="Task is timeout instead of returning exception on rpc backend",
                       strict=False)
    def test_task_replaced_with_chain(self, manager):
        orig_sig = replace_with_chain.si(42)
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == 42

    def test_chain_child_replaced_with_chain_first(self, manager):
        orig_sig = chain(replace_with_chain.si(42), identity.s())
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == 42

    def test_chain_child_replaced_with_chain_middle(self, manager):
        orig_sig = chain(
            identity.s(42), replace_with_chain.s(), identity.s()
        )
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == 42

    @pytest.mark.xfail(raises=TimeoutError,
                       reason="Task is timeout instead of returning exception on rpc backend",
                       strict=False)
    def test_chain_child_replaced_with_chain_last(self, manager):
        orig_sig = chain(identity.s(42), replace_with_chain.s())
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == 42

    @pytest.mark.parametrize('redis_key', ['redis-group-ids'])
    def test_chord_header_id_duplicated_on_rabbitmq_msg_duplication(self, manager, subtests, celery_session_app,
                                                                    redis_key):
        """
        When a task that predates a chord in a chain was duplicated by Rabbitmq (for whatever reason),
        the chord header id was not duplicated. This caused the chord header to have a different id.
        This test ensures that the chord header's id preserves itself in face of such an edge case.
        To validate the correct behavior is implemented, we collect the original and duplicated chord header ids
        in redis, to ensure that they are the same.
        """

        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        if manager.app.conf.broker_url.startswith('redis'):
            raise pytest.xfail('Redis broker does not duplicate the task (t1)')

        # Republish t1 to cause the chain to be executed twice
        @before_task_publish.connect
        def before_task_publish_handler(sender=None, body=None, exchange=None, routing_key=None, headers=None,
                                        properties=None,
                                        declare=None, retry_policy=None, **kwargs):
            """ We want to republish t1 to ensure that the chain is executed twice """

            metadata = {
                'body': body,
                'exchange': exchange,
                'routing_key': routing_key,
                'properties': properties,
                'headers': headers,
            }

            with celery_session_app.producer_pool.acquire(block=True) as producer:
                # Publish t1 to the message broker, just before it's going to be published which causes duplication
                return producer.publish(
                    metadata['body'],
                    exchange=metadata['exchange'],
                    routing_key=metadata['routing_key'],
                    retry=None,
                    retry_policy=retry_policy,
                    serializer='json',
                    delivery_mode=None,
                    headers=headers,
                    **kwargs
                )

        # Clean redis key
        redis_connection = get_redis_connection()
        if redis_connection.exists(redis_key):
            redis_connection.delete(redis_key)

        # Prepare tasks
        t1, t2, t3, t4 = identity.s(42), redis_echo_group_id.s(), identity.s(), identity.s()
        c = chain(t1, chord([t2, t3], t4))

        # Delay chain
        r1 = c.delay()
        r1.get(timeout=TIMEOUT)

        # Cleanup
        before_task_publish.disconnect(before_task_publish_handler)

        with subtests.test(msg='Compare group ids via redis list'):
            await_redis_list_message_length(2, redis_key=redis_key, timeout=15)
            compare_group_ids_in_redis(redis_key=redis_key)

        # Cleanup
        redis_connection = get_redis_connection()
        redis_connection.delete(redis_key)

    def test_chaining_upgraded_chords_pure_groups(self, manager, subtests):
        """ This test is built to reproduce the github issue https://github.com/celery/celery/issues/5958

        The issue describes a canvas where a chain of groups are executed multiple times instead of once.
        This test is built to reproduce the issue and to verify that the issue is fixed.
        """
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        redis_connection = get_redis_connection()
        redis_key = 'echo_chamber'

        c = chain(
            # letting the chain upgrade the chord, reproduces the issue in _chord.__or__
            group(
                redis_echo.si('1', redis_key=redis_key),
                redis_echo.si('2', redis_key=redis_key),
                redis_echo.si('3', redis_key=redis_key),
            ),
            group(
                redis_echo.si('4', redis_key=redis_key),
                redis_echo.si('5', redis_key=redis_key),
                redis_echo.si('6', redis_key=redis_key),
            ),
            group(
                redis_echo.si('7', redis_key=redis_key),
            ),
            group(
                redis_echo.si('8', redis_key=redis_key),
            ),
            redis_echo.si('9', redis_key=redis_key),
            redis_echo.si('Done', redis_key='Done'),
        )

        with subtests.test(msg='Run the chain and wait for completion'):
            redis_connection.delete(redis_key, 'Done')
            c.delay().get(timeout=TIMEOUT)
            await_redis_list_message_length(1, redis_key='Done', timeout=10)

        with subtests.test(msg='All tasks are executed once'):
            actual = [sig.decode('utf-8') for sig in redis_connection.lrange(redis_key, 0, -1)]
            expected = [str(i) for i in range(1, 10)]
            with subtests.test(msg='All tasks are executed once'):
                assert sorted(actual) == sorted(expected)

        # Cleanup
        redis_connection.delete(redis_key, 'Done')

    def test_chaining_upgraded_chords_starting_with_chord(self, manager, subtests):
        """ This test is built to reproduce the github issue https://github.com/celery/celery/issues/5958

        The issue describes a canvas where a chain of groups are executed multiple times instead of once.
        This test is built to reproduce the issue and to verify that the issue is fixed.
        """
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        redis_connection = get_redis_connection()
        redis_key = 'echo_chamber'

        c = chain(
            # by manually upgrading the chord to a group, we can reproduce the issue in _chain.__or__
            chord(group([redis_echo.si('1', redis_key=redis_key),
                         redis_echo.si('2', redis_key=redis_key),
                         redis_echo.si('3', redis_key=redis_key)]),
                  group([redis_echo.si('4', redis_key=redis_key),
                         redis_echo.si('5', redis_key=redis_key),
                         redis_echo.si('6', redis_key=redis_key)])),
            group(
                redis_echo.si('7', redis_key=redis_key),
            ),
            group(
                redis_echo.si('8', redis_key=redis_key),
            ),
            redis_echo.si('9', redis_key=redis_key),
            redis_echo.si('Done', redis_key='Done'),
        )

        with subtests.test(msg='Run the chain and wait for completion'):
            redis_connection.delete(redis_key, 'Done')
            c.delay().get(timeout=TIMEOUT)
            await_redis_list_message_length(1, redis_key='Done', timeout=10)

        with subtests.test(msg='All tasks are executed once'):
            actual = [sig.decode('utf-8') for sig in redis_connection.lrange(redis_key, 0, -1)]
            expected = [str(i) for i in range(1, 10)]
            with subtests.test(msg='All tasks are executed once'):
                assert sorted(actual) == sorted(expected)

        # Cleanup
        redis_connection.delete(redis_key, 'Done')

    def test_chaining_upgraded_chords_mixed_canvas(self, manager, subtests):
        """ This test is built to reproduce the github issue https://github.com/celery/celery/issues/5958

        The issue describes a canvas where a chain of groups are executed multiple times instead of once.
        This test is built to reproduce the issue and to verify that the issue is fixed.
        """
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        redis_connection = get_redis_connection()
        redis_key = 'echo_chamber'

        c = chain(
            chord(group([redis_echo.si('1', redis_key=redis_key),
                         redis_echo.si('2', redis_key=redis_key),
                         redis_echo.si('3', redis_key=redis_key)]),
                  group([redis_echo.si('4', redis_key=redis_key),
                         redis_echo.si('5', redis_key=redis_key),
                         redis_echo.si('6', redis_key=redis_key)])),
            redis_echo.si('7', redis_key=redis_key),
            group(
                redis_echo.si('8', redis_key=redis_key),
            ),
            redis_echo.si('9', redis_key=redis_key),
            redis_echo.si('Done', redis_key='Done'),
        )

        with subtests.test(msg='Run the chain and wait for completion'):
            redis_connection.delete(redis_key, 'Done')
            c.delay().get(timeout=TIMEOUT)
            await_redis_list_message_length(1, redis_key='Done', timeout=10)

        with subtests.test(msg='All tasks are executed once'):
            actual = [sig.decode('utf-8') for sig in redis_connection.lrange(redis_key, 0, -1)]
            expected = [str(i) for i in range(1, 10)]
            with subtests.test(msg='All tasks are executed once'):
                assert sorted(actual) == sorted(expected)

        # Cleanup
        redis_connection.delete(redis_key, 'Done')


class test_result_set:

    @flaky
    def test_result_set(self, manager):
        assert_ping(manager)

        rs = ResultSet([add.delay(1, 1), add.delay(2, 2)])
        assert rs.get(timeout=TIMEOUT) == [2, 4]

    @flaky
    def test_result_set_error(self, manager):
        assert_ping(manager)

        rs = ResultSet([raise_error.delay(), add.delay(1, 1)])
        rs.get(timeout=TIMEOUT, propagate=False)

        assert rs.results[0].failed()
        assert rs.results[1].successful()


class test_group:
    @flaky
    def test_ready_with_exception(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        g = group([add.s(1, 2), raise_error.s()])
        result = g.apply_async()
        while not result.ready():
            pass

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
        assert_ping(manager)

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
        assert_ping(manager)

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

    @flaky
    def test_large_group(self, manager):
        assert_ping(manager)

        c = group(identity.s(i) for i in range(1000))
        res = c.delay()

        assert res.get(timeout=TIMEOUT) == list(range(1000))

    def test_group_lone(self, manager):
        """
        Test that a simple group completes.
        """
        sig = group(identity.s(42), identity.s(42))  # [42, 42]
        res = sig.delay()
        assert res.get(timeout=TIMEOUT) == [42, 42]

    def test_nested_group_group(self, manager):
        """
        Confirm that groups nested inside groups get unrolled.
        """
        sig = group(
            group(identity.s(42), identity.s(42)),  # [42, 42]
        )  # [42, 42] due to unrolling
        res = sig.delay()
        assert res.get(timeout=TIMEOUT) == [42, 42]

    def test_nested_group_chord_counting_simple(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        gchild_sig = identity.si(42)
        child_chord = chord((gchild_sig,), identity.s())
        group_sig = group((child_chord,))
        res = group_sig.delay()
        # Wait for the result to land and confirm its value is as expected
        assert res.get(timeout=TIMEOUT) == [[42]]

    def test_nested_group_chord_counting_chain(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        gchild_count = 42
        gchild_sig = chain((identity.si(1337),) * gchild_count)
        child_chord = chord((gchild_sig,), identity.s())
        group_sig = group((child_chord,))
        res = group_sig.delay()
        # Wait for the result to land and confirm its value is as expected
        assert res.get(timeout=TIMEOUT) == [[1337]]

    def test_nested_group_chord_counting_group(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        gchild_count = 42
        gchild_sig = group((identity.si(1337),) * gchild_count)
        child_chord = chord((gchild_sig,), identity.s())
        group_sig = group((child_chord,))
        res = group_sig.delay()
        # Wait for the result to land and confirm its value is as expected
        assert res.get(timeout=TIMEOUT) == [[1337] * gchild_count]

    def test_nested_group_chord_counting_chord(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        gchild_count = 42
        gchild_sig = chord(
            (identity.si(1337),) * gchild_count, identity.si(31337),
        )
        child_chord = chord((gchild_sig,), identity.s())
        group_sig = group((child_chord,))
        res = group_sig.delay()
        # Wait for the result to land and confirm its value is as expected
        assert res.get(timeout=TIMEOUT) == [[31337]]

    def test_nested_group_chord_counting_mixed(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        gchild_count = 42
        child_chord = chord(
            (
                identity.si(42),
                chain((identity.si(42),) * gchild_count),
                group((identity.si(42),) * gchild_count),
                chord((identity.si(42),) * gchild_count, identity.si(1337)),
            ),
            identity.s(),
        )
        group_sig = group((child_chord,))
        res = group_sig.delay()
        # Wait for the result to land and confirm its value is as expected. The
        # group result gets unrolled into the encapsulating chord, hence the
        # weird unpacking below
        assert res.get(timeout=TIMEOUT) == [
            [42, 42, *((42,) * gchild_count), 1337]
        ]

    @pytest.mark.xfail(raises=TimeoutError, reason="#6734")
    def test_nested_group_chord_body_chain(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        child_chord = chord(identity.si(42), chain((identity.s(),)))
        group_sig = group((child_chord,))
        res = group_sig.delay()
        # The result can be expected to timeout since it seems like its
        # underlying promise might not be getting fulfilled (ref #6734). Pick a
        # short timeout since we don't want to block for ages and this is a
        # fairly simple signature which should run pretty quickly.
        expected_result = [[42]]
        with pytest.raises(TimeoutError) as expected_excinfo:
            res.get(timeout=TIMEOUT / 10)
        # Get the child `AsyncResult` manually so that we don't have to wait
        # again for the `GroupResult`
        assert res.children[0].get(timeout=TIMEOUT) == expected_result[0]
        assert res.get(timeout=TIMEOUT) == expected_result
        # Re-raise the expected exception so this test will XFAIL
        raise expected_excinfo.value

    def test_callback_called_by_group(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        callback_msg = str(uuid.uuid4()).encode()
        redis_key = str(uuid.uuid4())
        callback = redis_echo.si(callback_msg, redis_key=redis_key)

        group_sig = group(identity.si(42), identity.si(1337))
        group_sig.link(callback)
        redis_connection.delete(redis_key)
        with subtests.test(msg="Group result is returned"):
            res = group_sig.delay()
            assert res.get(timeout=TIMEOUT) == [42, 1337]
        with subtests.test(msg="Callback is called after group is completed"):
            await_redis_echo({callback_msg, }, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_errback_called_by_group_fail_first(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        errback_msg = str(uuid.uuid4()).encode()
        redis_key = str(uuid.uuid4())
        errback = redis_echo.si(errback_msg, redis_key=redis_key)

        group_sig = group(fail.s(), identity.si(42))
        group_sig.link_error(errback)
        redis_connection.delete(redis_key)
        with subtests.test(msg="Error propagates from group"):
            res = group_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after group task fails"):
            await_redis_echo({errback_msg, }, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_errback_called_by_group_fail_last(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        errback_msg = str(uuid.uuid4()).encode()
        redis_key = str(uuid.uuid4())
        errback = redis_echo.si(errback_msg, redis_key=redis_key)

        group_sig = group(identity.si(42), fail.s())
        group_sig.link_error(errback)
        redis_connection.delete(redis_key)
        with subtests.test(msg="Error propagates from group"):
            res = group_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after group task fails"):
            await_redis_echo({errback_msg, }, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_errback_called_by_group_fail_multiple(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        expected_errback_count = 42
        redis_key = str(uuid.uuid4())
        errback = redis_count.si(redis_key=redis_key)

        # Include a mix of passing and failing tasks
        group_sig = group(
            *(identity.si(42) for _ in range(24)),  # arbitrary task count
            *(fail.s() for _ in range(expected_errback_count)),
        )
        group_sig.link_error(errback)

        redis_connection.delete(redis_key)
        with subtests.test(msg="Error propagates from group"):
            res = group_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after group task fails"):
            await_redis_count(expected_errback_count, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_group_children_with_callbacks(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        redis_key = str(uuid.uuid4())
        callback = redis_count.si(redis_key=redis_key)

        child_task_count = 42
        child_sig = identity.si(1337)
        child_sig.link(callback)
        group_sig = group(child_sig for _ in range(child_task_count))

        redis_connection.delete(redis_key)
        with subtests.test(msg="Chain executes as expected"):
            res_obj = group_sig()
            assert res_obj.get(timeout=TIMEOUT) == [1337] * child_task_count
        with subtests.test(msg="Chain child task callbacks are called"):
            await_redis_count(child_task_count, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_group_children_with_errbacks(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        redis_key = str(uuid.uuid4())
        errback = redis_count.si(redis_key=redis_key)

        child_task_count = 42
        child_sig = fail.si()
        child_sig.link_error(errback)
        group_sig = group(child_sig for _ in range(child_task_count))

        redis_connection.delete(redis_key)
        with subtests.test(msg="Chain fails due to a child task dying"):
            res_obj = group_sig()
            with pytest.raises(ExpectedException):
                res_obj.get(timeout=TIMEOUT)
        with subtests.test(msg="Chain child task errbacks are called"):
            await_redis_count(child_task_count, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_group_with_callback_child_replaced(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        redis_key = str(uuid.uuid4())
        callback = redis_count.si(redis_key=redis_key)

        group_sig = group(add_replaced.si(42, 1337), identity.si(31337))
        group_sig.link(callback)

        redis_connection.delete(redis_key)
        with subtests.test(msg="Chain executes as expected"):
            res_obj = group_sig()
            assert res_obj.get(timeout=TIMEOUT) == [42 + 1337, 31337]
        with subtests.test(msg="Callback is called after group finishes"):
            await_redis_count(1, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_group_with_errback_child_replaced(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        redis_key = str(uuid.uuid4())
        errback = redis_count.si(redis_key=redis_key)

        group_sig = group(add_replaced.si(42, 1337), fail.s())
        group_sig.link_error(errback)

        redis_connection.delete(redis_key)
        with subtests.test(msg="Chain executes as expected"):
            res_obj = group_sig()
            with pytest.raises(ExpectedException):
                res_obj.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after group finishes"):
            await_redis_count(1, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_group_child_with_callback_replaced(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        redis_key = str(uuid.uuid4())
        callback = redis_count.si(redis_key=redis_key)

        child_sig = add_replaced.si(42, 1337)
        child_sig.link(callback)
        group_sig = group(child_sig, identity.si(31337))

        redis_connection.delete(redis_key)
        with subtests.test(msg="Chain executes as expected"):
            res_obj = group_sig()
            assert res_obj.get(timeout=TIMEOUT) == [42 + 1337, 31337]
        with subtests.test(msg="Callback is called after group finishes"):
            await_redis_count(1, redis_key=redis_key)
        redis_connection.delete(redis_key)

    def test_group_child_with_errback_replaced(self, manager, subtests):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        redis_key = str(uuid.uuid4())
        errback = redis_count.si(redis_key=redis_key)

        child_sig = fail_replaced.si()
        child_sig.link_error(errback)
        group_sig = group(child_sig, identity.si(42))

        redis_connection.delete(redis_key)
        with subtests.test(msg="Chain executes as expected"):
            res_obj = group_sig()
            with pytest.raises(ExpectedException):
                res_obj.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after group finishes"):
            await_redis_count(1, redis_key=redis_key)
        redis_connection.delete(redis_key)

    @pytest.mark.xfail(raises=TimeoutError,
                       reason="Task is timeout instead of returning exception on rpc backend",
                       strict=False)
    def test_group_child_replaced_with_chain_first(self, manager):
        orig_sig = group(replace_with_chain.si(42), identity.s(1337))
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == [42, 1337]

    @pytest.mark.xfail(raises=TimeoutError,
                       reason="Task is timeout instead of returning exception on rpc backend",
                       strict=False)
    def test_group_child_replaced_with_chain_middle(self, manager):
        orig_sig = group(
            identity.s(42), replace_with_chain.s(1337), identity.s(31337)
        )
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == [42, 1337, 31337]

    @pytest.mark.xfail(raises=TimeoutError,
                       reason="Task is timeout instead of returning exception on rpc backend",
                       strict=False)
    def test_group_child_replaced_with_chain_last(self, manager):
        orig_sig = group(identity.s(42), replace_with_chain.s(1337))
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == [42, 1337]


def assert_ids(r, expected_value, expected_root_id, expected_parent_id):
    root_id, parent_id, value = r.get(timeout=TIMEOUT)
    assert expected_value == value
    assert root_id == expected_root_id
    assert parent_id == expected_parent_id


def assert_ping(manager):
    ping_result = manager.inspect().ping()
    assert ping_result
    ping_val = list(ping_result.values())[0]
    assert ping_val == {"ok": "pong"}


class test_chord:
    @flaky
    def test_simple_chord_with_a_delay_in_group_save(self, manager, monkeypatch):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        if not isinstance(manager.app.backend, BaseKeyValueStoreBackend):
            raise pytest.skip("The delay may only occur in the cache backend")

        x = BaseKeyValueStoreBackend._apply_chord_incr

        def apply_chord_incr_with_sleep(self, *args, **kwargs):
            sleep(1)
            x(self, *args, **kwargs)

        monkeypatch.setattr(BaseKeyValueStoreBackend,
                            '_apply_chord_incr',
                            apply_chord_incr_with_sleep)

        c = chord(header=[add.si(1, 1), add.si(1, 1)], body=tsum.s())

        result = c()
        assert result.get(timeout=TIMEOUT) == 4

    @pytest.mark.xfail(reason="async_results aren't performed in async way")
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

        channels_before = get_active_redis_channels()
        manager.assert_result_tasks_in_progress_or_completed(async_results)

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
        assert sorted(res.get()) == [0, 5, 6, 7]

    @flaky
    def test_add_chord_to_chord(self, manager):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        c = group([add_chord_to_chord.s([1, 2, 3], 4)]) | identity.s()
        res = c()
        assert sorted(res.get()) == [0, 5 + 6 + 7]

    @flaky
    def test_eager_chord_inside_task(self, manager):
        from .tasks import chord_add

        prev = chord_add.app.conf.task_always_eager
        chord_add.app.conf.task_always_eager = True

        chord_add.apply_async(args=(4, 8), throw=True).get()

        chord_add.app.conf.task_always_eager = prev

    def test_group_chain(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c = (
            add.s(2, 2) |
            group(add.s(i) for i in range(4)) |
            add_to_all.s(8)
        )
        res = c()
        assert res.get(timeout=TIMEOUT) == [12, 13, 14, 15]

    def test_group_kwargs(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])
        c = (
            add.s(2, 2) |
            group(add.s(i) for i in range(4)) |
            add_to_all.s(8)
        )
        res = c.apply_async(kwargs={"z": 1})
        assert res.get(timeout=TIMEOUT) == [13, 14, 15, 16]

    def test_group_args_and_kwargs(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])
        c = (
            group(add.s(i) for i in range(4)) |
            add_to_all.s(8)
        )
        res = c.apply_async(args=(4,), kwargs={"z": 1})
        if manager.app.conf.result_backend.startswith('redis'):
            # for a simple chord like the one above, redis does not guarantee
            # the ordering of the results as a performance trade off.
            assert set(res.get(timeout=TIMEOUT)) == {13, 14, 15, 16}
        else:
            assert res.get(timeout=TIMEOUT) == [13, 14, 15, 16]

    def test_nested_group_chain(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

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

    def test_chord_on_error(self, manager):
        from celery import states

        from .tasks import ExpectedException

        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        # Run the chord and wait for the error callback to finish. Note that
        # this only works for old style callbacks since they get dispatched to
        # run async while new style errbacks are called synchronously so that
        # they can be passed the request object for the failing task.
        c1 = chord(
            header=[add.s(1, 2), add.s(3, 4), fail.s()],
            body=print_unicode.s('This should not be called').on_error(
                errback_old_style.s()),
        )
        res = c1()
        with pytest.raises(ExpectedException):
            res.get(propagate=True)

        # Got to wait for children to populate.
        check = (
            lambda: res.children,
            lambda: res.children[0].children,
            lambda: res.children[0].children[0].result,
        )
        start = monotonic()
        while not all(f() for f in check):
            if monotonic() > start + TIMEOUT:
                raise TimeoutError("Timed out waiting for children")
            sleep(0.1)

        # Extract the results of the successful tasks from the chord.
        #
        # We could do this inside the error handler, and probably would in a
        #  real system, but for the purposes of the test it's obnoxious to get
        #  data out of the error handler.
        #
        # So for clarity of our test, we instead do it here.

        # Use the error callback's result to find the failed task.
        uuid_patt = re.compile(
            r"[0-9A-Fa-f]{8}-([0-9A-Fa-f]{4}-){3}[0-9A-Fa-f]{12}"
        )
        callback_chord_exc = AsyncResult(
            res.children[0].children[0].result
        ).result
        failed_task_id = uuid_patt.search(str(callback_chord_exc))
        assert (failed_task_id is not None), "No task ID in %r" % callback_chord_exc
        failed_task_id = failed_task_id.group()

        # Use new group_id result metadata to get group ID.
        failed_task_result = AsyncResult(failed_task_id)
        original_group_id = failed_task_result._get_task_meta()['group_id']

        # Use group ID to get preserved group result.
        backend = fail.app.backend
        j_key = backend.get_key_for_group(original_group_id, '.j')
        redis_connection = get_redis_connection()
        # The redis key is either a list or zset depending on configuration
        if manager.app.conf.result_backend_transport_options.get(
            'result_chord_ordered', True
        ):
            job_results = redis_connection.zrange(j_key, 0, 3)
        else:
            job_results = redis_connection.lrange(j_key, 0, 3)
        chord_results = [backend.decode(t) for t in job_results]

        # Validate group result
        assert [cr[3] for cr in chord_results if cr[2] == states.SUCCESS] == \
               [3, 7]

        assert len([cr for cr in chord_results if cr[2] != states.SUCCESS]
                   ) == 1

    @flaky
    @pytest.mark.parametrize('size', [3, 4, 5, 6, 7, 8, 9])
    def test_generator(self, manager, size):
        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')

        def assert_generator(file_name):
            for i in range(size):
                sleep(1)
                if i == size - 1:
                    with open(file_name) as file_handle:
                        # ensures chord header generators tasks are processed incrementally #3021
                        assert file_handle.readline() == '0\n', "Chord header was unrolled too early"

                yield write_to_file_and_return_int.s(file_name, i)

        with tempfile.NamedTemporaryFile(mode='w', delete=False) as tmp_file:
            file_name = tmp_file.name
            c = chord(assert_generator(file_name), tsum.s())
            assert c().get(timeout=TIMEOUT) == size * (size - 1) // 2

    @flaky
    def test_parallel_chords(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c1 = chord(group(add.s(1, 2), add.s(3, 4)), tsum.s())
        c2 = chord(group(add.s(1, 2), add.s(3, 4)), tsum.s())
        g = group(c1, c2)
        r = g.delay()

        assert r.get(timeout=TIMEOUT) == [10, 10]

    @flaky
    def test_chord_in_chords_with_chains(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c = chord(
            group([
                chain(
                    add.si(1, 2),
                    chord(
                        group([add.si(1, 2), add.si(1, 2)]),
                        add.si(1, 2),
                    ),
                ),
                chain(
                    add.si(1, 2),
                    chord(
                        group([add.si(1, 2), add.si(1, 2)]),
                        add.si(1, 2),
                    ),
                ),
            ]),
            add.si(2, 2)
        )

        r = c.delay()

        assert r.get(timeout=TIMEOUT) == 4

    @flaky
    def test_chain_chord_chain_chord(self, manager):
        # test for #2573
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])
        c = chain(
            identity.si(1),
            chord(
                [
                    identity.si(2),
                    chain(
                        identity.si(3),
                        chord(
                            [identity.si(4), identity.si(5)],
                            identity.si(6)
                        )
                    )
                ],
                identity.si(7)
            )
        )
        res = c.delay()
        assert res.get(timeout=TIMEOUT) == 7

    @pytest.mark.xfail(reason="Issue #6176")
    def test_chord_in_chain_with_args(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c1 = chain(
            chord(
                [identity.s(), identity.s()],
                identity.s(),
            ),
            identity.s(),
        )
        res1 = c1.apply_async(args=(1,))
        assert res1.get(timeout=TIMEOUT) == [1, 1]
        res1 = c1.apply(args=(1,))
        assert res1.get(timeout=TIMEOUT) == [1, 1]

    @pytest.mark.xfail(reason="Issue #6200")
    def test_chain_in_chain_with_args(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c1 = chain(  # NOTE: This chain should have only 1 chain inside it
            chain(
                identity.s(),
                identity.s(),
            ),
        )

        res1 = c1.apply_async(args=(1,))
        assert res1.get(timeout=TIMEOUT) == 1
        res1 = c1.apply(args=(1,))
        assert res1.get(timeout=TIMEOUT) == 1

    @flaky
    def test_large_header(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c = group(identity.si(i) for i in range(1000)) | tsum.s()
        res = c.delay()
        assert res.get(timeout=TIMEOUT) == 499500

    @flaky
    def test_chain_to_a_chord_with_large_header(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c = identity.si(1) | group(
            identity.s() for _ in range(1000)) | tsum.s()
        res = c.delay()
        assert res.get(timeout=TIMEOUT) == 1000

    @flaky
    def test_priority(self, manager):
        c = chain(return_priority.signature(priority=3))()
        assert c.get(timeout=TIMEOUT) == "Priority: 3"

    @flaky
    def test_priority_chain(self, manager):
        c = return_priority.signature(priority=3) | return_priority.signature(
            priority=5)
        assert c().get(timeout=TIMEOUT) == "Priority: 5"

    def test_nested_chord_group(self, manager):
        """
        Confirm that groups nested inside chords get unrolled.
        """
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        sig = chord(
            (
                group(identity.s(42), identity.s(42)),  # [42, 42]
            ),
            identity.s()  # [42, 42]
        )
        res = sig.delay()
        assert res.get(timeout=TIMEOUT) == [42, 42]

    def test_nested_chord_group_chain_group_tail(self, manager):
        """
        Sanity check that a deeply nested group is completed as expected.

        Groups at the end of chains nested in chords have had issues and this
        simple test sanity check that such a task structure can be completed.
        """
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        sig = chord(
            group(
                chain(
                    identity.s(42),  # 42
                    group(
                        identity.s(),  # 42
                        identity.s(),  # 42
                    ),  # [42, 42]
                ),  # [42, 42]
            ),  # [[42, 42]] since the chain prevents unrolling
            identity.s(),  # [[42, 42]]
        )
        res = sig.delay()
        assert res.get(timeout=TIMEOUT) == [[42, 42]]

    @pytest.mark.xfail(TEST_BACKEND.startswith('redis://'), reason="Issue #6437")
    def test_error_propagates_from_chord(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        sig = add.s(1, 1) | fail.s() | group(add.s(1), add.s(1))
        res = sig.delay()

        with pytest.raises(ExpectedException):
            res.get(timeout=TIMEOUT)

    def test_error_propagates_from_chord2(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        sig = add.s(1, 1) | add.s(1) | group(add.s(1), fail.s())
        res = sig.delay()

        with pytest.raises(ExpectedException):
            res.get(timeout=TIMEOUT)

    def test_error_propagates_to_chord_from_simple(self, manager, subtests):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        child_sig = fail.s()

        chord_sig = chord((child_sig,), identity.s())
        with subtests.test(msg="Error propagates from simple header task"):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)

        chord_sig = chord((identity.si(42),), child_sig)
        with subtests.test(msg="Error propagates from simple body task"):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)

    def test_immutable_errback_called_by_chord_from_simple(
        self, manager, subtests
    ):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        errback_msg = str(uuid.uuid4()).encode()
        redis_key = str(uuid.uuid4())
        errback = redis_echo.si(errback_msg, redis_key=redis_key)
        child_sig = fail.s()

        chord_sig = chord((child_sig,), identity.s())
        chord_sig.link_error(errback)
        redis_connection.delete(redis_key)
        with subtests.test(msg="Error propagates from simple header task"):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(
            msg="Errback is called after simple header task fails"
        ):
            await_redis_echo({errback_msg, }, redis_key=redis_key)

        chord_sig = chord((identity.si(42),), child_sig)
        chord_sig.link_error(errback)
        redis_connection.delete(redis_key)
        with subtests.test(msg="Error propagates from simple body task"):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(
            msg="Errback is called after simple body task fails"
        ):
            await_redis_echo({errback_msg, }, redis_key=redis_key)
        redis_connection.delete(redis_key)

    @pytest.mark.parametrize(
        "errback_task", [errback_old_style, errback_new_style, ],
    )
    def test_mutable_errback_called_by_chord_from_simple(
        self, errback_task, manager, subtests
    ):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        errback = errback_task.s()
        child_sig = fail.s()

        chord_sig = chord((child_sig,), identity.s())
        chord_sig.link_error(errback)
        expected_redis_key = chord_sig.body.freeze().id
        redis_connection.delete(expected_redis_key)
        with subtests.test(msg="Error propagates from simple header task"):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(
            msg="Errback is called after simple header task fails"
        ):
            await_redis_count(1, redis_key=expected_redis_key)

        chord_sig = chord((identity.si(42),), child_sig)
        chord_sig.link_error(errback)
        expected_redis_key = chord_sig.body.freeze().id
        redis_connection.delete(expected_redis_key)
        with subtests.test(msg="Error propagates from simple body task"):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(
            msg="Errback is called after simple body task fails"
        ):
            await_redis_count(1, redis_key=expected_redis_key)
        redis_connection.delete(expected_redis_key)

    def test_error_propagates_to_chord_from_chain(self, manager, subtests):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        child_sig = chain(identity.si(42), fail.s(), identity.si(42))

        chord_sig = chord((child_sig,), identity.s())
        with subtests.test(
            msg="Error propagates from header chain which fails before the end"
        ):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)

        chord_sig = chord((identity.si(42),), child_sig)
        with subtests.test(
            msg="Error propagates from body chain which fails before the end"
        ):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)

    def test_immutable_errback_called_by_chord_from_chain(
        self, manager, subtests
    ):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        errback_msg = str(uuid.uuid4()).encode()
        redis_key = str(uuid.uuid4())
        errback = redis_echo.si(errback_msg, redis_key=redis_key)
        child_sig = chain(identity.si(42), fail.s(), identity.si(42))

        chord_sig = chord((child_sig,), identity.s())
        chord_sig.link_error(errback)
        redis_connection.delete(redis_key)
        with subtests.test(
            msg="Error propagates from header chain which fails before the end"
        ):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(
            msg="Errback is called after header chain which fails before the end"
        ):
            await_redis_echo({errback_msg, }, redis_key=redis_key)

        chord_sig = chord((identity.si(42),), child_sig)
        chord_sig.link_error(errback)
        redis_connection.delete(redis_key)
        with subtests.test(
            msg="Error propagates from body chain which fails before the end"
        ):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(
            msg="Errback is called after body chain which fails before the end"
        ):
            await_redis_echo({errback_msg, }, redis_key=redis_key)
        redis_connection.delete(redis_key)

    @pytest.mark.parametrize(
        "errback_task", [errback_old_style, errback_new_style, ],
    )
    def test_mutable_errback_called_by_chord_from_chain(
        self, errback_task, manager, subtests
    ):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        errback = errback_task.s()
        fail_sig = fail.s()
        fail_sig_id = fail_sig.freeze().id
        child_sig = chain(identity.si(42), fail_sig, identity.si(42))

        chord_sig = chord((child_sig,), identity.s())
        chord_sig.link_error(errback)
        expected_redis_key = chord_sig.body.freeze().id
        redis_connection.delete(expected_redis_key)
        with subtests.test(
            msg="Error propagates from header chain which fails before the end"
        ):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(
            msg="Errback is called after header chain which fails before the end"
        ):
            await_redis_count(1, redis_key=expected_redis_key)

        chord_sig = chord((identity.si(42),), child_sig)
        chord_sig.link_error(errback)
        expected_redis_key = fail_sig_id
        redis_connection.delete(expected_redis_key)
        with subtests.test(
            msg="Error propagates from body chain which fails before the end"
        ):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(
            msg="Errback is called after body chain which fails before the end"
        ):
            await_redis_count(1, redis_key=expected_redis_key)
        redis_connection.delete(expected_redis_key)

    def test_error_propagates_to_chord_from_chain_tail(self, manager, subtests):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        child_sig = chain(identity.si(42), fail.s())

        chord_sig = chord((child_sig,), identity.s())
        with subtests.test(
            msg="Error propagates from header chain which fails at the end"
        ):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)

        chord_sig = chord((identity.si(42),), child_sig)
        with subtests.test(
            msg="Error propagates from body chain which fails at the end"
        ):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)

    def test_immutable_errback_called_by_chord_from_chain_tail(
        self, manager, subtests
    ):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        errback_msg = str(uuid.uuid4()).encode()
        redis_key = str(uuid.uuid4())
        errback = redis_echo.si(errback_msg, redis_key=redis_key)
        child_sig = chain(identity.si(42), fail.s())

        chord_sig = chord((child_sig,), identity.s())
        chord_sig.link_error(errback)
        redis_connection.delete(redis_key)
        with subtests.test(
            msg="Error propagates from header chain which fails at the end"
        ):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(
            msg="Errback is called after header chain which fails at the end"
        ):
            await_redis_echo({errback_msg, }, redis_key=redis_key)

        chord_sig = chord((identity.si(42),), child_sig)
        chord_sig.link_error(errback)
        redis_connection.delete(redis_key)
        with subtests.test(
            msg="Error propagates from body chain which fails at the end"
        ):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(
            msg="Errback is called after body chain which fails at the end"
        ):
            await_redis_echo({errback_msg, }, redis_key=redis_key)
        redis_connection.delete(redis_key)

    @pytest.mark.parametrize(
        "errback_task", [errback_old_style, errback_new_style, ],
    )
    def test_mutable_errback_called_by_chord_from_chain_tail(
        self, errback_task, manager, subtests
    ):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        errback = errback_task.s()
        fail_sig = fail.s()
        fail_sig_id = fail_sig.freeze().id
        child_sig = chain(identity.si(42), fail_sig)

        chord_sig = chord((child_sig,), identity.s())
        chord_sig.link_error(errback)
        expected_redis_key = chord_sig.body.freeze().id
        redis_connection.delete(expected_redis_key)
        with subtests.test(
            msg="Error propagates from header chain which fails at the end"
        ):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(
            msg="Errback is called after header chain which fails at the end"
        ):
            await_redis_count(1, redis_key=expected_redis_key)

        chord_sig = chord((identity.si(42),), child_sig)
        chord_sig.link_error(errback)
        expected_redis_key = fail_sig_id
        redis_connection.delete(expected_redis_key)
        with subtests.test(
            msg="Error propagates from header chain which fails at the end"
        ):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(
            msg="Errback is called after header chain which fails at the end"
        ):
            await_redis_count(1, redis_key=expected_redis_key)
        redis_connection.delete(expected_redis_key)

    def test_error_propagates_to_chord_from_group(self, manager, subtests):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        child_sig = group(identity.si(42), fail.s())

        chord_sig = chord((child_sig,), identity.s())
        with subtests.test(msg="Error propagates from header group"):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)

        chord_sig = chord((identity.si(42),), child_sig)
        with subtests.test(msg="Error propagates from body group"):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)

    def test_immutable_errback_called_by_chord_from_group(
        self, manager, subtests
    ):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        errback_msg = str(uuid.uuid4()).encode()
        redis_key = str(uuid.uuid4())
        errback = redis_echo.si(errback_msg, redis_key=redis_key)
        child_sig = group(identity.si(42), fail.s())

        chord_sig = chord((child_sig,), identity.s())
        chord_sig.link_error(errback)
        redis_connection.delete(redis_key)
        with subtests.test(msg="Error propagates from header group"):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after header group fails"):
            await_redis_echo({errback_msg, }, redis_key=redis_key)

        chord_sig = chord((identity.si(42),), child_sig)
        chord_sig.link_error(errback)
        redis_connection.delete(redis_key)
        with subtests.test(msg="Error propagates from body group"):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after body group fails"):
            await_redis_echo({errback_msg, }, redis_key=redis_key)
        redis_connection.delete(redis_key)

    @pytest.mark.parametrize(
        "errback_task", [errback_old_style, errback_new_style, ],
    )
    def test_mutable_errback_called_by_chord_from_group(
        self, errback_task, manager, subtests
    ):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        errback = errback_task.s()
        fail_sig = fail.s()
        fail_sig_id = fail_sig.freeze().id
        child_sig = group(identity.si(42), fail_sig)

        chord_sig = chord((child_sig,), identity.s())
        chord_sig.link_error(errback)
        expected_redis_key = chord_sig.body.freeze().id
        redis_connection.delete(expected_redis_key)
        with subtests.test(msg="Error propagates from header group"):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after header group fails"):
            await_redis_count(1, redis_key=expected_redis_key)

        chord_sig = chord((identity.si(42),), child_sig)
        chord_sig.link_error(errback)
        expected_redis_key = fail_sig_id
        redis_connection.delete(expected_redis_key)
        with subtests.test(msg="Error propagates from body group"):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after body group fails"):
            await_redis_count(1, redis_key=expected_redis_key)
        redis_connection.delete(expected_redis_key)

    def test_immutable_errback_called_by_chord_from_group_fail_multiple(
        self, manager, subtests
    ):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        fail_task_count = 42
        redis_key = str(uuid.uuid4())
        errback = redis_count.si(redis_key=redis_key)
        # Include a mix of passing and failing tasks
        child_sig = group(
            *(identity.si(42) for _ in range(24)),  # arbitrary task count
            *(fail.s() for _ in range(fail_task_count)),
        )

        chord_sig = chord((child_sig,), identity.s())
        chord_sig.link_error(errback)
        redis_connection.delete(redis_key)
        with subtests.test(msg="Error propagates from header group"):
            redis_connection.delete(redis_key)
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after header group fails"):
            # NOTE: Here we only expect the errback to be called once since it
            # is attached to the chord body which is a single task!
            await_redis_count(1, redis_key=redis_key)

        chord_sig = chord((identity.si(42),), child_sig)
        chord_sig.link_error(errback)
        redis_connection.delete(redis_key)
        with subtests.test(msg="Error propagates from body group"):
            res = chord_sig.delay()
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after body group fails"):
            # NOTE: Here we expect the errback to be called once per failing
            # task in the chord body since it is a group
            await_redis_count(fail_task_count, redis_key=redis_key)
        redis_connection.delete(redis_key)

    @pytest.mark.parametrize("errback_task", [errback_old_style, errback_new_style])
    def test_mutable_errback_called_by_chord_from_group_fail_multiple_on_header_failure(
        self, errback_task, manager, subtests
    ):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        fail_task_count = 42
        # We have to use failing task signatures with unique task IDs to ensure
        # the chord can complete when they are used as part of its header!
        fail_sigs = tuple(
            fail.s() for _ in range(fail_task_count)
        )
        errback = errback_task.s()
        # Include a mix of passing and failing tasks
        child_sig = group(
            *(identity.si(42) for _ in range(8)),  # arbitrary task count
            *fail_sigs,
        )

        chord_sig = chord((child_sig,), identity.s())
        chord_sig.link_error(errback)
        expected_redis_key = chord_sig.body.freeze().id
        redis_connection.delete(expected_redis_key)
        with subtests.test(msg="Error propagates from header group"):
            res = chord_sig.delay()
            sleep(1)
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after header group fails"):
            # NOTE: Here we only expect the errback to be called once since it
            # is attached to the chord body which is a single task!
            await_redis_count(1, redis_key=expected_redis_key)

    @pytest.mark.parametrize("errback_task", [errback_old_style, errback_new_style])
    def test_mutable_errback_called_by_chord_from_group_fail_multiple_on_body_failure(
        self, errback_task, manager, subtests
    ):
        if not manager.app.conf.result_backend.startswith("redis"):
            raise pytest.skip("Requires redis result backend.")
        redis_connection = get_redis_connection()

        fail_task_count = 42
        # We have to use failing task signatures with unique task IDs to ensure
        # the chord can complete when they are used as part of its header!
        fail_sigs = tuple(
            fail.s() for _ in range(fail_task_count)
        )
        fail_sig_ids = tuple(s.freeze().id for s in fail_sigs)
        errback = errback_task.s()
        # Include a mix of passing and failing tasks
        child_sig = group(
            *(identity.si(42) for _ in range(8)),  # arbitrary task count
            *fail_sigs,
        )

        chord_sig = chord((identity.si(42),), child_sig)
        chord_sig.link_error(errback)
        for fail_sig_id in fail_sig_ids:
            redis_connection.delete(fail_sig_id)
        with subtests.test(msg="Error propagates from body group"):
            res = chord_sig.delay()
            sleep(1)
            with pytest.raises(ExpectedException):
                res.get(timeout=TIMEOUT)
        with subtests.test(msg="Errback is called after body group fails"):
            # NOTE: Here we expect the errback to be called once per failing
            # task in the chord body since it is a group, and each task has a
            # unique task ID
            for i, fail_sig_id in enumerate(fail_sig_ids):
                await_redis_count(
                    1, redis_key=fail_sig_id,
                    # After the first one is seen, check the rest with no
                    # timeout since waiting to confirm that each one doesn't
                    # get over-incremented will take a long time
                    timeout=TIMEOUT if i == 0 else 0,
                )
        for fail_sig_id in fail_sig_ids:
            redis_connection.delete(fail_sig_id)

    def test_chord_header_task_replaced_with_chain(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        orig_sig = chord(
            replace_with_chain.si(42),
            identity.s(),
        )
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == [42]

    def test_chord_header_child_replaced_with_chain_first(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        orig_sig = chord(
            (replace_with_chain.si(42), identity.s(1337),),
            identity.s(),
        )
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == [42, 1337]

    def test_chord_header_child_replaced_with_chain_middle(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        orig_sig = chord(
            (identity.s(42), replace_with_chain.s(1337), identity.s(31337),),
            identity.s(),
        )
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == [42, 1337, 31337]

    def test_chord_header_child_replaced_with_chain_last(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        orig_sig = chord(
            (identity.s(42), replace_with_chain.s(1337),),
            identity.s(),
        )
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == [42, 1337]

    def test_chord_body_task_replaced_with_chain(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        orig_sig = chord(
            identity.s(42),
            replace_with_chain.s(),
        )
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == [42]

    def test_chord_body_chain_child_replaced_with_chain_first(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        orig_sig = chord(
            identity.s(42),
            chain(replace_with_chain.s(), identity.s(), ),
        )
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == [42]

    def test_chord_body_chain_child_replaced_with_chain_middle(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        orig_sig = chord(
            identity.s(42),
            chain(identity.s(), replace_with_chain.s(), identity.s(), ),
        )
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == [42]

    def test_chord_body_chain_child_replaced_with_chain_last(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        orig_sig = chord(
            identity.s(42),
            chain(identity.s(), replace_with_chain.s(), ),
        )
        res_obj = orig_sig.delay()
        assert res_obj.get(timeout=TIMEOUT) == [42]

    def test_enabling_flag_allow_error_cb_on_chord_header(self, manager, subtests):
        """
        Test that the flag allow_error_callback_on_chord_header works as
        expected. To confirm this, we create a chord with a failing header
        task, and check that the body does not execute when the header task fails.
        This allows preventing the body from executing when the chord header fails
        when the flag is turned on. In addition, we make sure the body error callback
        is also executed when the header fails and the flag is turned on.
        """
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')
        redis_connection = get_redis_connection()

        manager.app.conf.task_allow_error_cb_on_chord_header = True

        header_errback_msg = 'header errback called'
        header_errback_key = 'echo_header_errback'
        header_errback_sig = redis_echo.si(header_errback_msg, redis_key=header_errback_key)

        body_errback_msg = 'body errback called'
        body_errback_key = 'echo_body_errback'
        body_errback_sig = redis_echo.si(body_errback_msg, redis_key=body_errback_key)

        body_msg = 'chord body called'
        body_key = 'echo_body'
        body_sig = redis_echo.si(body_msg, redis_key=body_key)

        headers = (
            (fail.si(),),
            (fail.si(), fail.si(), fail.si()),
            (fail.si(), identity.si(42)),
            (fail.si(), identity.si(42), identity.si(42)),
            (fail.si(), identity.si(42), fail.si()),
            (fail.si(), identity.si(42), fail.si(), identity.si(42)),
            (fail.si(), identity.si(42), fail.si(), identity.si(42), fail.si()),
        )

        # for some reason using parametrize breaks the test so we do it manually unfortunately
        for header in headers:
            chord_sig = chord(header, body_sig)
            # link error to chord header ONLY
            [header_task.link_error(header_errback_sig) for header_task in chord_sig.tasks]
            # link error to chord body ONLY
            chord_sig.body.link_error(body_errback_sig)
            redis_connection.delete(header_errback_key, body_errback_key, body_key)

            with subtests.test(msg='Error propagates from failure in header'):
                res = chord_sig.delay()
                with pytest.raises(ExpectedException):
                    res.get(timeout=TIMEOUT)

            with subtests.test(msg='Confirm the body was not executed'):
                with pytest.raises(TimeoutError):
                    # confirm the chord body was not called
                    await_redis_echo((body_msg,), redis_key=body_key, timeout=10)
                # Double check
                assert not redis_connection.exists(body_key), 'Chord body was called when it should have not'

            with subtests.test(msg='Confirm the errback was called for each failed header task + body'):
                # confirm the errback was called for each task in the chord header
                failed_header_tasks_count = len(list(filter(lambda f_sig: f_sig == fail.si(), header)))
                expected_header_errbacks = tuple(header_errback_msg for _ in range(failed_header_tasks_count))
                await_redis_echo(expected_header_errbacks, redis_key=header_errback_key)

                # confirm the errback was called for the chord body
                await_redis_echo((body_errback_msg,), redis_key=body_errback_key)

            redis_connection.delete(header_errback_key, body_errback_key)

    def test_disabling_flag_allow_error_cb_on_chord_header(self, manager, subtests):
        """
        Confirm that when allow_error_callback_on_chord_header is disabled, the default
        behavior is kept.
        """
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')
        redis_connection = get_redis_connection()

        manager.app.conf.task_allow_error_cb_on_chord_header = False

        errback_msg = 'errback called'
        errback_key = 'echo_errback'
        errback_sig = redis_echo.si(errback_msg, redis_key=errback_key)

        body_msg = 'chord body called'
        body_key = 'echo_body'
        body_sig = redis_echo.si(body_msg, redis_key=body_key)

        headers = (
            (fail.si(),),
            (fail.si(), fail.si(), fail.si()),
            (fail.si(), identity.si(42)),
            (fail.si(), identity.si(42), identity.si(42)),
            (fail.si(), identity.si(42), fail.si()),
            (fail.si(), identity.si(42), fail.si(), identity.si(42)),
            (fail.si(), identity.si(42), fail.si(), identity.si(42), fail.si()),
        )

        # for some reason using parametrize breaks the test so we do it manually unfortunately
        for header in headers:
            chord_sig = chord(header, body_sig)
            chord_sig.link_error(errback_sig)
            redis_connection.delete(errback_key, body_key)

            with subtests.test(msg='Error propagates from failure in header'):
                res = chord_sig.delay()
                with pytest.raises(ExpectedException):
                    res.get(timeout=TIMEOUT)

            with subtests.test(msg='Confirm the body was not executed'):
                with pytest.raises(TimeoutError):
                    # confirm the chord body was not called
                    await_redis_echo((body_msg,), redis_key=body_key, timeout=10)
                # Double check
                assert not redis_connection.exists(body_key), 'Chord body was called when it should have not'

            with subtests.test(msg='Confirm only one errback was called'):
                await_redis_echo((errback_msg,), redis_key=errback_key, timeout=10)
                with pytest.raises(TimeoutError):
                    await_redis_echo((errback_msg,), redis_key=errback_key, timeout=10)

            # Cleanup
            redis_connection.delete(errback_key)

    def test_flag_allow_error_cb_on_chord_header_on_upgraded_chord(self, manager, subtests):
        """
        Confirm that allow_error_callback_on_chord_header flag supports upgraded chords
        """
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        if not manager.app.conf.result_backend.startswith('redis'):
            raise pytest.skip('Requires redis result backend.')
        redis_connection = get_redis_connection()

        manager.app.conf.task_allow_error_cb_on_chord_header = True

        errback_msg = 'errback called'
        errback_key = 'echo_errback'
        errback_sig = redis_echo.si(errback_msg, redis_key=errback_key)

        body_msg = 'chord body called'
        body_key = 'echo_body'
        body_sig = redis_echo.si(body_msg, redis_key=body_key)

        headers = (
            # (fail.si(),),  <-- this is not supported because it's not a valid chord header (only one task)
            (fail.si(), fail.si(), fail.si()),
            (fail.si(), identity.si(42)),
            (fail.si(), identity.si(42), identity.si(42)),
            (fail.si(), identity.si(42), fail.si()),
            (fail.si(), identity.si(42), fail.si(), identity.si(42)),
            (fail.si(), identity.si(42), fail.si(), identity.si(42), fail.si()),
        )

        # for some reason using parametrize breaks the test so we do it manually unfortunately
        for header in headers:
            implicit_chord_sig = chain(group(list(header)), body_sig)
            implicit_chord_sig.link_error(errback_sig)
            redis_connection.delete(errback_key, body_key)

            with subtests.test(msg='Error propagates from failure in header'):
                res = implicit_chord_sig.delay()
                with pytest.raises(ExpectedException):
                    res.get(timeout=TIMEOUT)

            with subtests.test(msg='Confirm the body was not executed'):
                with pytest.raises(TimeoutError):
                    # confirm the chord body was not called
                    await_redis_echo((body_msg,), redis_key=body_key, timeout=10)
                # Double check
                assert not redis_connection.exists(body_key), 'Chord body was called when it should have not'

            with subtests.test(msg='Confirm the errback was called for each failed header task + body'):
                # confirm the errback was called for each task in the chord header
                failed_header_tasks_count = len(list(filter(lambda f_sig: f_sig.name == fail.si().name, header)))
                expected_errbacks_count = failed_header_tasks_count + 1  # +1 for the body
                expected_errbacks = tuple(errback_msg for _ in range(expected_errbacks_count))
                await_redis_echo(expected_errbacks, redis_key=errback_key)

                # confirm there are not leftovers
                assert not redis_connection.exists(errback_key)

            # Cleanup
            redis_connection.delete(errback_key)


class test_signature_serialization:
    """
    Confirm nested signatures can be rebuilt after passing through a backend.

    These tests are expected to finish and return `None` or raise an exception
    in the error case. The exception indicates that some element of a nested
    signature object was not properly deserialized from its dictionary
    representation, and would explode later on if it were used as a signature.
    """

    def test_rebuild_nested_chain_chain(self, manager):
        sig = chain(
            tasks.return_nested_signature_chain_chain.s(),
            tasks.rebuild_signature.s()
        )
        sig.delay().get(timeout=TIMEOUT)

    def test_rebuild_nested_chain_group(self, manager):
        sig = chain(
            tasks.return_nested_signature_chain_group.s(),
            tasks.rebuild_signature.s()
        )
        sig.delay().get(timeout=TIMEOUT)

    def test_rebuild_nested_chain_chord(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        sig = chain(
            tasks.return_nested_signature_chain_chord.s(),
            tasks.rebuild_signature.s()
        )
        sig.delay().get(timeout=TIMEOUT)

    def test_rebuild_nested_group_chain(self, manager):
        sig = chain(
            tasks.return_nested_signature_group_chain.s(),
            tasks.rebuild_signature.s()
        )
        sig.delay().get(timeout=TIMEOUT)

    def test_rebuild_nested_group_group(self, manager):
        sig = chain(
            tasks.return_nested_signature_group_group.s(),
            tasks.rebuild_signature.s()
        )
        sig.delay().get(timeout=TIMEOUT)

    def test_rebuild_nested_group_chord(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        sig = chain(
            tasks.return_nested_signature_group_chord.s(),
            tasks.rebuild_signature.s()
        )
        sig.delay().get(timeout=TIMEOUT)

    def test_rebuild_nested_chord_chain(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        sig = chain(
            tasks.return_nested_signature_chord_chain.s(),
            tasks.rebuild_signature.s()
        )
        sig.delay().get(timeout=TIMEOUT)

    def test_rebuild_nested_chord_group(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        sig = chain(
            tasks.return_nested_signature_chord_group.s(),
            tasks.rebuild_signature.s()
        )
        sig.delay().get(timeout=TIMEOUT)

    def test_rebuild_nested_chord_chord(self, manager):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        sig = chain(
            tasks.return_nested_signature_chord_chord.s(),
            tasks.rebuild_signature.s()
        )
        sig.delay().get(timeout=TIMEOUT)


class test_stamping_mechanism:
    def test_stamping_workflow(self, manager, subtests):
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        workflow = group(
            add.s(1, 2) | add.s(3),
            add.s(4, 5) | add.s(6),
            identity.si(21),
        ) | group(
            xsum.s(),
            xsum.s(),
        )

        @task_received.connect
        def task_received_handler(request=None, **kwargs):
            nonlocal assertion_result
            link = None
            if request._Request__payload[2]["callbacks"]:
                link = signature(request._Request__payload[2]["callbacks"][0])
            link_error = None
            if request._Request__payload[2]["errbacks"]:
                link_error = signature(request._Request__payload[2]["errbacks"][0])

            assertion_result = all(
                [
                    assertion_result,
                    [stamped_header in request.stamps for stamped_header in request.stamped_headers],
                    [
                        stamped_header in link.options
                        for stamped_header in link.options["stamped_headers"]
                        if link  # the link itself doensn't have a link
                    ],
                    [
                        stamped_header in link_error.options
                        for stamped_header in link_error.options["stamped_headers"]
                        if link_error  # the link_error itself doensn't have a link
                    ],
                ]
            )

        @before_task_publish.connect
        def before_task_publish_handler(
            body=None,
            headers=None,
            **kwargs,
        ):
            nonlocal assertion_result

            assertion_result = all(
                [stamped_header in headers["stamps"] for stamped_header in headers["stamped_headers"]]
            )

        class CustomStampingVisitor(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return {"on_signature": 42}

        with subtests.test("Prepare canvas workflow and stamp it"):
            link_sig = identity.si("link")
            link_error_sig = identity.si("link_error")
            canvas_workflow = workflow
            canvas_workflow.link(link_sig)
            canvas_workflow.link_error(link_error_sig)
            canvas_workflow.stamp(visitor=CustomStampingVisitor())

        with subtests.test("Check canvas was executed successfully"):
            assertion_result = False
            assert canvas_workflow.apply_async().get() == [42] * 2
            assert assertion_result

    def test_stamping_example_canvas(self, manager):
        """Test the stamping example canvas from the examples directory"""
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        c = chain(
            group(identity.s(i) for i in range(1, 4)) | xsum.s(),
            chord(group(mul.s(10) for _ in range(1, 4)), xsum.s()),
        )

        res = c()
        assert res.get(timeout=TIMEOUT) == 180

    def test_stamp_value_type_defined_by_visitor(self, manager, subtests):
        """Test that the visitor can define the type of the stamped value"""

        @before_task_publish.connect
        def before_task_publish_handler(
            sender=None,
            body=None,
            exchange=None,
            routing_key=None,
            headers=None,
            properties=None,
            declare=None,
            retry_policy=None,
            **kwargs,
        ):
            nonlocal task_headers
            task_headers = headers.copy()

        with subtests.test(msg="Test stamping a single value"):

            class CustomStampingVisitor(StampingVisitor):
                def on_signature(self, sig, **headers) -> dict:
                    return {"stamp": 42}

            stamped_task = add.si(1, 1)
            stamped_task.stamp(visitor=CustomStampingVisitor())
            result = stamped_task.freeze()
            task_headers = None
            stamped_task.apply_async()
            assert task_headers is not None
            assert result.get() == 2
            assert "stamps" in task_headers
            assert "stamp" in task_headers["stamps"]
            assert not isinstance(task_headers["stamps"]["stamp"], list)

        with subtests.test(msg="Test stamping a list of values"):

            class CustomStampingVisitor(StampingVisitor):
                def on_signature(self, sig, **headers) -> dict:
                    return {"stamp": [4, 2]}

            stamped_task = add.si(1, 1)
            stamped_task.stamp(visitor=CustomStampingVisitor())
            result = stamped_task.freeze()
            task_headers = None
            stamped_task.apply_async()
            assert task_headers is not None
            assert result.get() == 2
            assert "stamps" in task_headers
            assert "stamp" in task_headers["stamps"]
            assert isinstance(task_headers["stamps"]["stamp"], list)

    def test_properties_not_affected_from_stamping(self, manager, subtests):
        """Test that the task properties are not dirty with stamping visitor entries"""

        @before_task_publish.connect
        def before_task_publish_handler(
            sender=None,
            body=None,
            exchange=None,
            routing_key=None,
            headers=None,
            properties=None,
            declare=None,
            retry_policy=None,
            **kwargs,
        ):
            nonlocal task_headers
            nonlocal task_properties
            task_headers = headers.copy()
            task_properties = properties.copy()

        class CustomStampingVisitor(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return {"stamp": 42}

        stamped_task = add.si(1, 1)
        stamped_task.stamp(visitor=CustomStampingVisitor())
        result = stamped_task.freeze()
        task_headers = None
        task_properties = None
        stamped_task.apply_async()
        assert task_properties is not None
        assert result.get() == 2
        assert "stamped_headers" in task_headers
        stamped_headers = task_headers["stamped_headers"]

        with subtests.test(msg="Test that the task properties are not dirty with stamping visitor entries"):
            assert "stamped_headers" not in task_properties, "stamped_headers key should not be in task properties"
            for stamp in stamped_headers:
                assert stamp not in task_properties, f'The stamp "{stamp}" should not be in the task properties'

    def test_task_received_has_access_to_stamps(self, manager):
        """Make sure that the request has the stamps using the task_received signal"""

        assertion_result = False

        @task_received.connect
        def task_received_handler(sender=None, request=None, signal=None, **kwargs):
            nonlocal assertion_result
            assertion_result = all([stamped_header in request.stamps for stamped_header in request.stamped_headers])

        class CustomStampingVisitor(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return {"stamp": 42}

        stamped_task = add.si(1, 1)
        stamped_task.stamp(visitor=CustomStampingVisitor())
        stamped_task.apply_async().get()
        assert assertion_result

    def test_all_tasks_of_canvas_are_stamped(self, manager, subtests):
        """Test that complex canvas are stamped correctly"""
        try:
            manager.app.backend.ensure_chords_allowed()
        except NotImplementedError as e:
            raise pytest.skip(e.args[0])

        @task_received.connect
        def task_received_handler(**kwargs):
            request = kwargs["request"]
            nonlocal assertion_result

            assertion_result = all(
                [
                    assertion_result,
                    all([stamped_header in request.stamps for stamped_header in request.stamped_headers]),
                    request.stamps["stamp"] == 42,
                ]
            )

        # Using a list because pytest.mark.parametrize does not play well
        canvas = [
            add.s(1, 1),
            group(add.s(1, 1), add.s(2, 2)),
            chain(add.s(1, 1), add.s(2, 2)),
            chord([add.s(1, 1), add.s(2, 2)], xsum.s()),
            chain(group(add.s(0, 0)), add.s(-1)),
            add.s(1, 1) | add.s(10),
            group(add.s(1, 1) | add.s(10), add.s(2, 2) | add.s(20)),
            chain(add.s(1, 1) | add.s(10), add.s(2) | add.s(20)),
            chord([add.s(1, 1) | add.s(10), add.s(2, 2) | add.s(20)], xsum.s()),
            chain(
                chain(add.s(1, 1) | add.s(10), add.s(2) | add.s(20)),
                add.s(3) | add.s(30),
            ),
            chord(
                group(
                    chain(add.s(1, 1), add.s(2)),
                    chord([add.s(3, 3), add.s(4, 4)], xsum.s()),
                ),
                xsum.s(),
            ),
        ]

        for sig in canvas:
            with subtests.test(msg="Assert all tasks are stamped"):

                class CustomStampingVisitor(StampingVisitor):
                    def on_signature(self, sig, **headers) -> dict:
                        return {"stamp": 42}

                stamped_task = sig
                stamped_task.stamp(visitor=CustomStampingVisitor())
                assertion_result = True
                stamped_task.apply_async().get()
                assert assertion_result

    def test_replace_merge_stamps(self, manager):
        """Test that replacing a task keeps the previous and new stamps"""

        @task_received.connect
        def task_received_handler(**kwargs):
            request = kwargs["request"]
            nonlocal assertion_result
            expected_stamp_key = list(StampOnReplace.stamp.keys())[0]
            expected_stamp_value = list(StampOnReplace.stamp.values())[0]

            assertion_result = all(
                [
                    assertion_result,
                    all([stamped_header in request.stamps for stamped_header in request.stamped_headers]),
                    request.stamps["stamp"] == 42,
                    request.stamps[expected_stamp_key] == expected_stamp_value
                    if "replaced_with_me" in request.task_name
                    else True,
                ]
            )

        class CustomStampingVisitor(StampingVisitor):
            def on_signature(self, sig, **headers) -> dict:
                return {"stamp": 42}

        stamped_task = replace_with_stamped_task.s()
        stamped_task.stamp(visitor=CustomStampingVisitor())
        assertion_result = False
        stamped_task.delay()
        assertion_result = True
        sleep(1)
        # stamped_task needs to be stamped with CustomStampingVisitor
        # and the replaced task with both CustomStampingVisitor and StampOnReplace
        assert assertion_result, "All of the tasks should have been stamped"

    def test_linking_stamped_sig(self, manager):
        """Test that linking a callback after stamping will stamp the callback correctly"""

        assertion_result = False

        @task_received.connect
        def task_received_handler(sender=None, request=None, signal=None, **kwargs):
            nonlocal assertion_result
            link = request._Request__payload[2]["callbacks"][0]
            assertion_result = all(
                [stamped_header in link["options"] for stamped_header in link["options"]["stamped_headers"]]
            )

        class FixedMonitoringIdStampingVisitor(StampingVisitor):
            def __init__(self, msg_id):
                self.msg_id = msg_id

            def on_signature(self, sig, **headers):
                mtask_id = self.msg_id
                return {"mtask_id": mtask_id}

        link_sig = identity.si("link_sig")
        stamped_pass_sig = identity.si("passing sig")
        stamped_pass_sig.stamp(visitor=FixedMonitoringIdStampingVisitor(str(uuid.uuid4())))
        stamped_pass_sig.link(link_sig)
        stamped_pass_sig.stamp(visitor=FixedMonitoringIdStampingVisitor("1234"))
        stamped_pass_sig.apply_async().get(timeout=2)
        assert assertion_result

    def test_err_linking_stamped_sig(self, manager):
        """Test that linking an error after stamping will stamp the errlink correctly"""

        assertion_result = False

        @task_received.connect
        def task_received_handler(sender=None, request=None, signal=None, **kwargs):
            nonlocal assertion_result
            link_error = request.errbacks[0]
            assertion_result = all(
                [
                    stamped_header in link_error["options"]
                    for stamped_header in link_error["options"]["stamped_headers"]
                ]
            )

        class FixedMonitoringIdStampingVisitor(StampingVisitor):
            def __init__(self, msg_id):
                self.msg_id = msg_id

            def on_signature(self, sig, **headers):
                mtask_id = self.msg_id
                return {"mtask_id": mtask_id}

        link_error_sig = identity.si("link_error")
        stamped_fail_sig = fail.si()
        stamped_fail_sig.stamp(visitor=FixedMonitoringIdStampingVisitor(str(uuid.uuid4())))
        stamped_fail_sig.link_error(link_error_sig)
        with pytest.raises(ExpectedException):
            stamped_fail_sig.stamp(visitor=FixedMonitoringIdStampingVisitor("1234"))
            stamped_fail_sig.apply_async().get()
        assert assertion_result
