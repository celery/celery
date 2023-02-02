import collections
import uuid
from time import sleep

import pytest
import pytest_subtests  # noqa

from celery import chain, chord, group
from celery.canvas import StampingVisitor
from celery.exceptions import TimeoutError
from celery.signals import before_task_publish, task_received

from . import tasks  # noqa
from .conftest import get_redis_connection
from .tasks import ExpectedException, StampOnReplace, add, fail, identity, mul, replace_with_stamped_task, xsum

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
        e if not isinstance(e, str) else e.encode("utf-8") for e in expected_msgs
    )

    # This can technically wait for `len(expected_msg_or_msgs) * timeout` :/
    while +expected_msgs:
        maybe_key_msg = redis_connection.blpop(redis_key, timeout)
        if maybe_key_msg is None:
            raise TimeoutError(
                "Fetching from {!r} timed out - still awaiting {!r}".format(
                    redis_key, dict(+expected_msgs)
                )
            )
        retrieved_key, msg = maybe_key_msg
        assert retrieved_key.decode("utf-8") == redis_key
        expected_msgs[msg] -= 1  # silently accepts unexpected messages

    # There should be no more elements - block momentarily
    assert redis_connection.blpop(redis_key, min(1, timeout)) is None


def await_redis_list_message_length(
    expected_length, redis_key="redis-group-ids", timeout=TIMEOUT
):
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
        raise TimeoutError(
            f"{redis_key!r} has length of {length}, but expected to be of length {expected_length}"
        )

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


class test_stamping_mechanism:
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

        with subtests.test(
            msg="Test that the task properties are not dirty with stamping visitor entries"
        ):
            assert (
                "stamped_headers" not in task_properties
            ), "stamped_headers key should not be in task properties"
            for stamp in stamped_headers:
                assert (
                    stamp not in task_properties
                ), f'The stamp "{stamp}" should not be in the task properties'

    def test_task_received_has_access_to_stamps(self, manager):
        """Make sure that the request has the stamps using the task_received signal"""

        assertion_result = False

        @task_received.connect
        def task_received_handler(sender=None, request=None, signal=None, **kwargs):
            nonlocal assertion_result
            assertion_result = all(
                [
                    stamped_header in request.stamps
                    for stamped_header in request.stamped_headers
                ]
            )

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
                    all(
                        [
                            stamped_header in request.stamps
                            for stamped_header in request.stamped_headers
                        ]
                    ),
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
                    all(
                        [
                            stamped_header in request.stamps
                            for stamped_header in request.stamped_headers
                        ]
                    ),
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
                [
                    stamped_header in link["options"]
                    for stamped_header in link["options"]["stamped_headers"]
                ]
            )

        class FixedMonitoringIdStampingVisitor(StampingVisitor):
            def __init__(self, msg_id):
                self.msg_id = msg_id

            def on_signature(self, sig, **headers):
                mtask_id = self.msg_id
                return {"mtask_id": mtask_id}

        link_sig = identity.si("link_sig")
        stamped_pass_sig = identity.si("passing sig")
        stamped_pass_sig.stamp(
            visitor=FixedMonitoringIdStampingVisitor(str(uuid.uuid4()))
        )
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
        stamped_fail_sig.stamp(
            visitor=FixedMonitoringIdStampingVisitor(str(uuid.uuid4()))
        )
        stamped_fail_sig.link_error(link_error_sig)
        with pytest.raises(ExpectedException):
            stamped_fail_sig.stamp(visitor=FixedMonitoringIdStampingVisitor("1234"))
            stamped_fail_sig.apply_async().get()
        assert assertion_result
