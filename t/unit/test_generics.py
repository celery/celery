import contextlib
import sys
import typing
from typing import get_args

import pytest

from celery.app import Celery
from celery.app.task import Context, Task
from celery.canvas import Signature
from celery.local import class_property
from celery.result import AsyncResult
from celery.utils.objects import FallbackContext
from celery.utils.threads import _FastLocalStack, _LocalStack


class test_Generics:
    def test_Celery__class_getitem__(self):
        app = Celery[Task]()
        assert isinstance(app, Celery), "Celery can be instantiated with type parameters"
        assert get_args(Celery[Task]) == (Task,), "__class_getitem__ returns a GenericAlias"

    def test_Task__class_getitem__(self):
        task = Task[[int], str]()
        assert isinstance(task, Task), "Task can be instantiated with type parameters"
        assert get_args(Task[[int], str]) == ([int], str),  "__class_getitem__ returns a GenericAlias"

    @pytest.mark.usefixtures('depends_on_current_app')
    def test_AsyncResult__class_getitem__(self):
        result = AsyncResult[str]("some-id")
        assert isinstance(result, AsyncResult), "AsyncResult can be instantiated with type parameters"
        assert get_args(AsyncResult[str]) == (str,), "__class_getitem__ returns a GenericAlias"

    def test_Signature__class_getitem__(self):
        s = Signature[str]()
        assert isinstance(s, Signature), "Signature can be instantiated with type parameters"
        assert get_args(Signature[str]) == (str,), "__class_getitem__ returns a GenericAlias"

    def test__LocalStack__class_getitem__(self):
        stack = _LocalStack[Context]()
        assert isinstance(stack, _LocalStack), "_LocalStack can be instantiated with type parameters"
        assert get_args(_LocalStack[Context]) == (Context,), "__class_getitem__ returns a GenericAlias"

    def test__FastLocalStack__class_getitem__(self):
        s = _FastLocalStack[Context]()
        assert isinstance(s, _FastLocalStack), "_FastLocalStack can be instantiated with type parameters"
        assert get_args(_FastLocalStack[Context]) == (Context,), "__class_getitem__ returns a GenericAlias"

    def test_FallbackContext__class_getitem__(self):
        @contextlib.contextmanager
        def make_thing(int_count):
            yield f'dynamic_thing_{int_count}'

        thing_manager = FallbackContext[str, [int]]('static_thing', make_thing)
        assert isinstance(thing_manager, FallbackContext), "FallbackContext can be instantiated with type parameters"
        assert get_args(FallbackContext[str, [int]]) == (str, [int]), "__class_getitem__ returns a GenericAlias"

    @pytest.mark.skipif(sys.version_info < (3, 11), reason="typing.Self is only available in Python 3.11 or newer.")
    def test_class_property__class_getitem__(self):
        class Thing:
            def _get_my_prop(self):
                return "hello"

            def _set_my_prop(self, str_value):
                pass

            my_prop = class_property[typing.Self, str](_get_my_prop, _set_my_prop)

        assert isinstance(Thing.__dict__['my_prop'], class_property), \
            "class_property can be instantiated with type parameters"
        assert Thing.my_prop == "hello", "class_property works as expected"
        assert get_args(class_property[Thing, str]) == (Thing, str), "__class_getitem__ returns a GenericAlias"
