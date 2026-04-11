import inspect
import sys
import typing

import pytest
from pydantic import BaseModel

from celery.utils.annotations import annotation_issubclass, get_optional_arg, is_none_type


@pytest.mark.parametrize(
    'value,expected',
    ((3, False), ('x', False), (int, False), (type(None), True)),
)
def test_is_none_type(value: typing.Any, expected: bool) -> None:
    assert is_none_type(value) is expected


def test_is_none_type_with_optional_annotations() -> None:
    annotation = typing.Optional[int]
    int_type, none_type = typing.get_args(annotation)
    assert int_type == int  # just to make sure that order is correct
    assert is_none_type(int_type) is False
    assert is_none_type(none_type) is True


def test_get_optional_arg() -> None:
    def func(
        arg: int,
        optional: typing.Optional[int],
        optional2: typing.Union[int, None],
        optional3: typing.Union[None, int],
        not_optional1: typing.Union[str, int],
        not_optional2: typing.Union[str, int, bool],
    ) -> None:
        pass

    parameters = inspect.signature(func).parameters

    assert get_optional_arg(parameters['arg'].annotation) is None
    assert get_optional_arg(parameters['optional'].annotation) is int
    assert get_optional_arg(parameters['optional2'].annotation) is int
    assert get_optional_arg(parameters['optional3'].annotation) is int
    assert get_optional_arg(parameters['not_optional1'].annotation) is None
    assert get_optional_arg(parameters['not_optional2'].annotation) is None


@pytest.mark.skipif(sys.version_info < (3, 10), reason="Notation is only supported in Python 3.10 or newer.")
def test_get_optional_arg_with_pipe_notation() -> None:
    def func(optional: int | None, optional2: None | int) -> None:
        pass

    parameters = inspect.signature(func).parameters

    assert get_optional_arg(parameters['optional'].annotation) is int
    assert get_optional_arg(parameters['optional2'].annotation) is int


def test_annotation_issubclass() -> None:
    def func(
        int_arg: int,
        base_model: BaseModel,
        list_arg: list,  # type: ignore[type-arg]  # what we test
        dict_arg: dict,  # type: ignore[type-arg]  # what we test
        list_typing_arg: typing.List,  # type: ignore[type-arg]  # what we test
        dict_typing_arg: typing.Dict,  # type: ignore[type-arg]  # what we test
        list_typing_generic_arg: typing.List[str],
        dict_typing_generic_arg: typing.Dict[str, str],
    ) -> None:
        pass

    parameters = inspect.signature(func).parameters
    assert annotation_issubclass(parameters['int_arg'].annotation, int) is True
    assert annotation_issubclass(parameters['base_model'].annotation, BaseModel) is True
    assert annotation_issubclass(parameters['list_arg'].annotation, list) is True
    assert annotation_issubclass(parameters['dict_arg'].annotation, dict) is True

    # Here the annotation is simply not a class, so function must return False
    assert annotation_issubclass(parameters['list_typing_arg'].annotation, BaseModel) is False
    assert annotation_issubclass(parameters['dict_typing_arg'].annotation, BaseModel) is False
    assert annotation_issubclass(parameters['list_typing_generic_arg'].annotation, BaseModel) is False
    assert annotation_issubclass(parameters['dict_typing_generic_arg'].annotation, BaseModel) is False


@pytest.mark.skipif(sys.version_info < (3, 9), reason="Notation is only supported in Python 3.9 or newer.")
def test_annotation_issubclass_with_generic_classes() -> None:
    def func(list_arg: list[str], dict_arg: dict[str, str]) -> None:
        pass

    parameters = inspect.signature(func).parameters
    assert annotation_issubclass(parameters['list_arg'].annotation, list) is False
    assert annotation_issubclass(parameters['dict_arg'].annotation, dict) is False

    # issubclass() behaves differently with BaseModel (and maybe other classes?).
    assert annotation_issubclass(parameters['list_arg'].annotation, BaseModel) is False
    assert annotation_issubclass(parameters['dict_arg'].annotation, BaseModel) is False
