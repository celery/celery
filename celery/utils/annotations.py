"""Code related to handling annotations."""

import types
import typing
from inspect import isclass


def is_none_type(value: typing.Any) -> bool:
    """Check if the given value is a NoneType."""
    return value == types.NoneType


def get_optional_arg(annotation: typing.Any) -> typing.Any:
    """Get the argument from a type | None annotation, or None if it is not such an annotation.
    
    Examples:
        - int | None -> int
        - str | None -> str
        - str | int -> None (not an Optional type)
        - None | None -> None (invalid)
    """
    origin = typing.get_origin(annotation)
    if origin != types.UnionType:
        return None

    union_args = typing.get_args(annotation)
    if len(union_args) != 2:  # type1 | type2 does not have two members, so it's not an Optional
        return None

    has_none_arg = any(is_none_type(arg) for arg in union_args)
    # There will always be at least one type arg, as we have already established that this is a type1 | type2 with exactly
    # two members, and both cannot be None (`None | None` does not work).
    type_arg = next(arg for arg in union_args if not is_none_type(arg))  # pragma: no branch

    if has_none_arg:
        return type_arg
    return None


def annotation_is_class(annotation: typing.Any) -> bool:
    """Test if a given annotation is a class that can be used in isinstance()/issubclass()."""
    # Generic type hints (e.g. `list[str]`) are not classes in Python 3.10+
    if isinstance(annotation, types.GenericAlias):
        return False
    return isclass(annotation)


def annotation_issubclass(annotation: typing.Any, cls: type) -> bool:
    """Test if a given annotation is of the given subclass."""
    return annotation_is_class(annotation) and issubclass(annotation, cls)
