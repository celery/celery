"""Code related to handling annotations."""

import sys
import types
import typing
from inspect import isclass


def is_none_type(value: typing.Any) -> bool:
    """Check if the given value is a NoneType."""
    if sys.version_info < (3, 10):
        # raise Exception('below 3.10', value, type(None))
        return value is type(None)
    return value == types.NoneType  # type: ignore[no-any-return]


def get_optional_arg(annotation: typing.Any) -> typing.Any:
    """Get the argument from an Optional[...] annotation, or None if it is no such annotation."""
    origin = typing.get_origin(annotation)
    if origin != typing.Union and (sys.version_info >= (3, 10) and origin != types.UnionType):
        return None

    union_args = typing.get_args(annotation)
    if len(union_args) != 2:  # Union does _not_ have two members, so it's not an Optional
        return None

    has_none_arg = any(is_none_type(arg) for arg in union_args)
    # There will always be at least one type arg, as we have already established that this is a Union with exactly
    # two members, and both cannot be None (`Union[None, None]` does not work).
    type_arg = next(arg for arg in union_args if not is_none_type(arg))  # pragma: no branch

    if has_none_arg:
        return type_arg
    return None


def annotation_is_class(annotation: typing.Any) -> bool:
    """Test if a given annotation is a class that can be used in isinstance()/issubclass()."""
    # isclass() returns True for generic type hints (e.g. `list[str]`) until Python 3.10.
    # NOTE: The guard for Python 3.9 is because types.GenericAlias is only added in Python 3.9. This is not a problem
    #       as the syntax is added in the same version in the first place.
    if (3, 9) <= sys.version_info < (3, 11) and isinstance(annotation, types.GenericAlias):
        return False
    return isclass(annotation)


def annotation_issubclass(annotation: typing.Any, cls: type) -> bool:
    """Test if a given annotation is of the given subclass."""
    return annotation_is_class(annotation) and issubclass(annotation, cls)
