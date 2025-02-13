from __future__ import annotations

import sys
from typing import TYPE_CHECKING, TypedDict

from packaging.requirements import InvalidRequirement, Requirement
from packaging.utils import canonicalize_name

from tox.tox_env.errors import Fail

if TYPE_CHECKING:
    from pathlib import Path


if sys.version_info >= (3, 11):  # pragma: no cover (py311+)
    import tomllib
else:  # pragma: no cover (py311+)
    import tomli as tomllib

_IncludeGroup = TypedDict("_IncludeGroup", {"include-group": str})


def resolve(root: Path, groups: set[str]) -> set[Requirement]:
    pyproject_file = root / "pyproject.toml"
    if not pyproject_file.exists():  # check if it's static PEP-621 metadata
        return set()
    with pyproject_file.open("rb") as file_handler:
        pyproject = tomllib.load(file_handler)
    dependency_groups = pyproject["dependency-groups"]
    if not isinstance(dependency_groups, dict):
        msg = f"dependency-groups is {type(dependency_groups).__name__} instead of table"
        raise Fail(msg)
    result: set[Requirement] = set()
    for group in groups:
        result = result.union(_resolve_dependency_group(dependency_groups, group))
    return result


def _resolve_dependency_group(
    dependency_groups: dict[str, list[str] | _IncludeGroup], group: str, past_groups: tuple[str, ...] = ()
) -> set[Requirement]:
    if group in past_groups:
        msg = f"Cyclic dependency group include: {group!r} -> {past_groups!r}"
        raise Fail(msg)
    if group not in dependency_groups:
        msg = f"dependency group {group!r} not found"
        raise Fail(msg)
    raw_group = dependency_groups[group]
    if not isinstance(raw_group, list):
        msg = f"dependency group {group!r} is not a list"
        raise Fail(msg)

    result = set()
    for item in raw_group:
        if isinstance(item, str):
            # packaging.requirements.Requirement parsing ensures that this is a valid
            # PEP 508 Dependency Specifier
            # raises InvalidRequirement on failure
            try:
                result.add(Requirement(item))
            except InvalidRequirement as exc:
                msg = f"{item!r} is not valid requirement due to {exc}"
                raise Fail(msg) from exc
        elif isinstance(item, dict) and tuple(item.keys()) == ("include-group",):
            include_group = canonicalize_name(next(iter(item.values())))
            result = result.union(_resolve_dependency_group(dependency_groups, include_group, (*past_groups, group)))
        else:
            msg = f"invalid dependency group item: {item!r}"
            raise Fail(msg)
    return result


__all__ = [
    "resolve",
]
