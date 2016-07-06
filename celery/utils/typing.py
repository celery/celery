from datetime import datetime, timedelta
from numbers import Number

from typing import (
    Any, AnyStr, Callable, Iterator, Mapping,
    Optional, Sequence, SupportsInt, Tuple, Union,
)

Timeout = Optional[Number]

Interval = Union[Number, timedelta]

Int = Union[SupportsInt, AnyStr]

DatetimeNowFun = Callable[[], datetime]

#: Argument to `dict(x)` like function.
DictArgument = Union[
    Iterator[Tuple[str, Any]],
    Sequence[Tuple[str, Any]],
    Mapping,
]

ExcInfo = Tuple[Any, Any, Any]
