"""

Utility functions

"""
import uuid


def chunks(it, n):
    """Split an iterator into chunks with ``n`` elements each.

    Examples

        # n == 2
        >>> x = chunks(iter([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), 2)
        >>> list(x)
        [[0, 1], [2, 3], [4, 5], [6, 7], [8, 9], [10]]

        # n == 3
        >>> x = chunks(iter([0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]), 3)
        >>> list(x)
        [[0, 1, 2], [3, 4, 5], [6, 7, 8], [9, 10]]

    """
    acc = []
    for i, item in enumerate(it):
        if i and not i % n:
            yield acc
            acc = []
        acc.append(item)
    yield acc


def gen_unique_id():
    """Generate a unique id, having - hopefully - a very small chance of
    collission.
    
    For now this is provided by :func:`uuid.uuid4`.
    """
    return str(uuid.uuid4())


def mitemgetter(*keys):
    """Like :func:`operator.itemgetter` but returns `None` on missing keys
    instead of raising :exc:`KeyError`."""
    return lambda dict_: map(dict_.get, keys)
