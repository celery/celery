"""

Utility functions

"""

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
