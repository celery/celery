def ids_in_as_tuple(tuple_repr):
    """Return the id of every node in an as_tuple() tree."""
    ids = []

    def walk(tup):
        if tup is None:
            return
        (res, nodes) = tup
        (node_id, parent) = res
        ids.append(node_id)
        for child in nodes or ():
            walk(child)
        walk(parent)

    walk(tuple_repr)
    return ids
