# protocol v2 implies UTC=True
# 'class' header existing means protocol is v2

properties = {
    'correlation_id': (uuid)task_id,
    'content_type': (string)mime,
    'content_encoding': (string)encoding,

    # optional
    'reply_to': (string)queue_or_url,
}
headers = {
    'lang': (string)'py'
    'class': (string)task,

    # optional
    'method': (string)'',
    'eta': (iso8601)eta,
    'expires'; (iso8601)expires,
    'callbacks': (list)Signature,
    'errbacks': (list)Signature,
    'chain': (list)Signature,  # non-recursive
    'group': (uuid)group_id,
    'chord': (uuid)chord_id,
    'retries': (int)retries,
}

body = (args, kwargs)




Example:

    # chain: add(add(add(2, 2), 4), 8) = 2 + 2 + 4 + 8

    task_id = uuid()
    basic_publish(
        message=json.dumps([[2, 2], {}]),
        application_headers={
            'lang': 'py',
            'class': 'proj.tasks.add',
            'chain': [
                {'task': 'proj.tasks.add', 'args': (4, )},
                {'task': 'proj.tasks.add', 'args': (8, )},
            ]
        }
        properties={
            'correlation_id': task_id,
            'content_type': 'application/json',
            'content_encoding': 'utf-8',
        }
    )
