.. _protov2draft:

========================================
 Task Message Protocol v2 (Draft Spec.)
========================================

Notes
=====

- Support for multiple languages via the ``lang`` header.

    Worker may redirect the message to a worker that supports
    the language.

- Metadata moved to headers.

    This means that workers/intermediates can inspect the message
    and make decisions based on the headers without decoding
    the payload (which may be language specific, e.g. serialized by the
    Python specific pickle serializer).

- Body is only for language specific data.

    - Python stores args/kwargs in body.

    - If a message uses raw encoding then the raw data
      will be passed as a single argument to the function.

    - Java/C, etc. can use a thrift/protobuf document as the body

- Dispatches to actor based on ``c_type``, ``c_meth`` headers

    ``c_meth`` is unused by python, but may be used in the future
    to specify class+method pairs.

- Chain gains a dedicated field.

    Reducing the chain into a recursive ``callbacks`` argument
    causes problems when the recursion limit is exceeded.

    This is fixed in the new message protocol by specifying
    a list of signatures, each task will then pop a task off the list
    when sending the next message::

        execute_task(message)
        chain = message.headers['chain']
        if chain:
            sig = maybe_signature(chain.pop())
            sig.apply_async(chain=chain)

- ``correlation_id`` replaces ``task_id`` field.


- ``c_shadow`` lets you specify a different name for logs, monitors
  can be used for e.g. meta tasks that calls any function::

    from celery.utils.imports import qualname

    class PickleTask(Task):
        abstract = True

        def unpack_args(self, fun, args=()):
            return fun, args

        def apply_async(self, args, kwargs, **options):
            fun, real_args = self.unpack_args(*args)
            return super(PickleTask, self).apply_async(
                (fun, real_args, kwargs), shadow=qualname(fun), **options
            )

    @app.task(base=PickleTask)
    def call(fun, args, kwargs):
        return fun(*args, **kwargs)



Undecided
---------

- May consider moving callbacks/errbacks/chain into body.

    Will huge lists in headers cause overhead?
    The downside of keeping them in the body is that intermediates
    won't be able to introspect these values.

Definition
==========

.. code-block:: python

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
        'c_type': (string)task,

        # optional
        'c_meth': (string)unused,
        'c_shadow': (string)replace_name,
        'eta': (iso8601)eta,
        'expires'; (iso8601)expires,
        'callbacks': (list)Signature,
        'errbacks': (list)Signature,
        'chain': (list)Signature,  # non-recursive, reversed list of signatures
        'group': (uuid)group_id,
        'chord': (uuid)chord_id,
        'retries': (int)retries,
        'timelimit': (tuple)(soft, hard),
    }

    body = (args, kwargs)

Example
=======

.. code-block:: python

    # chain: add(add(add(2, 2), 4), 8) == 2 + 2 + 4 + 8

    task_id = uuid()
    basic_publish(
        message=json.dumps([[2, 2], {}]),
        application_headers={
            'lang': 'py',
            'c_type': 'proj.tasks.add',
            'chain': [
                # reversed chain list
                {'task': 'proj.tasks.add', 'args': (8, )},
                {'task': 'proj.tasks.add', 'args': (4, )},
            ]
        }
        properties={
            'correlation_id': task_id,
            'content_type': 'application/json',
            'content_encoding': 'utf-8',
        }
    )
