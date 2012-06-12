from _librabbitmq import Request

from celery import task, uuid, current_app

from datetime import datetime

@task(accept_magic_kwargs=False)
def add(x, y):
    return x + y


app = current_app._get_current_object()

def on_ack(x): pass


eta = datetime.utcnow().isoformat()
expires = datetime.utcnow().isoformat()
task = {"task": add.name, "id": uuid(), "args": (2, 2), "kwargs": {},
        "eta": eta, "expires": expires}

x = Request(task, app=app, on_ack=on_ack, hostname="foo",
            delivery_info={"exchange": "celery", "routing_key": "celery"})

print(x.app)
print(x.name)
print(x.id)
print(x.args)
print(x.kwargs)
print(x.eta)
print(x.expires)
print(x.flags)
print(x.on_ack)
print(x.task)
print(x.delivery_info)

print;
print(x.request_dict)

print;
print(x.acknowledged)
print(x._already_revoked)
print(x._terminate_on_ack)
