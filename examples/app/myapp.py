from celery import Celery
from celery.canvas import chain, group

app = Celery(
    "myapp",
    broker="redis://localhost/1",
)


@app.task
def identity(x):
    return x


@app.task
def fail():
    raise Exception("fail")


chain_sig = chain(
    identity.si("start"),
    group(
        identity.si("a"),
        fail.si(),
    ),
    identity.si("break"),
    identity.si("end"),
)

# chain_sig = identity.si("break") | identity.si("end")
# chain_sig.freeze()
# result = chain_sig.apply_async()

if __name__ == "__main__":
    app.start()
