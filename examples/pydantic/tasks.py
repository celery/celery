from pydantic import BaseModel

from celery import Celery

app = Celery('tasks', broker='amqp://')


class ArgModel(BaseModel):
    value: int


class ReturnModel(BaseModel):
    value: str


@app.task(pydantic=True)
def x(arg: ArgModel) -> ReturnModel:
    # args/kwargs type hinted as Pydantic model will be converted
    assert isinstance(arg, ArgModel)
    # The returned model will be converted to a dict automatically
    return ReturnModel(value=f"example: {arg.value}")
