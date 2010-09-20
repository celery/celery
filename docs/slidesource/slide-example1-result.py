from myapp.tasks import MyTask

res = MyTask.delay(2, 4)
res.get()
#8

res = MyTask.apply_async(args=[8, 4],
                         countdown=5)
res.get()                               # Is executed after 5 seconds.
#32
