from celery.task.control import revoke

from tasks import add

print '4 + 3 ='
result = add.delay(4, 3)
revoke(result.task_id)
print result.get(timeout=10)
