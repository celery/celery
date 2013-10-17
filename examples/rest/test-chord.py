from tasks import add, tsum
from celery import chord

print '1+1 + 2+2 + ... + 100+100 ='
c = chord(add.subtask((i, i)) for i in xrange(100))
result = c(tsum.subtask())
print result.get(timeout=10)
