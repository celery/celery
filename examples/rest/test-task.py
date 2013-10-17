from tasks import add

print '4 + 3 ='
result = add.delay(4, 3)
print result.get(timeout=10)
