from tasks import add


print(">>> from tasks import add")
print(">>> add(4, 4)")
res = add(4, 4)
print(repr(res))

print(">>> add.delay(4, 4)")
res = add.delay(4, 4)
print(repr(res))

print(">>> add.delay(4, 4).wait()")
res = add.delay(4, 4).wait()
print(repr(res))
