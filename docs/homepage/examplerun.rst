
>>> result = add.delay(8, 8)
>>> result.wait() # wait for and return the result
16
