"""mysecureapp.py

Usage::

   Generate Certificate:
   ```
   mkdir ssl
   openssl req -x509 -newkey rsa:4096 -keyout ssl/worker.key -out ssl/worker.pem -days 365
   # remove passphrase
   openssl rsa -in ssl/worker.key -out ssl/worker.key
   Enter pass phrase for ssl/worker.key:
   writing RSA key
   ```

   cd examples/security

   (window1)$ python mysecureapp.py worker -l info

   (window2)$ cd examples/security
   (window2)$ python
   >>> from mysecureapp import boom
   >>> boom.delay().get()
   "I am a signed message"


"""
from __future__ import absolute_import, unicode_literals
from celery import Celery

app = Celery(
    'mysecureapp',
    broker='redis://localhost:6379/0',
    backend='redis://localhost:6379/0'
)
app.conf.update(
    security_key='ssl/worker.key',
    security_certificate='ssl/worker.pem',
    security_cert_store='ssl/*.pem',
    task_serializer='auth',
    event_serializer='auth',
    accept_content=['auth'],
    result_accept_content=['json']
)
app.setup_security()


@app.task
def boom():
    return "I am a signed message"


if __name__ == '__main__':
    app.start()
