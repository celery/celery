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

   (window1)$ python mysecureapp.py worker -l info

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
    security_key='examples/security/ssl/worker.key',
    security_certificate='examples/security/ssl/worker.pem',
    security_cert_store='examples/security/ssl/*.pem',
    security_digest='sha256',
    task_serializer='auth',
    accept_content=['auth']
)
app.setup_security()


@app.task
def boom():
    return "I am a signed message"


if __name__ == '__main__':
    app.start()
