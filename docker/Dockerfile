FROM python:3.6

WORKDIR /celery

COPY requirements requirements
RUN pip install \
  -r requirements/default.txt \
  -r requirements/test.txt \
  -r requirements/test-integration.txt

COPY MANIFEST.in Makefile setup.py setup.cfg tox.ini /celery/
COPY t t
COPY celery celery

RUN pip install .
