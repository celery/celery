from celery import task
from eventlet.green import urllib2


@task()
def urlopen(url):
    print('Opening: {0}'.format(url))
    try:
        body = urllib2.urlopen(url).read()
    except Exception as exc:
        print('URL {0} gave error: {1!r}'.format(url, exc))
    return len(body)
