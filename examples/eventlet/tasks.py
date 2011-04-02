from celery.task import task
from eventlet.green import urllib2


@task
def urlopen(url):
    print("Opening: %r" % (url, ))
    try:
        body = urllib2.urlopen(url).read()
    except Exception, exc:
        print("URL %r gave error: %r" % (url, exc))
    return len(body)
