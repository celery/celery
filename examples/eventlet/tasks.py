from celery.decorators import task
from eventlet.green import urllib2


@task(ignore_result=True)
def urlopen(url):
    print("Opening: %r" % (url, ))
    try:
        body = urllib2.urlopen(url).read()
    except Exception, exc:
        print("Exception for %r: %r" % (url, exc, ))
        return url, 0
    print("Done with: %r" % (url, ))
    return url, 1
