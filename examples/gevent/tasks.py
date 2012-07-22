import urllib2

from celery import task


@task(ignore_result=True)
def urlopen(url):
    print('Opening: {0}'.format(url))
    try:
        body = urllib2.urlopen(url).read()
    except Exception as exc:
        print('Exception for {0}: {1!r}'.format(url, exc))
        return url, 0
    print('Done with: {0}'.format(url))
    return url, 1
