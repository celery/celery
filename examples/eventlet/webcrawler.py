"""Recursive webcrawler example.

One problem with this solution is that it does not remember
urls it has already seen.

To add support for this a bloom filter or redis sets can be used.

"""

from __future__ import with_statement

import re
import time
import urlparse

from celery.decorators import task
from eventlet import Timeout
from eventlet.green import urllib2

# http://daringfireball.net/2009/11/liberal_regex_for_matching_urls
url_regex = re.compile(
    r'\b(([\w-]+://?|www[.])[^\s()<>]+ (?:\([\w\d]+\)|([^[:punct:]\s]|/)))')


def domain(url):
    return urlparse.urlsplit(url)[1].split(":")[0]


@task
def crawl(url):
    print("crawling: %r" % (url, ))
    location = domain(url)
    data = ''
    with Timeout(5, False):
        data = urllib2.urlopen(url).read()
    for url_match in url_regex.finditer(data):
        new_url = url_match.group(0)
        # Don't destroy the internet
        if location in domain(new_url):
            crawl.delay(new_url)
            time.sleep(0.3)
