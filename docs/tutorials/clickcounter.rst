.. _tut-clickcounter:

============================================================
 Tutorial: Creating a click counter using Kombu and celery
============================================================

.. contents::
    :local:

Introduction
============

A click counter should be easy, right? Just a simple view that increments
a click in the DB and forwards you to the real destination.

This would work well for most sites, but when traffic starts to increase,
you are likely to bump into problems. One database write for every click is
not good if you have millions of clicks a day.

So what can you do? In this tutorial we will send the individual clicks as
messages using `kombu`, and then process them later with a Celery periodic
task.

Celery and Kombu are excellent in tandem, and while this might not be
the perfect example, you'll at least see one example how of they can be used
to solve a task.

The model
=========

The model is simple, `Click` has the URL as primary key and a number of
clicks for that URL. Its manager, `ClickManager` implements the
`increment_clicks` method, which takes a URL and by how much to increment
its count by.


*clickmuncher/models.py*:

.. code-block:: python

    from django.db import models
    from django.utils.translation import ugettext_lazy as _


    class ClickManager(models.Manager):

        def increment_clicks(self, for_url, increment_by=1):
            """Increment the click count for an URL.

                >>> Click.objects.increment_clicks("http://google.com", 10)

            """
            click, created = self.get_or_create(url=for_url,
                                    defaults={"click_count": increment_by})
            if not created:
                click.click_count += increment_by
                click.save()

            return click.click_count


    class Click(models.Model):
        url = models.URLField(_(u"URL"), verify_exists=False, unique=True)
        click_count = models.PositiveIntegerField(_(u"click_count"),
                                                  default=0)

        objects = ClickManager()

        class Meta:
            verbose_name = _(u"URL clicks")
            verbose_name_plural = _(u"URL clicks")

Using Kombu to send clicks as messages
========================================

The model is normal django stuff, nothing new there. But now we get on to
the messaging. It has been a tradition for me to put the projects messaging
related code in its own `messaging.py` module, and I will continue to do so
here so maybe you can adopt this practice. In this module we have two
functions:

* `send_increment_clicks`

  This function sends a simple message to the broker. The message body only
  contains the URL we want to increment as plain-text, so the exchange and
  routing key play a role here. We use an exchange called `clicks`, with a
  routing key of `increment_click`, so any consumer binding a queue to
  this exchange using this routing key will receive these messages.

* `process_clicks`

  This function processes all currently gathered clicks sent using
  `send_increment_clicks`. Instead of issuing one database query for every
  click it processes all of the messages first, calculates the new click count
  and issues one update per URL. A message that has been received will not be
  deleted from the broker until it has been acknowledged by the receiver, so
  if the receiver dies in the middle of processing the message, it will be
  re-sent at a later point in time. This guarantees delivery and we respect
  this feature here by not acknowledging the message until the clicks has
  actually been written to disk.

  .. note::

    This could probably be optimized further with
    some hand-written SQL, but it will do for now. Let's say it's an exercise
    left for the picky reader, albeit a discouraged one if you can survive
    without doing it.

On to the code...

*clickmuncher/messaging.py*:

.. code-block:: python

    from celery.messaging import establish_connection
    from kombu.compat import Publisher, Consumer
    from clickmuncher.models import Click


    def send_increment_clicks(for_url):
        """Send a message for incrementing the click count for an URL."""
        connection = establish_connection()
        publisher = Publisher(connection=connection,
                              exchange="clicks",
                              routing_key="increment_click",
                              exchange_type="direct")

        publisher.send(for_url)

        publisher.close()
        connection.close()


    def process_clicks():
        """Process all currently gathered clicks by saving them to the
        database."""
        connection = establish_connection()
        consumer = Consumer(connection=connection,
                            queue="clicks",
                            exchange="clicks",
                            routing_key="increment_click",
                            exchange_type="direct")

        # First process the messages: save the number of clicks
        # for every URL.
        clicks_for_url = {}
        messages_for_url = {}
        for message in consumer.iterqueue():
            url = message.body
            clicks_for_url[url] = clicks_for_url.get(url, 0) + 1
            # We also need to keep the message objects so we can ack the
            # messages as processed when we are finished with them.
            if url in messages_for_url:
                messages_for_url[url].append(message)
            else:
                messages_for_url[url] = [message]

        # Then increment the clicks in the database so we only need
        # one UPDATE/INSERT for each URL.
        for url, click_count in clicks_for_urls.items():
            Click.objects.increment_clicks(url, click_count)
            # Now that the clicks has been registered for this URL we can
            # acknowledge the messages
            [message.ack() for message in messages_for_url[url]]

        consumer.close()
        connection.close()


View and URLs
=============

This is also simple stuff, don't think I have to explain this code to you.
The interface is as follows, if you have a link to http://google.com you
would want to count the clicks for, you replace the URL with:

    http://mysite/clickmuncher/count/?u=http://google.com

and the `count` view will send off an increment message and forward you to
that site.

*clickmuncher/views.py*:

.. code-block:: python

    from django.http import HttpResponseRedirect
    from clickmuncher.messaging import send_increment_clicks


    def count(request):
        url = request.GET["u"]
        send_increment_clicks(url)
        return HttpResponseRedirect(url)


*clickmuncher/urls.py*:

.. code-block:: python

    from django.conf.urls.defaults import patterns, url
    from clickmuncher import views

    urlpatterns = patterns("",
        url(r'^$', views.count, name="clickmuncher-count"),
    )


Creating the periodic task
==========================

Processing the clicks every 30 minutes is easy using celery periodic tasks.

*clickmuncher/tasks.py*:

.. code-block:: python

    from celery.task import PeriodicTask
    from clickmuncher.messaging import process_clicks
    from datetime import timedelta


    class ProcessClicksTask(PeriodicTask):
        run_every = timedelta(minutes=30)

        def run(self, **kwargs):
            process_clicks()

We subclass from :class:`celery.task.base.PeriodicTask`, set the `run_every`
attribute and in the body of the task just call the `process_clicks`
function we wrote earlier. 


Finishing
=========

There are still ways to improve this application. The URLs could be cleaned
so the URL http://google.com and http://google.com/ is the same. Maybe it's
even possible to update the click count using a single UPDATE query?

If you have any questions regarding this tutorial, please send a mail to the
mailing-list or come join us in the #celery IRC channel at Freenode:
http://celeryq.org/introduction.html#getting-help
