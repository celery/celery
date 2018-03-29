.. _whatsnew-4.2:

===========================================
 What's new in Celery 4.2 (windowlicker)
===========================================
:Author: Omer Katz (``omer.drow at gmail.com``)

.. sidebar:: Change history

    What's new documents describe the changes in major versions,
    we also have a :ref:`changelog` that lists the changes in bugfix
    releases (0.0.x), while older series are archived under the :ref:`history`
    section.

Celery is a simple, flexible, and reliable distributed system to
process vast amounts of messages, while providing operations with
the tools required to maintain such a system.

It's a task queue with focus on real-time processing, while also
supporting task scheduling.

Celery has a large and diverse community of users and contributors,
you should come join us :ref:`on IRC <irc-channel>`
or :ref:`our mailing-list <mailing-list>`.

To read more about Celery you should go read the :ref:`introduction <intro>`.

While this version is backward compatible with previous versions
it's important that you read the following section.

This version is officially supported on CPython 2.7, 3.4, 3.5 & 3.6
and is also supported on PyPy.

.. _`website`: http://celeryproject.org/

.. topic:: Table of Contents

    Make sure you read the important notes before upgrading to this version.

.. contents::
    :local:
    :depth: 2

Preface
=======

The 4.2.0 release continues to improve our efforts to provide you with
the best task execution platform for Python.

This release is mainly a bug fix release, ironing out some issues and regressions
found in Celery 4.0.0.

Traditionally, releases were named after `Autechre <https://en.wikipedia.org/wiki/Autechre>`_'s track names.
This release continues this tradition in a slightly different way.
Each major version of Celery will use a different artist's track names as codenames.

From now on, the 4.x series will be codenamed after `Aphex Twin <https://en.wikipedia.org/wiki/Aphex_Twin>`_'s track names.
This release is codenamed after his very famous track, `Windowlicker <https://youtu.be/UBS4Gi1y_nc?t=4m>`_.

Thank you for your support!

*— Omer Katz*

Wall of Contributors
--------------------

Alejandro Varas <alej0varas@gmail.com>
Alex Garel <alex@garel.org>
Alex Hill <alex@hill.net.au>
Alex Zaitsev <azaitsev@gmail.com>
Alexander Ovechkin <frostoov@gmail.com>
Andrew Wong <argsno@gmail.com>
Anton <anton.gladkov@gmail.com>
Anton Gladkov <atn18@yandex-team.ru>
Armenak Baburyan <kanemra@gmail.com>
Asif Saifuddin Auvi <auvipy@users.noreply.github.com>
BR <b.rabiega@gmail.com>
Ben Welsh <ben.welsh@gmail.com>
Bohdan Rybak <bohdan.rybak@gmail.com>
Chris Mitchell <chris.mit7@gmail.com>
DDevine <devine@ddevnet.net>
Dan Wilson <danjwilson@gmail.com>
David Baumgold <david@davidbaumgold.com>
David Davis <daviddavis@users.noreply.github.com>
Denis Podlesniy <Haos616@Gmail.com>
Denis Shirokov <dan@rexuni.com>
Fengyuan Chen <cfy1990@gmail.com>
GDR! <gdr@gdr.name>
Geoffrey Bauduin <bauduin.geo@gmail.com>
George Psarakis <giwrgos.psarakis@gmail.com>
Harry Moreno <morenoh149@gmail.com>
Huang Huang <mozillazg101@gmail.com>
Igor Kasianov <super.hang.glider@gmail.com>
JJ <jairojair@gmail.com>
Jackie Leng <Jackie.Leng@nelen-schuurmans.nl>
James M. Allen <james.m.allen@gmail.com>
Javier Martin Montull <javier.martin.montull@cern.ch>
John Arnold <johnar@microsoft.com>
Jon Dufresne <jon.dufresne@gmail.com>
Jozef <knaperek@users.noreply.github.com>
Kevin Gu <guqi@reyagroup.com>
Kxrr <Hi@Kxrr.Us>
Leo Singer <leo.singer@ligo.org>
Mads Jensen <mje@inducks.org>
Manuel Vázquez Acosta <mvaled@users.noreply.github.com>
Marcelo Da Cruz Pinto <Marcelo_DaCruzPinto@McAfee.com>
Marco Schweighauser <marco@mailrelay.ch>
Markus Kaiserswerth <github@sensun.org>
Matt Davis <matteius@gmail.com>
Michael <michael-k@users.noreply.github.com>
Michael Peake <michaeljpeake@icloud.com>
Mikołaj <mikolevy1@gmail.com>
Misha Wolfson <myw@users.noreply.github.com>
Nick Eaket <4418194+neaket360pi@users.noreply.github.com>
Nicolas Mota <nicolas_mota@live.com>
Nicholas Pilon <npilon@gmail.com>
Omer Katz <omer.drow@gmail.com>
Patrick Cloke <clokep@users.noreply.github.com>
Patrick Zhang <patdujour@gmail.com>
Paulo <PauloPeres@users.noreply.github.com>
Rachel Johnson <racheljohnson457@gmail.com>
Raphaël Riel <raphael.riel@gmail.com>
Russell Keith-Magee <russell@keith-magee.com>
Ryan Guest <ryanguest@gmail.com>
Ryan P Kilby <rpkilby@ncsu.edu>
Régis B <github@behmo.com>
Sammie S. Taunton <diemuzi@gmail.com>
Samuel Dion-Girardeau <samueldg@users.noreply.github.com>
Scott Cooper <scttcper@gmail.com>
Sergi Almacellas Abellana <sergi@koolpi.com>
Sergio Fernandez <ElAutoestopista@users.noreply.github.com>
Shitikanth <golu3990@gmail.com>
Theodore Dubois <tbodt@users.noreply.github.com>
Thijs Triemstra <info@collab.nl>
Tobias Kunze <rixx@cutebit.de>
Vincent Barbaresi <vbarbaresi@users.noreply.github.com>
Vinod Chandru <vinod.chandru@gmail.com>
Wido den Hollander <wido@widodh.nl>
Xavier Hardy <xavierhardy@users.noreply.github.com>
anentropic <ego@anentropic.com>
arpanshah29 <ashah29@stanford.edu>
dmollerm <d.moller.m@gmail.com>
hclihn <23141651+hclihn@users.noreply.github.com>
jess <jessachandler@gmail.com>
lead2gold <caronc@users.noreply.github.com>
mariia-zelenova <32500603+mariia-zelenova@users.noreply.github.com>
martialp <martialp@users.noreply.github.com>
mperice <mperice@users.noreply.github.com>
pachewise <pachewise@users.noreply.github.com>
partizan <serg.partizan@gmail.com>
y0ngdi <36658095+y0ngdi@users.noreply.github.com>

.. note::

    This wall was automatically generated from git history,
    so sadly it doesn't not include the people who help with more important
    things like answering mailing-list questions.


.. _v420-important:

Important Notes
===============

Supported Python Versions
-------------------------

The supported Python Versions are:

- CPython 2.7
- CPython 3.4
- CPython 3.5
- CPython 3.6
- PyPy 5.8 (``pypy2``)

.. _v420-news:

News
====

Result Backends
---------------

New Redis Sentinel Results Backend
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Redis Sentinel provides high availability for Redis.
A new result backend supporting it was added.

Cassandra Results Backend
~~~~~~~~~~~~~~~~~~~~~~~~~

A new `cassandra_options` configuration option was introduced in order to configure
the cassandra client.

See :ref:`conf-cassandra-result-backend` for more information.

DynamoDB Results Backend
~~~~~~~~~~~~~~~~~~~~~~~~

A new `dynamodb_endpoint_url` configuration option was introduced in order
to point the result backend to a local endpoint during development or testing.

See :ref:`conf-dynamodb-result-backend` for more information.

Python 2/3 Compatibility Fixes
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Both the CouchDB and the Consul result backends accepted byte strings without decoding them to Unicode first.
This is now no longer the case.

Canvas
------

Multiple bugs were resolved resulting in a much smoother experience when using Canvas.

Tasks
-----

Bound Tasks as Error Callbacks
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We fixed a regression that occured when bound tasks are used as error callbacks.
This used to work in Celery 3.x but raised an exception in 4.x until this release.

In both 4.0 and 4.1 the following code wouldn't work:

.. code-block:: python

  @app.task(name="raise_exception", bind=True)
  def raise_exception(self):
      raise Exception("Bad things happened")


  @app.task(name="handle_task_exception", bind=True)
  def handle_task_exception(self):
      print("Exception detected")

  subtask = raise_exception.subtask()

  subtask.apply_async(link_error=handle_task_exception.s())

Task Representation
~~~~~~~~~~~~~~~~~~~

- Shadowing task names now works as expected.
  The shadowed name is properly presented in flower, the logs and the traces.
- `argsrepr` and `kwargsrepr` were previously not used even if specified.
  They now work as expected. See :ref:`task-hiding-sensitive-information` for more information.

Custom Requests
~~~~~~~~~~~~~~~

We now allow tasks to use custom `request <celery.worker.request.Request>`:class: classes
for custom task classes.

See :ref:`task-requests-and-custom-requests` for more information.

Retries with Exponential Backoff
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Retries can now be performed with exponential backoffs to avoid overwhelming
external services with requests.

See :ref:`task-autoretry` for more information.

Sphinx Extension
----------------

Tasks were supposed to be automatically documented when using Sphinx's Autodoc was used.
The code that would have allowed automatic documentation had a few bugs which are now fixed.

Also, The extension is now documented properly. See :ref:`sphinx` for more information.
