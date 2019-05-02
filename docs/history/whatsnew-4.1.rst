.. _whatsnew-4.1:

===========================================
 What's new in Celery 4.1 (latentcall)
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

The 4.1.0 release continues to improve our efforts to provide you with
the best task execution platform for Python.

This release is mainly a bug fix release, ironing out some issues and regressions
found in Celery 4.0.0.

We added official support for Python 3.6 and PyPy 5.8.0.

This is the first time we release without Ask Solem as an active contributor.
We'd like to thank him for his hard work in creating and maintaining Celery over the years.

Since Ask Solem was not involved there were a few kinks in the release process
which we promise to resolve in the next release.
This document was missing when we did release Celery 4.1.0.
Also, we did not update the release codename as we should have.
We apologize for the inconvenience.

For the time being, I, Omer Katz will be the release manager.

Thank you for your support!

*— Omer Katz*

Wall of Contributors
--------------------

Acey <huiwang.e@gmail.com>
Acey9 <huiwang.e@gmail.com>
Alan Hamlett <alanhamlett@users.noreply.github.com>
Alan Justino da Silva <alan.justino@yahoo.com.br>
Alejandro Pernin <ale.pernin@gmail.com>
Alli <alzeih@users.noreply.github.com>
Andreas Pelme <andreas@pelme.se>
Andrew de Quincey <adq@lidskialf.net>
Anthony Lukach <anthonylukach@gmail.com>
Arcadiy Ivanov <arcadiy@ivanov.biz>
Arnaud Rocher <cailloumajor@users.noreply.github.com>
Arthur Vigil <ahvigil@mail.sfsu.edu>
Asif Saifuddin Auvi <auvipy@users.noreply.github.com>
Ask Solem <ask@celeryproject.org>
BLAGA Razvan-Paul <razvan.paul.blaga@gmail.com>
Brendan MacDonell <macdonellba@gmail.com>
Brian Luan <jznight@gmail.com>
Brian May <brian@linuxpenguins.xyz>
Bruno Alla <browniebroke@users.noreply.github.com>
Chris Kuehl <chris@techxonline.net>
Christian <github@penpal4u.net>
Christopher Hoskin <mans0954@users.noreply.github.com>
Daniel Hahler <github@thequod.de>
Daniel Huang <dxhuang@gmail.com>
Derek Harland <donkopotamus@users.noreply.github.com>
Dmytro Petruk <bavaria95@gmail.com>
Ed Morley <edmorley@users.noreply.github.com>
Eric Poelke <epoelke@gmail.com>
Felipe <fcoelho@users.noreply.github.com>
François Voron <fvoron@gmail.com>
GDR! <gdr@gdr.name>
George Psarakis <giwrgos.psarakis@gmail.com>
J Alan Brogan <jalanb@users.noreply.github.com>
James Michael DuPont <JamesMikeDuPont@gmail.com>
Jamie Alessio <jamie@stoic.net>
Javier Domingo Cansino <javierdo1@gmail.com>
Jay McGrath <jaymcgrath@users.noreply.github.com>
Jian Yu <askingyj@gmail.com>
Joey Wilhelm <tarkatronic@gmail.com>
Jon Dufresne <jon.dufresne@gmail.com>
Kalle Bronsen <bronsen@nrrd.de>
Kirill Romanov <djaler1@gmail.com>
Laurent Peuch <cortex@worlddomination.be>
Luke Plant <L.Plant.98@cantab.net>
Marat Sharafutdinov <decaz89@gmail.com>
Marc Gibbons <marc_gibbons@rogers.com>
Marc Hörsken <mback2k@users.noreply.github.com>
Michael <michael-k@users.noreply.github.com>
Michael Howitz <mh@gocept.com>
Michal Kuffa <beezz@users.noreply.github.com>
Mike Chen <yi.chen.it@gmail.com>
Mike Helmick <michaelhelmick@users.noreply.github.com>
Morgan Doocy <morgan@doocy.net>
Moussa Taifi <moutai10@gmail.com>
Omer Katz <omer.drow@gmail.com>
Patrick Cloke <clokep@users.noreply.github.com>
Peter Bittner <django@bittner.it>
Preston Moore <prestonkmoore@gmail.com>
Primož Kerin <kerin.primoz@gmail.com>
Pysaoke <pysaoke@gmail.com>
Rick Wargo <rickwargo@users.noreply.github.com>
Rico Moorman <rico.moorman@gmail.com>
Roman Sichny <roman@sichnyi.com>
Ross Patterson <me@rpatterson.net>
Ryan Hiebert <ryan@ryanhiebert.com>
Rémi Marenco <remi.marenco@gmail.com>
Salvatore Rinchiera <srinchiera@college.harvard.edu>
Samuel Dion-Girardeau <samuel.diongirardeau@gmail.com>
Sergey Fursov <GeyseR85@gmail.com>
Simon Legner <Simon.Legner@gmail.com>
Simon Schmidt <schmidt.simon@gmail.com>
Slam <3lnc.slam@gmail.com>
Static <staticfox@staticfox.net>
Steffen Allner <sa@gocept.com>
Steven <rh0dium@users.noreply.github.com>
Steven Johns <duoi@users.noreply.github.com>
Tamer Sherif <tamer.sherif@flyingelephantlab.com>
Tao Qingyun <845767657@qq.com>
Tayfun Sen <totayfun@gmail.com>
Taylor C. Richberger <taywee@gmx.com>
Thierry RAMORASOAVINA <thierry.ramorasoavina@orange.com>
Tom 'Biwaa' Riat <riat.tom@gmail.com>
Viktor Holmqvist <viktorholmqvist@gmail.com>
Viraj <vnavkal0@gmail.com>
Vivek Anand <vivekanand1101@users.noreply.github.com>
Will <paradox41@users.noreply.github.com>
Wojciech Żywno <w.zywno@gmail.com>
Yoichi NAKAYAMA <yoichi.nakayama@gmail.com>
YuLun Shih <shih@yulun.me>
Yuhannaa <yuhannaa@gmail.com>
abhinav nilaratna <anilaratna2@bloomberg.net>
aydin <adigeaydin@gmail.com>
csfeathers <csfeathers@users.noreply.github.com>
georgepsarakis <giwrgos.psarakis@gmail.com>
orf <tom@tomforb.es>
shalev67 <shalev67@gmail.com>
sww <sww@users.noreply.github.com>
tnir <tnir@users.noreply.github.com>
何翔宇(Sean Ho) <h1x2y3awalm@gmail.com>

.. note::

    This wall was automatically generated from git history,
    so sadly it doesn't not include the people who help with more important
    things like answering mailing-list questions.


.. _v410-important:

Important Notes
===============

Added support for Python 3.6 & PyPy 5.8.0
-----------------------------------------

We now run our unit test suite and integration test suite on Python 3.6.x
and PyPy 5.8.0.

We expect newer versions of PyPy to work but unfortunately we do not have the
resources to test PyPy with those versions.

The supported Python Versions are:

- CPython 2.7
- CPython 3.4
- CPython 3.5
- CPython 3.6
- PyPy 5.8 (``pypy2``)

.. _v410-news:

News
====

Result Backends
---------------

New DynamoDB Results Backend
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

We added a new results backend for those of you who are using DynamoDB.

If you are interested in using this results backend, refer to :ref:`conf-dynamodb-result-backend` for more information.

Elasticsearch
~~~~~~~~~~~~~

The Elasticsearch results backend is now more robust and configurable.

See :ref:`conf-elasticsearch-result-backend` for more information
about the new configuration options.

Redis
~~~~~

The Redis results backend can now use TLS to encrypt the communication with the
Redis database server.

See :ref:`conf-redis-result-backend`.

MongoDB
~~~~~~~

The MongoDB results backend can now handle binary-encoded task results.

This was a regression from 4.0.0 which resulted in a problem using serializers
such as MsgPack or Pickle in conjunction with the MongoDB results backend.

Periodic Tasks
--------------

The task schedule now updates automatically when new tasks are added.
Now if you use the Django database scheduler, you can add and remove tasks from the schedule without restarting Celery beat.

Tasks
-----

The ``disable_sync_subtasks`` argument was added to allow users to override disabling
synchronous subtasks.

See :ref:`task-synchronous-subtasks`

Canvas
------

Multiple bugs were resolved resulting in a much smoother experience when using Canvas.
