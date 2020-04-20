.. _changelog-4.1:

================
 Change history
================

This document contains change notes for bugfix releases in
the 4.1.x series, please see :ref:`whatsnew-4.2` for
an overview of what's new in Celery 4.2.

.. _version-4.1.1:

4.1.1
=====
:release-date: 2018-05-21 12:48 PM PST
:release-by: Omer Katz

.. important::

	Please upgrade as soon as possible or pin Kombu to 4.1.0.

- **Breaking Change**: The module `async` in Kombu changed to `asynchronous`.

Contributed by **Omer Katz & Asif Saifuddin Auvi**

.. _version-4.1.0:

4.1.0
=====
:release-date: 2017-07-25 00:00 PM PST
:release-by: Omer Katz


- **Configuration**: CELERY_SEND_EVENTS instead of CELERYD_SEND_EVENTS for 3.1.x compatibility (#3997)

 Contributed by **abhinav nilaratna**.

- **App**: Restore behavior so Broadcast queues work. (#3934)

 Contributed by **Patrick Cloke**.

- **Sphinx**: Make appstr use standard format (#4134) (#4139)

 Contributed by **Preston Moore**.

- **App**: Make id, name always accessible from logging.Formatter via extra (#3994)

 Contributed by **Yoichi NAKAYAMA**.

- **Worker**: Add worker_shutting_down signal (#3998)

 Contributed by **Daniel Huang**.

- **PyPy**: Support PyPy version 5.8.0 (#4128)

 Contributed by **Omer Katz**.

- **Results**: Elasticsearch: Fix serializing keys (#3924)

 Contributed by :github_user:`staticfox`.

- **Canvas**: Deserialize all tasks in a chain (#4015)

 Contributed by :github_user:`fcoelho`.

- **Systemd**: Recover loglevel for ExecStart in systemd config (#4023)

 Contributed by **Yoichi NAKAYAMA**.

- **Sphinx**: Use the Sphinx add_directive_to_domain API. (#4037)

 Contributed by **Patrick Cloke**.

- **App**: Pass properties to before_task_publish signal (#4035)

 Contributed by **Javier Domingo Cansino**.

- **Results**: Add SSL option for Redis backends (#3831)

 Contributed by **Chris Kuehl**.

- **Beat**: celery.schedule.crontab: fix reduce (#3826) (#3827)

 Contributed by **Taylor C. Richberger**.

- **State**: Fix celery issues when using flower REST API

 Contributed by **Thierry RAMORASOAVINA**.

- **Results**: Elasticsearch: Fix serializing document id.

 Contributed by **Acey9**.

- **Beat**: Make shallow copy of schedules dictionary

 Contributed by **Brian May**.

- **Beat**: Populate heap when periodic tasks are changed

 Contributed by **Wojciech Żywno**.

- **Task**: Allow class methods to define tasks (#3952)

 Contributed by **georgepsarakis**.

- **Platforms**: Always return boolean value when checking if signal is supported (#3962).

 Contributed by **Jian Yu**.

- **Canvas**: Avoid duplicating chains in chords (#3779)

 Contributed by **Ryan Hiebert**.

- **Canvas**: Lookup task only if list has items (#3847)

 Contributed by **Marc Gibbons**.

- **Results**: Allow unicode message for exception raised in task (#3903)

 Contributed by **George Psarakis**.

- **Python3**: Support for Python 3.6 (#3904, #3903, #3736)

 Contributed by **Jon Dufresne**, **George Psarakis**, **Asif Saifuddin Auvi**, **Omer Katz**.

- **App**: Fix retried tasks with expirations (#3790)

 Contributed by **Brendan MacDonell**.

- * Fixes items format route in docs (#3875)

 Contributed by **Slam**.

- **Utils**: Fix maybe_make_aware (#3850)

 Contributed by **Taylor C. Richberger**.

- **Task**: Fix task ETA issues when timezone is defined in configuration (#3867)

 Contributed by **George Psarakis**.

- **Concurrency**: Consumer does not shutdown properly when embedded in gevent application (#3746)

 Contributed by **Arcadiy Ivanov**.

- **Canvas**: Fix #3725: Task replaced with group does not complete (#3731)

 Contributed by **Morgan Doocy**.

- **Task**: Correct order in chains with replaced tasks (#3730)

 Contributed by **Morgan Doocy**.

- **Result**: Enable synchronous execution of sub-tasks (#3696)

 Contributed by **shalev67**.

- **Task**: Fix request context for blocking task apply (added hostname) (#3716)

 Contributed by **Marat Sharafutdinov**.

- **Utils**: Fix task argument handling (#3678) (#3693)

 Contributed by **Roman Sichny**.

- **Beat**: Provide a transparent method to update the Scheduler heap (#3721)

 Contributed by **Alejandro Pernin**.

- **Beat**: Specify default value for pidfile option of celery beat. (#3722)

 Contributed by **Arnaud Rocher**.

- **Results**: Elasticsearch: Stop generating a new field every time when a new result is being put (#3708)

 Contributed by **Mike Chen**.

- **Requirements**

    - Now depends on :ref:`Kombu 4.1.0 <kombu:version-4.1.0>`.

- **Results**: Elasticsearch now reuses fields when new results are added.

    Contributed by **Mike Chen**.

- **Results**: Fixed MongoDB integration when using binary encodings
  (Issue #3575).

    Contributed by **Andrew de Quincey**.

- **Worker**: Making missing ``*args`` and ``**kwargs`` in Task protocol 1
  return empty value in protocol 2 (Issue #3687).

    Contributed by **Roman Sichny**.

- **App**: Fixed :exc:`TypeError` in AMQP when using deprecated signal
  (Issue #3707).

    Contributed by :github_user:`michael-k`.

- **Beat**: Added a transparent method to update the scheduler heap.

    Contributed by **Alejandro Pernin**.

- **Task**: Fixed handling of tasks with keyword arguments on Python 3
  (Issue #3657).

    Contributed by **Roman Sichny**.

- **Task**: Fixed request context for blocking task apply by adding missing
  hostname attribute.

    Contributed by **Marat Sharafutdinov**.

- **Task**: Added option to run subtasks synchronously with
  ``disable_sync_subtasks`` argument.

    Contributed by :github_user:`shalev67`.

- **App**: Fixed chaining of replaced tasks (Issue #3726).

    Contributed by **Morgan Doocy**.

- **Canvas**: Fixed bug where replaced tasks with groups were not completing
  (Issue #3725).

    Contributed by **Morgan Doocy**.

- **Worker**: Fixed problem where consumer does not shutdown properly when
  embedded in a gevent application (Issue #3745).

    Contributed by **Arcadiy Ivanov**.

- **Results**: Added support for using AWS DynamoDB as a result backend (#3736).

    Contributed by **George Psarakis**.

- **Testing**: Added caching on pip installs.

    Contributed by :github_user:`orf`.

- **Worker**: Prevent consuming queue before ready on startup (Issue #3620).

    Contributed by **Alan Hamlett**.

- **App**: Fixed task ETA issues when timezone is defined in configuration
  (Issue #3753).

    Contributed by **George Psarakis**.

- **Utils**: ``maybe_make_aware`` should not modify datetime when it is
  already timezone-aware (Issue #3849).

    Contributed by **Taylor C. Richberger**.

- **App**: Fixed retrying tasks with expirations (Issue #3734).

    Contributed by **Brendan MacDonell**.

- **Results**: Allow unicode message for exceptions raised in task
  (Issue #3858).

    Contributed by :github_user:`staticfox`.

- **Canvas**: Fixed :exc:`IndexError` raised when chord has an empty header.

    Contributed by **Marc Gibbons**.

- **Canvas**: Avoid duplicating chains in chords (Issue #3771).

    Contributed by **Ryan Hiebert** and **George Psarakis**.

- **Utils**: Allow class methods to define tasks (Issue #3863).

    Contributed by **George Psarakis**.

- **Beat**: Populate heap when periodic tasks are changed.

    Contributed by :github_user:`wzywno` and **Brian May**.

- **Results**: Added support for Elasticsearch backend options settings.

    Contributed by :github_user:`Acey9`.

- **Events**: Ensure ``Task.as_dict()`` works when not all information about
  task is available.

    Contributed by :github_user:`tramora`.

- **Schedules**: Fixed pickled crontab schedules to restore properly (Issue #3826).

    Contributed by **Taylor C. Richberger**.

- **Results**: Added SSL option for redis backends (Issue #3830).

    Contributed by **Chris Kuehl**.

- Documentation and examples improvements by:

    - **Bruno Alla**
    - **Jamie Alessio**
    - **Vivek Anand**
    - **Peter Bittner**
    - **Kalle Bronsen**
    - **Jon Dufresne**
    - **James Michael DuPont**
    - **Sergey Fursov**
    - **Samuel Dion-Girardeau**
    - **Daniel Hahler**
    - **Mike Helmick**
    - **Marc Hörsken**
    - **Christopher Hoskin**
    - **Daniel Huang**
    - **Primož Kerin**
    - **Michal Kuffa**
    - **Simon Legner**
    - **Anthony Lukach**
    - **Ed Morley**
    - **Jay McGrath**
    - **Rico Moorman**
    - **Viraj Navkal**
    - **Ross Patterson**
    - **Dmytro Petruk**
    - **Luke Plant**
    - **Eric Poelke**
    - **Salvatore Rinchiera**
    - **Arnaud Rocher**
    - **Kirill Romanov**
    - **Simon Schmidt**
    - **Tamer Sherif**
    - **YuLun Shih**
    - **Ask Solem**
    - **Tom 'Biwaa' Riat**
    - **Arthur Vigil**
    - **Joey Wilhelm**
    - **Jian Yu**
    - **YuLun Shih**
    - **Arthur Vigil**
    - **Joey Wilhelm**
    - :github_user:`baixuexue123`
    - :github_user:`bronsen`
    - :github_user:`michael-k`
    - :github_user:`orf`
    - :github_user:`3lnc`
