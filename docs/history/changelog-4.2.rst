.. _changelog-4.2:

================
 Change history
================

This document contains change notes for bugfix releases in
the 4.x series, please see :ref:`whatsnew-4.2` for
an overview of what's new in Celery 4.2.

4.2.1
=====
:release-date: 2018-07-18 11:00 AM IST
:release-by: Omer Katz

- **Result Backend**: Fix deserialization of exceptions that are present in the producer codebase but not in the consumer codebase.

  Contributed by **John Arnold**

- **Message Protocol Compatibility**: Fix error caused by an invalid (None) timelimit value in the message headers when migrating messages from 3.x to 4.x.

  Contributed by **Robert Kopaczewski**

- **Result Backend**: Fix serialization of exception arguments when exception arguments are not JSON serializable by default.

  Contributed by **Tom Booth**

- **Worker**: Fixed multiple issues with rate limited tasks

  Maintain scheduling order.
  Fix possible scheduling of a :class:`celery.worker.request.Request` with the wrong :class:`kombu.utils.limits.TokenBucket` which could cause tasks' rate limit to behave incorrectly.
  Fix possible duplicated execution of tasks that were rate limited or if ETA/Countdown was provided for them.

  Contributed by :github_user:`ideascf`

- **Worker**: Defensively handle invalid timelimit header values in requests.

  Contributed by **Omer Katz**

Documentation fixes:


    - **Matt Wiens**
    - **Seunghun Lee**
    - **Lewis M. Kabui**
    - **Prathamesh Salunkhe**

4.2.0
=====
:release-date: 2018-06-10 21:30 PM IST
:release-by: Omer Katz

- **Task**: Add ``ignore_result`` as task execution option (#4709, #3834)

    Contributed by **Andrii Kostenko** and **George Psarakis**.

- **Redis Result Backend**: Do not create PubSub subscriptions when results are ignored (#4709, #3834)

    Contributed by **Andrii Kostenko** and **George Psarakis**.

- **Redis Result Backend**: Result consumer always unsubscribes when task state is ready (#4666)

    Contributed by **George Psarakis**.

- **Development/Testing**: Add docker-compose and base Dockerfile for development (#4482)

    Contributed by **Chris Mitchell**.

- **Documentation/Sphinx**: Teach autodoc to document tasks if undoc-members is not set (#4588)

    Contributed by **Leo Singer**.

- **Documentation/Sphinx**: Put back undoc-members option in sphinx test (#4586)

    Contributed by **Leo Singer**.

- **Documentation/Sphinx**: Sphinx autodoc picks up tasks automatically only if `undoc-members` is set (#4584)

    Contributed by **Leo Singer**.

- **Task**: Fix shadow_name issue when using previous version Task class (#4572)

    Contributed by :github_user:`pachewise`.

- **Task**: Add support for bound tasks as `link_error` parameter (Fixes #3723) (#4545)

    Contributed by :github_user:`brabiega`.

- **Deployment**: Add a command line option for setting the Result Backend URL (#4549)

    Contributed by :github_user:`y0ngdi`.

- **CI**: Enable pip cache in appveyor build (#4546)

    Contributed by **Thijs Triemstra**.

- **Concurrency/Asynpool**: Fix errno property name shadowing.

    Contributed by **Omer Katz**.

- **DynamoDB Backend**: Configurable endpoint URL (#4532)

    Contributed by **Bohdan Rybak**.

- **Timezones**: Correctly detect UTC timezone and timezone from settings (Fixes #4517) (#4519)

    Contributed by :github_user:`last-partizan`.

- **Control**: Cleanup the mailbox's producer pool after forking (#4472)

    Contributed by **Nick Eaket**.

- **Documentation**: Start Celery and Celery Beat on Azure WebJob (#4484)

    Contributed by **PauloPeres**.

- **Celery Beat**: Schedule due tasks on startup, after Beat restart has occurred (#4493)

    Contributed by **Igor Kasianov**.

- **Worker**: Use absolute time when task is accepted by worker pool (#3684)

    Contributed by **Régis Behmo**.

- **Canvas**: Propagate arguments to chains inside groups (#4481)

    Contributed by **Chris Mitchell**.

- **Canvas**: Fix `Task.replace` behavior in nested chords (fixes #4368) (#4369)

    Contributed by **Denis Shirokov** & **Alex Hill**.

- **Installation**: Pass python_requires argument to setuptools (#4479)

    Contributed by **Jon Dufresne**.

- **Message Protocol Compatibility**: Handle "hybrid" messages that have moved between Celery versions (#4358) (Issue #4356)

    Contributed by **Russell Keith-Magee**.

- **Canvas**: request on_timeout now ignores soft time limit exception (fixes #4412) (#4473)

    Contributed by **Alex Garel**.

- **Redis Result Backend**: Integration test to verify PubSub unsubscriptions (#4468)

    Contributed by **George Psarakis**.

- **Message Protocol Properties**: Allow the shadow keyword argument and the shadow_name method to set shadow properly (#4381)

    Contributed by :github_user:`hclihn`.

- **Canvas**: Run chord_unlock on same queue as chord body (#4448) (Issue #4337)

    Contributed by **Alex Hill**.

- **Canvas**: Support chords with empty header group (#4443)

    Contributed by **Alex Hill**.

- **Timezones**: make astimezone call in localize more safe (#4324)

    Contributed by **Matt Davis**.

- **Canvas**: Fix length-1 and nested chords (#4437) (Issues #4393, #4055, #3885, #3597, #3574, #3323, #4301)

    Contributed by **Alex Hill**.

- **CI**: Run `Openstack Bandit <https://pypi.org/project/bandit/1.0.1/>`_ in Travis CI in order to detect security issues.

    Contributed by **Omer Katz**.

- **CI**: Run `isort <https://github.com/timothycrosley/isort>`_ in Travis CI in order to lint Python **import** statements.

    Contributed by **Omer Katz**.

- **Canvas**: Resolve TypeError on `.get` from nested groups (#4432) (Issue #4274)

    Contributed by **Misha Wolfson**.

- **CouchDB Backend**: Correct CouchDB key string type for Python 2/3 compatibility (#4166)

    Contributed by :github_user:`fmind` && **Omer Katz**.

- **Group Result**: Fix current_app fallback in GroupResult.restore() (#4431)

    Contributed by **Alex Hill**.

- **Consul Backend**: Correct key string type for Python 2/3 compatibility (#4416)

    Contributed by **Wido den Hollander**.

- **Group Result**: Correctly restore an empty GroupResult (#2202) (#4427)

    Contributed by **Alex Hill** & **Omer Katz**.

- **Result**: Disable synchronous waiting for sub-tasks on eager mode(#4322)

    Contributed by **Denis Podlesniy**.

- **Celery Beat**: Detect timezone or Daylight Saving Time changes (#1604) (#4403)

    Contributed by **Vincent Barbaresi**.

- **Canvas**: Fix append to an empty chain. Fixes #4047. (#4402)

    Contributed by **Omer Katz**.

- **Task**: Allow shadow to override task name in trace and logging messages. (#4379)

    Contributed by :github_user:`hclihn`.

- **Documentation/Sphinx**: Fix getfullargspec Python 2.x compatibility in contrib/sphinx.py (#4399)

    Contributed by **Javier Martin Montull**.

- **Documentation**: Updated installation instructions for SQS broker (#4382)

    Contributed by **Sergio Fernandez**.

- **Celery Beat**: Better equality comparison for ScheduleEntry instances (#4312)

    Contributed by :github_user:`mariia-zelenova`.

- **Task**: Adding 'shadow' property to as_task_v2 (#4350)

    Contributed by **Marcelo Da Cruz Pinto**.

- Try to import directly, do not use deprecated imp method (#4216)

    Contributed by **Tobias Kunze**.

- **Task**: Enable `kwargsrepr` and `argsrepr` override for modifying task argument representation (#4260)

    Contributed by **James M. Allen**.

- **Result Backend**: Add Redis Sentinel backend (#4144)

    Contributed by **Geoffrey Bauduin**.

- Use unique time values for Collections/LimitedSet (#3879 and #3891) (#3892)

    Contributed by :github_user:`lead2gold`.

- **CI**: Report coverage for all result backends.

    Contributed by **Omer Katz**.

- **Django**: Use Django DB max age connection setting (fixes #4116) (#4292)

    Contributed by **Marco Schweighauser**.

- **Canvas**: Properly take into account chain tasks link_error (#4240)

    Contributed by :github_user:`agladkov`.

- **Canvas**: Allow to create group with single task (fixes issue #4255) (#4280)

    Contributed by :github_user:`agladkov`.

- **Canvas**: Copy dictionary parameter in chord.from_dict before modifying (fixes issue #4223) (#4278)

    Contributed by :github_user:`agladkov`.

- **Results Backend**: Add Cassandra options (#4224)

    Contributed by **Scott Cooper**.

- **Worker**: Apply rate limiting for tasks with ETA (#4251)

    Contributed by :github_user:`arpanshah29`.

- **Celery Beat**: support scheduler entries without a schedule (#4235)

    Contributed by **Markus Kaiserswerth**.

- **SQS Broker**: Updated SQS requirements file with correct boto3 version (#4231)

    Contributed by **Alejandro Varas**.

- Remove unused code from _create_app contextmanager (#4204)

    Contributed by **Ryan P Kilby**.

- **Group Result**: Modify GroupResult.as_tuple() to include parent (fixes #4106) (#4205)

    Contributed by :github_user:`pachewise`.

- **Beat**: Set default scheduler class in beat command. (#4189)

    Contributed by :github_user:`Kxrr`.

- **Worker**: Retry signal receiver after raised exception (#4192)

    Contributed by **David Davis**.

- **Task**: Allow custom Request class for tasks (#3977)

    Contributed by **Manuel Vázquez Acosta**.

- **Django**: Django fixup should close all cache backends (#4187)

    Contributed by **Raphaël Riel**.

- **Deployment**: Adds stopasgroup to the supervisor scripts (#4200)

    Contributed by :github_user:`martialp`.

- Using Exception.args to serialize/deserialize exceptions (#4085)

    Contributed by **Alexander Ovechkin**.

- **Timezones**: Correct calculation of application current time with timezone (#4173)

    Contributed by **George Psarakis**.

- **Remote Debugger**: Set the SO_REUSEADDR option on the socket (#3969)

    Contributed by **Theodore Dubois**.

- **Django**: Celery ignores exceptions raised during `django.setup()` (#4146)

    Contributed by **Kevin Gu**.

- Use heartbeat setting from application configuration for Broker connection (#4148)

    Contributed by :github_user:`mperice`.

- **Celery Beat**: Fixed exception caused by next_transit receiving an unexpected argument. (#4103)

    Contributed by **DDevine**.

- **Task** Introduce exponential backoff with Task auto-retry (#4101)

    Contributed by **David Baumgold**.

- **AsyncResult**: Remove weak-references to bound methods in AsyncResult promises. (#4131)

    Contributed by **Vinod Chandru**.

- **Development/Testing**: Allow eager application of canvas structures (#4576)

    Contributed by **Nicholas Pilon**.

- **Command Line**: Flush stderr before exiting with error code 1.

    Contributed by **Antonin Delpeuch**.

- **Task**: Escapes single quotes in kwargsrepr strings.

    Contributed by **Kareem Zidane**

- **AsyncResult**: Restore ability to join over ResultSet after fixing celery/#3818.

    Contributed by **Derek Harland**

- **Redis Results Backend**: Unsubscribe on message success.

  Previously Celery would leak channels, filling the memory of the Redis instance.

  Contributed by **George Psarakis**

- **Task**: Only convert eta to isoformat when it is not already a string.

  Contributed by **Omer Katz**

- **Redis Results Backend**: The result_backend setting now supports rediss:// URIs

  Contributed by **James Remeika**

- **Canvas** Keyword arguments are passed to tasks in chain as expected.

  Contributed by :github_user:`tothegump`

- **Django** Fix a regression causing Celery to crash when using Django.

  Contributed by **Jonas Haag**

- **Canvas** Chain with one task now runs as expected.

  Contributed by :github_user:`tothegump`

- **Kombu** Celery 4.2 now requires Kombu 4.2 or better.

  Contributed by **Omer Katz & Asif Saifuddin Auvi**

- `GreenletExit` is not in `__all__` in greenlet.py which can not be imported by Python 3.6.

  The import was adjusted to work on Python 3.6 as well.

  Contributed by **Hsiaoming Yang**

- Fixed a regression that occurred during the development of Celery 4.2 which caused `celery report` to crash when Django is installed.

  Contributed by **Josue Balandrano Coronel**

- Matched the behavior of `GroupResult.as_tuple()` to that of `AsyncResult.as_tuple()`.

  The group's parent is now serialized correctly.

  Contributed by **Josue Balandrano Coronel**

- Use Redis coercion mechanism for converting URI query parameters.

  Contributed by **Justin Patrin**

- Fixed the representation of `GroupResult`.

  The dependency graph is now presented correctly.

  Contributed by **Josue Balandrano Coronel**

Documentation, CI, Installation and Tests fixes:


    - **Sammie S. Taunton**
    - **Dan Wilson**
    - :github_user:`pachewise`
    - **Sergi Almacellas Abellana**
    - **Omer Katz**
    - **Alex Zaitsev**
    - **Leo Singer**
    - **Rachel Johnson**
    - **Jon Dufresne**
    - **Samuel Dion-Girardeau**
    - **Ryan Guest**
    - **Huang Huang**
    - **Geoffrey Bauduin**
    - **Andrew Wong**
    - **Mads Jensen**
    - **Jackie Leng**
    - **Harry Moreno**
    - :github_user:`michael-k`
    - **Nicolas Mota**
    - **Armenak Baburyan**
    - **Patrick Zhang**
    - :github_user:`anentropic`
    - :github_user:`jairojair`
    - **Ben Welsh**
    - **Michael Peake**
    - **Fengyuan Chen**
    - :github_user:`arpanshah29`
    - **Xavier Hardy**
    - **Shitikanth**
    - **Igor Kasianov**
    - **John Arnold**
    - :github_user:`dmollerm`
    - **Robert Knight**
    - **Asif Saifuddin Auvi**
    - **Eduardo Ramírez**
    - **Kamil Breguła**
    - **Juan Gutierrez**
