Change history
================

This document contains change notes for bugfix & new features
in the main branch & 5.7.x series, please see :ref:`whatsnew-5.7` for
an overview of what's new in Celery v5.7.0.


.. _version-5.7.0:

5.7.0
======

:release-date: 2026-11-05
:release-by: Asif Saif Uddin

Celery v5.7.0 Stable Release is now available for testing and deploy in production.
Please help us test this version and report any issues.


What's Changed Since 5.7.0rc1
==============================

This section summarizes the changes made since the Celery 5.7.0rc1 pre-release.

Canvas, Chains, Groups, and Chords
----------------------------------

* Run a replaced task's callbacks once when it is replaced with a chain (`:ghpull:`10927`).
* Fix chord link and link-error options so they are passed to the body only (`:ghpull:`10778`).
* Fix stamped header revocation (`:ghpull:`10884`).
* Count each chord header task only once in the Redis backend (`:ghpull:`10919`).
* Fix Redis chord claim cleanup ownership (`:ghpull:`10998`).
* Fail the chord when ``chord_unlock`` retry is refused by the broker (`:ghpull:`10959`).

Task Execution and Result Handling
----------------------------------

* Fix retry argument conflicts when switching calling conventions (`:ghpull:`10895`).
* Pass values to backend hooks by position again (`:ghpull:`10918`).
* Don't mark a task ``REVOKED`` while another worker is running it (`:ghpull:`10922`).
* Keep the first ``SUCCESS`` in the database, MongoDB, and Cassandra backends (`:ghpull:`10920`, :ghpull:`10944`, :ghpull:`10945`).
* Don't record ``FAILURE`` when ``on_success`` raises after ``SUCCESS`` was stored (`:ghpull:`10759`).
* Don't fail a task whose ``SUCCESS`` was stored before the worker was lost (`:ghpull:`10752`).
* Fix a worker hang in ``WorkerLostError`` when ``result_backend_always_retry=True`` (`:ghpull:`10960`).
* Fix ``send_task()`` overriding ``ignore_result`` defaults (`:ghpull:`10953`).
* Preserve ``ignore_result`` overrides across task retries (`:ghpull:`10958`).
* Make ``Task.apply()`` respect the ``task.ignore_result`` default (`:ghpull:`11001`).
* Don't store ``RETRY`` for an ignored result when retrying after a lost worker (`:ghpull:`11009`).
* Track every ``AsyncResult`` waiting on a task ID (`:ghpull:`10956`).
* Allow ``ResultSet.add()`` after the set's results are ready (`:ghpull:`10930`).
* Use the documented retry defaults in the database backend (`:ghpull:`10921`).
* Fix Manager warning output during retries (`:ghpull:`10948`).
* Wait between retries when the broker refuses a callback publish (`:ghpull:`10975`).
* Merge signature options in ``map`` and ``starmap`` ``apply_async()`` (`:ghpull:`10787`).
* Stop gating task publishing signals on cached receiver lists (`:ghpull:`10791`).

Worker Operations
-----------------

* Ensure worker shutdown state is set even if the shutdown handler's logging or signal dispatch raises (`:ghpull:`10738`).
* Cover the second call of ``_ensure_after_fork`` in a test (`:ghpull:`10924`).
* Cancel only per-connection timer entries on broker reconnect instead of clearing the worker timer (`:ghpull:`10901`).

Celery Beat and Scheduling
--------------------------

* Keep Celery Beat running when a schedule entry fails, and reject impossible crontab dates (`:ghpull:`10896`).
* Fix solar schedule timing across daylight-saving time changes (`:ghpull:`10915`).
* Enhance timezone localization tests (`:ghpull:`10996`).
* Use Cuba's 2025 fall-back in the midnight DST tests (`:ghpull:`11011`).

Utilities and Data Structures
-----------------------------

* Don't expire or evict re-added ``LimitedSet`` items using their old timestamp (`:ghpull:`10758`).
* Give entries copied by ``LimitedSet.update()`` this set's sequence numbers (`:ghpull:`10943`).
* Keep the closing bracket when ``saferepr`` truncates the last element (`:ghpull:`10771`).
* Fix ``MultiParser`` to honor its configured ``Node`` class (`:ghpull:`10938`).

Application and Configuration
-----------------------------

* Raise a clear error when ``@shared_task`` is applied twice (`:ghpull:`10889`).

Documentation and Developer Experience
--------------------------------------

* Fix an incorrect sentence in the user guide (`:ghpull:`10902`).
* Document the quorum queue delivery limit (`:ghpull:`10926`).
* Improve signature documentation (`:ghpull:`10978`).
* Update the pull request template with AI-spamming guidelines (`:ghpull:`10979`).
* Add guidance for AI agents in the Celery project (`:ghpull:`10999`).

Dependencies and Packaging
--------------------------

* Update ``click-plugins`` requirement to ``>=1.1.1.2`` (`:ghpull:`10899`).
* Update Django requirement to ``>=3.2.1`` (`:ghpull:`10816`).
* Replace ``brotlipy`` with ``brotlicffi`` in the Brotli extra (`:ghpull:`10992`).
* Update Brotli dependencies for PyPy and CPython (`:ghpull:`10994`).
* Update Billiard and click-repl versions in requirements (`:ghpull:`11002`).
* Update ``grpcio`` to ``1.85.1`` (`:ghpull:`11006`).
* Update Kombu to ``5.7.0`` (`:ghpull:`11007`).

Development Dependencies
------------------------

* Update GitHub CodeQL action to ``4.39.0`` and ``4.39.1`` (`:ghpull:`10900`, :ghpull:`10974`).
* Update Pydantic to ``2.14.1`` (`:ghpull:`10903`).
* Update tox to ``4.65.0`` (`:ghpull:`10935`).
* Update mypy to ``2.4.0`` (`:ghpull:`10984`).
* Update ``grpcio`` to ``1.84.1`` (`:ghpull:`10973`).
* Update ``azure-storage-blob`` to ``12.31.1`` (`:ghpull:`11005`).
* Update ``google-cloud-storage`` to ``3.17.1`` (`:ghpull:`11013`).
* Update ``azure-identity`` to ``1.27.0`` (`:ghpull:`11020`).

Testing
-------

* Assert exact task replacement nesting levels (`:ghpull:`10925`).
* Run mock assertions inside ``pytest.raises`` (`:ghpull:`11004`).


.. _version-5.7.0rc1:

5.7.0rc1
========

:release-date: 2026-10-18
:release-by: Asif Saif Uddin

Celery v5.7.0rc1 Release Candidate 1 is now available for testing.
Please help us test this version and report any issues.


Features
~~~~~~~~

- Add ``result_failure_expires`` setting for Redis result backend chord keys (#10826)
- Add ``task_retry_on_worker_lost`` setting to limit requeues after worker loss (#10843)
- Accept Pydantic models when calling tasks with ``pydantic=True`` (#10844)

Canvas
~~~~~~

- Keep eager group application from accessing the result backend (#10750)
- Avoid geometric message growth in standalone chord bodies (#10824)
- Fix empty group and empty chain handling in chains (#10834)
- Build chains in a single pass instead of folding with ``|`` (#10840)
- Fix duplicate nodes in a chain's ``as_tuple()`` after a group (#10829)
- Fix errback arity classification with stored signature arguments (#10838)
- Fix chain/group representations mangling task names and arguments (#10855)
- Prevent nested groups from taking their parent group's ID (#10857)
- Fire chain links only once, on the last task in eager chains (#10864)
- Fix exponential message growth in chains of groups (#10873)

Workers
~~~~~~~

- Fix prefork write generators raising ``StopIteration`` (PEP 479) (#10842)
- Send the worker hostname as the ``worker_process_init`` signal sender (#10849)
- Reset autoscale keepalive for immediate tasks (#10839)
- Avoid calling ``setup_defaults`` twice when creating a worker (#10828)
- Skip exited children in ``AsynPool._help_stuff_finish`` (#10861)
- Pass preload options only to commands that accept them (#10837)
- Fix worker expiration checks during DST fall back (#10868)
- Add source replacement support to ``regen`` during cached iteration (#10823)

Task Publishing and Execution
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

- Fix ``send_task`` ETA and expiration calculations across DST transitions (#10813)
- Fix task execution after ETA conversion failure (#10876)
- Copy only declared stamps into task execution options (#10853)

Results
~~~~~~~

- Fix MongoDB result cleanup across DST transitions (#10852)
- Add ``result_chord_expires`` for Redis chord keys (#10827)
- Keep cache backend chords alive past ``result_expires`` (#10830)
- Avoid calling ``get_state`` for plain chain failures (#10836)
- Fix database backend cleanup timezone handling (#10866)

Beat
~~~~

- Fix relative schedule rounding across DST transitions (#10818)

Security
~~~~~~~~

- Redact Azure credentials in diagnostic output (#10822)

Documentation
~~~~~~~~~~~~~

- Fix ``broker_use_ssl`` setting target being swallowed into the previous paragraph (#10847)
- Fix unresolved mailing-list and IRC links in the contributing guide and introduction (#10848)
- Fix stray RST markup in changelog entries (#10851)

Dependencies
~~~~~~~~~~~~

- Update ``cryptography`` to 50.0.2 (#10815)
- Update the minimum ``boto3`` requirement to 1.43.113 (#10820, #10869)
- Update ``azure-storage-blob`` to 12.31.0 (#10825)
- Update ``azure-identity`` to 1.26.0 (#10863)
- Update ``tox`` to 4.64.9 (#10832)
- Align the minimum ``pymemcache`` version with ``RetryingClient`` support and update it to 4.0.0 (#10831, #10859)
- Update GitHub CodeQL Action to 4.38.3 (#10860)
- Update Kombu to 5.7.0rc1 (#10882)

Other
~~~~~

- Remove unused ``AQLQueryError`` import from the ArangoDB backend (#10878)
- Fix invalid Bandit baseline JSON and validate it before commit (#10766)


.. _version-5.7.0b1:

5.7.0b1
=======

:release-date: 2026-10-06
:release-by: Asif Saif Uddin

Celery v5.7.0b1 Beta 1 is now available for testing.
Please help us test this version and report any issues.


Features
~~~~~~~~

- Add opt-in remote control (inspect ping) support to ``celery beat`` (#10422)
- Add ``pymemcache`` result backend support (#9977)
- Make ``--concurrency=auto`` aware of cgroup CPU limits (#10328)

Results
~~~~~~~

- Fix Couchbase missing-document handling to raise instead of returning
  ``None`` (#10680)
- Preserve ``propagate`` when joining nested native results (#10689)
- Keep ``forget()`` idempotent on Elasticsearch and CouchDB backends (#10697)
- Keep ``forget()`` idempotent on Azure Blob Storage and CosmosDB (#10714)
- Handle missing ArangoDB documents when deleting results (#10701)
- Upsert CosmosDB results so later task states are stored correctly (#10746)
- Publish the Cassandra session only once the connection is ready (#10657)
- Avoid holding the Redis pubsub lock while waiting for messages (#10715)
- Defer re-entrant ``cancel_for`` to avoid Redis pubsub deadlocks (#10716)
- Preserve ``EagerResult.name`` when copying or pickling (#10773)
- Raise ``TaskRevokedError`` from ``EagerResult.get()`` after ``revoke()`` (#10772)
- Finish a half-read child result on prefork shutdown and acknowledge it
  during the join (#10768)

Canvas
~~~~~~

- Apply ``countdown`` and ``eta`` to chord headers (#10735)
- Fail chords after a chord whose header failed (#10767)
- Fix geometric message bloat in chains of implicit chords (#10743)
- Resolve the app of a chain or chord through its tasks (#10749)
- Avoid raising in ``chain.app`` when the first task's canvas contains a
  dict (#10792)

Workers
~~~~~~~

- Flush buffered events with ``hub.call_soon()`` (#10736)
- Synchronize ``EventDispatcher`` shutdown with publishing (#10745)
- Preserve pending operation order in the consumer (#10734)
- Fix consumer reconnect prefetch reduction for green pools (#10804)
- Fix Beat scheduler connection cleanup on shutdown (#10776)
- Fix ``countdown``, expiration, and Beat intervals across DST transitions
  (#10769)
- Limit Django fixup signal handlers to its own app (#10731)
- Tell apart signal receivers bound to different instances (#10699)

Task Publishing
~~~~~~~~~~~~~~~

* Use the merged retry policy for `before_task_publish` and task-sent
  events (#10708)
* Avoid calling `isoformat()` on protocol v1 ETA and expiration strings
  (#10720)

Command Line
~~~~~~~~~~~~

- Apply index lists such as ``-c:1,2`` to named nodes in ``celery multi``
  (#10661)
- Expand ``~`` in ``--workdir`` for spawned workers (#10739)
- Replace deprecated ``codecs.open`` in ``celery upgrade`` settings (#10706)
- Fix escaped percent signs in node formatting (#10663)

Utilities
~~~~~~~~~

- Fix ``BufferMap`` handling of ``bufmaxsize`` and total size accounting
  (#10705)
- Fix regeneration iteration when consumed elsewhere during the loop
  (#10756)
- Stop ``firstmethod()`` from swallowing ``AttributeError`` (#10774)
- Handle ``AttributeError`` in ``iterate_file_descriptors_safely()`` after
  connection loss (#10318)

Docker
~~~~~~

- Fix cache directory ownership in Docker builds (#10707)

Packaging
~~~~~~~~~

- Update the ``billiard`` dependency to 4.3.1 or later (#10802)
- Update the Kombu dependency to 5.7.0b1 (#10809)
- Remove version constraints for Redis requirements (#10810)



.. _version-5.7.0a1:

5.7.0a1
=======

:release-date: 2026-09-22
:release-by: Asif Saif Uddin

Celery v5.7.0 Alpha 1 is now available for testing.
Please help us test this version and report any issues.

Features
~~~~~~~~

- Allow Redis backend SSL config with redis:// URLs (#10313)
- Add ``task_repr_maxlevels`` setting to control argsrepr/kwargsrepr depth (#10371)
- Add ``celery_beat_task`` header in task options (#9702)
- Add runtime to the task-success signal (#9516)
- Add the request context to ``task_failure`` when a worker is lost (#9720)
- Support queue aliases in ``Queues.deselect()`` (#10649)
- Allow ``worker_pool_start_method='spawn'`` with the asynchronous prefork pool (#10584)
- Add ``datefmt`` support to ``ColorFormatter``/``TaskFormatter`` (#10488)
- Enable heartbeat on the event dispatcher connection (#10500)

Bug Fixes
~~~~~~~~~

Canvas / chains / chords / groups
----------------------------------

- Propagate chained task failures to the chord body (#10323); fix chord hang
  when a member is revoked via ``app.control.revoke`` (#10527, reworked at #10564)
- Fix ``Signature.clone()`` sharing kwargs with the original (#10571); fix
  linked-signature option isolation during deepcopy (#10594)
- ``Task.replace()`` with a chain containing a group now returns a result
  instead of hanging (#9604); fix crash with tuple-backed chains (#10626)
- Skip empty groups in (eager) chains (#10321, #10644); fix empty group/chain
  handling in headers (#9772, #10646)
- Preserve group errback mutability (#10385); preserve ``aggregate_duplicates``
  through nested stamp merges

Worker / broker / connection handling
--------------------------------------

- Fix worker consuming unexpected queues, and stalling, after broker
  reconnect (#10335, #10346)
- Fix ``Consumer.on_close()`` wiping in-flight reservations, breaking
  ``worker_disable_prefetch`` (#10345)
- Detect spawned pool children in ``process_initializer`` (#10583); keep a
  cancelled task unacked when a child reports ``SystemExit`` (#10667)
- Fix revoked tasks running anyway after mingle merged revoke stamps from
  another host (#10668)
- Bound already-open broker sockets before connection teardown (#10570)

Results / backends
--------------------

- Make ``result_compression`` actually compress stored results (#10504);
  fix Redis result compression with ``decode_responses`` (#10612)
- Honor a timeout of 0 instead of waiting/polling forever (``wait_for``,
  ``ResultSet.join``, ``get_many``, result drainer) (#10487, #10524, #10603)
- ``DatabaseBackend`` now honors ``result_serializer`` (#10536, closes #3025);
  fix ``KeyError`` in MongoDB backend for requestless results (#10503)
- Redact SAS tokens / lowercase account keys in the Azure Block Blob backend,
  and keep it picklable (#10620, #10647)
- Cache extension entry-point discovery to fix ``apply_async`` CPU overhead
  (#10516); bound ``State._tasks_to_resolve`` to fix unbounded memory growth
  (#10534)

Beat / scheduling / timezones
--------------------------------

- Handle DST gaps in ``make_aware``; normalize aware datetimes into the
  schedule timezone in ``crontab.remaining_delta`` (#10324, #10420, #10494)
- Fix beat sleeping too long / reheap entries that request a later retry
  (#10558, #10468); fix crontab state restoration across persistence cycles
  (#10563)
- Return the exact fractional ``utcoffset()`` instead of rounding it (#10625);
  preserve negative UTC offsets in ``LocalTimezone.fromutc`` (#10519)

Configuration
---------------

- ``ConfigurationView``: fix membership checks, map-order lookups,
  ``swap_with()``, and honor ``config_from_object(silent=True)`` on lazy
  config load (#10586, #10597, #10604, #10532)
- Keep preconfigured values out of configuration defaults (#10575); prevent
  ``add_defaults()`` crashing when ``config_from_object()`` fails on an
  already-configured app (#10621)
- Mask ``broker_read_url``/``broker_write_url`` and credentials in inspect
  stats (#10398, #10485)

Documentation
~~~~~~~~~~~~~

23 doc fixes/clarifications, including: clarified chord errback signatures
(#10300), canvas calling behavior (#10447), acks-late vs. retry semantics
(#10448), rate-limit/prefetch interactions (#10291), SQS FIFO message
properties (#10446), and numerous grammar/typo fixes across the config and
extending guides.

Tests & CI
~~~~~~~~~~

13 test-suite improvements (moving custom-worker tests into the smoke suite,
guarding integration tests, pinning Redis client timeouts, etc.) plus 9
CI/tooling updates (CI matrix changes, trying Python 3.15 pre-release,
pre-commit autoupdates).

Dependency Updates
~~~~~~~~~~~~~~~~~~~

~65 dependency bumps, mostly routine Dependabot/pre-commit updates (pytest,
cryptography, boto3/moto, gevent, GitHub Actions like ``actions/checkout``,
``codeql-action``, plus ``kombu`` and ``click-repl`` version bumps).

Reverts
~~~~~~~

- Reverted the RabbitMQ 4.3.0 "exclusive queues" default fix (#10289), later
  replaced with a transient-queue-specific fix (#10290)
- Reverted a Redis ``_pending_messages`` leak fix and a chord-hang fix, both
  later reworked



What's Changed
~~~~~~~~~~~~~~

- fix(defaults): default to exclusive queues for RabbitMQ 4.3.0 compatibility (#10287)
- Revert "fix(defaults): default to exclusive queues for RabbitMQ 4.3.0 compati…" (#10289)
- fix(defaults,rpc): RabbitMQ 4.3.0 compatibility for transient queues (#10290)
- Removed socket.timeout from reconnection conditions (#10285)
- fix(worker): skip Broadcast queues in native delayed delivery binding (#10293)
- [pre-commit.ci] pre-commit autoupdate (#10292)
- fix(worker): guard heartbeat control command against None event_dispatcher (#10295)
- fix(redis): keep _reconnect_pubsub compatible with redis-py < 5.3.0 (#10298)
- docs: clarify chord errback signatures (#10300)
- Chore(deps): Update python-memcached requirement from >=1.61 to >=1.62 (#10282)
- Allow Redis backend SSL config with redis URLs (#10313)
- Skip empty groups in chains (#10321)
- Handle DST gaps in make_aware (#10324)
- Fix worker consuming unexpected queues after connection loss (#10335)
- Apply backend retry policy to all DatabaseBackend DB operations (#10334)
- Explain prefetching with late acknowledgments (#10343)
- Add documentation to workers guide around signal exception bytecode stuff (#10276)
- Fix Consumer.on_close() wiping in-flight reservations and breaking worker_disable_prefetch (#10345)
- Fix: wrap new-style errback calls in try/except to prevent silent chain halt (#10214)
- Fix free worker stalling after broker reconnect (AsynPool.flush clearing _busy_workers) (#10346)
- Fix Queues.deselect leaking routing-only queues into consumers (#10348)
- fix(redis): prevent _pending_messages memory leak in ResultConsumer (#10342)
- chore(deps): bump cryptography in /requirements/extras (#10350)
- chore(deps): bump codecov/codecov-action from 6 to 7 (#10349)
- docs(tasks): warn that rate-limited tasks block the worker's prefetch (#10291)
- Fix add_consumer not restoring consume_from for known queues (#10352)
- Fix queue selection by alias to store the real queue name (#10358)
- Defer default queue lookup for explicitly routed tasks (#10357)
- Fix pidbox consumer cleanup during reset cycles (#10363)
- Fix typo (#10365)
- Redis ResultConsumer: clean up leaked _pending_messages after on_wait_for_pending (#10366)
- Mark rejected tasks as failed when not requeued (#10369)
- chore(deps): bump cryptography in /requirements/extras (#10368)
- chore(deps): bump actions/checkout from 6 to 7 (#10362)
- Update reserved requests to use new event dispatcher on reconnect (#10383)
- Tolerate task cancellation errors after connection loss (#10382)
- Preserve group errback mutability (#10385)
- fix(canvas): propagate chained task failures to the chord body (#10323)
- Document task registration requirement for the celery_worker fixture (#10378)
- Raise a clear error instead of a bare ValueError when fast_trace_task's worker registry is empty (#10397)
- chore(deps): bump actions/setup-python from 6 to 7
- Fix preload options for control command (#10403)
- Handle UUID task ids in key-value store backend key generation (#10406)
- Stop eager chain execution on Ignore and Reject (#10404)
- fix(canvas): restore GroupResult fan-out in chain as_tuple() (#8903 regression) (#10408)
- Finalize ResultSet barrier when `iter_native` begins (#10407)
- Move custom-worker integration tests to the smoke suite (#10410)
- Guard integration tests (#10411)
- Handle EPERM from os.kill in multi stopwait node liveness check
- Preserve re-buffered events during dispatcher flush (#10401)
- Possible secret in repository (#10257)
- Fix inconsistencies in systemd service examples (#10425)
- docs: describe canvas calling behavior accurately (#10447)
- docs: clarify acks late versus retry behavior (#10448)
- docs: link worker settings to CLI options (#10445)
- Preserve absolute event timestamps in Gossip (#10418)
- fix(events): don't drop buffered events appended during publish (#10435)
- Respect result_extended in the RPC result backend (#10431)
- docs: Fix grammar errors in extending user guide (#10429)
- Warn when routing options are set as task attributes
- docs: show SQS FIFO message properties directly (#10446)
- Replace static analysis hack in `__init__.py` with `TYPE_CHECKING` guard (#10409)
- RPC result backend recirculates final results on every state poll (#10459)
- chore(deps): bump github/codeql-action from 4 to 4.37.4 (#10454)
- Move task_sent and Result deprecations to the 6.0 timeline (#10455)
- Raise ImproperlyConfigured with install hint when ephem is missing for solar schedules
- Normalize aware datetimes into the schedule timezone in crontab.remaining_delta (#10420)
- Document EventReceiver's first argument under the name it actually has (#10460)
- Reheap beat entries that request to retry later (#10468)
- docs: document purpose of bulk_task_producer.py example (#10399)
- docs: document single-argument link_error errback used by send_task (#10427)
- chore(deps): bump github/codeql-action from 4.37.4 to 4.37.6 (#10469)
- Fix shared retry_kwargs mutation in autoretry (#10461)
- Add MemoryError regression for request callbacks (#10482)
- chore(deps): bump useblacksmith/setup-docker-builder from 1 to 2
- chore(deps): bump useblacksmith/setup-docker-builder from 2 to 2.0.1 (#10450)
- Remove redundant maybe_signature call in unlock_chord (#10395)
- Fix ineffective traceback clearing in timeout handling (#10493)
- Fix Redis result consumer redundant unsubscribe (#10497)
- Fix green pool autoscale capacity handling (#10361)
- fix(worker): mask credentials in alternates returned by inspect stats (#10485)
- Add datefmt support to ColorFormatter and TaskFormatter (#10488)
- Fix TypeError when slicing a regen built from a generator (#10501)
- Update boto3 requirement from >=1.26.143 to >=1.43.6 and moto 5.2 (#10306)
- Make result_compression actually compress stored results (#10504)
- fix: preserve negative UTC offsets in LocalTimezone.fromutc (Fixes #10517) (#10519)
- fix: handle invalid worker environment values (#10513)
- Fix certificate expiration during verification (#10510)
- Fix KeyError in MongoDB backend for results stored without a request (#10503)
- fix: crontab.remaining_delta skips later minute slots on an earlier day (#10494)
- Honor a timeout of 0 in wait_for and ResultSet.join instead of waiting forever (#10487)
- Mask broker_read_url and broker_write_url in filter_hidden_settings (#10398)
- handle beat_cron_starting_deadline correctly for non-uniform crontabs (#10315)
- Add task_repr_maxlevels setting to control argsrepr/kwargsrepr depth (#10371)
- feature: add celery_beat_task header in task options (#9702)
- Enable heartbeat on the event dispatcher connection (#10500)
- Update README.rst (#10521)
- docs: repoint dead :ref: targets to the getting-help/bug-tracker labels (#10578)
- docs(result): drop doubled 'in in' in the ResultSet class comment (#10577)
- Document interaction between rate_limit and countdown/eta
- Cache extension entry-point discovery to fix apply_async CPU overhead (#10516)
- Bound State._tasks_to_resolve to fix unbounded memory growth (#10534)
- Fix DjangoTask selection for Celery subclasses without a custom task class (#10529)
- Honor a timeout of 0 in get_many instead of polling forever (#10524)
- Fix the delay beat returns when the heap top changes (#10531)
- Fix: DatabaseBackend now honors result_serializer (#3025) (#10536)
- Honour `config_from_object(silent=True)` on the lazy config load (#10532)
- Apply ChainMap._key consistently across its methods (#10545)
- [Bug] chord hangs when a member is revoked through app.control.revoke (#10527)
- Show original and prefixed keys in configuration KeyError
- Forward chord body routing options to chord_unlock (#10442)
- Update CI matrix to exclude Python 3.10 and 3.14t (#10550)
- Fix beat sleeping too long when is_due changes the heap (#10558)
- Fix celery worker shutdown timeout example (#10416)
- Fix --detach stalling in Docker containers with high fdmax (#9886) (#10080)
- Fix crontab state restoration across persistence cycles (#10563)
- Return Option objects consistently from find() (#10565)
- Propagate setup logging signal errors (#10568)
- Honour broker connection retry settings when purging at startup (#10567)
- Fix Signature.clone() sharing kwargs with the original signature (#10571)
- fix(worker): bound already-open broker sockets before connection teardown (GH-9705) (#10570)
- Document how the Redis broker stores messages (#10566)
- `prefork` is not safe anymore (#10339)
- Revert "fix(redis): prevent _pending_messages memory leak in ResultConsumer (…"
- Detect spawned pool children in process_initializer (#10583)
- Allow worker_pool_start_method='spawn' with the asynchronous prefork pool (#10584)
- Keep preconfigured values out of configuration defaults (#10575)
- Clean up Docker examples in the contributing guide (#10587)
- Honor zero timeouts in the result drainer (#10603)
- Make ConfigurationView membership match key lookup (#10586)
- Respect map order in ConfigurationView lookups (#10597)
- Include _keys in ConfigurationView.swap_with() (#10604)
- Retain fallback context managers until exit (#10610)
- Preserve explicitly supplied empty timer schedules (#10609)
- Fix: query_task doesn't find tasks scheduled with an ETA/countdown (#5321) (#10535)
- Fix Redis result compression with decode_responses enabled (#10612)
- Docs: add missing @app.task decorator to chain example (#4750) (#10613)
- fix(canvas): propagate aggregate_duplicates through nested stamp merges
- Deny access to private attributes and dunders via registered() (#10602)
- Fix argument forwarding through nested chains in eager execution (#10611)
- Replacing a task with a chain which contains a group now returns a result instead of hanging (#9604)
- Fix linked signature option isolation during deepcopy (#10594)
- Prevent add_defaults() from crashing when config_from_object() fails on an already configured app (#10621)
- fix(backends.database): store task children with database backend (#8336) (#10606)
- Prevent spawned worker crashes with explicit lazy=True (#10619)
- Redact SAS tokens and lowercase account keys in AzureBlockBlobBackend.as_uri (#10620)
- Preserve configuration when copying settings (#10623)
- docs: fix 'allows to' grammar in dont_autoretry_for docs (#10615)
- docs: fix 'Allows to' grammar in configuration settings
- Warn when two callables collide under one task name (#10576) (#10598)
- Task.replace() crashes with tuple-backed chains (#10626)
- Prevent apply_async() from mutating cached execution options (#10624)
- Support queue aliases in Queues.deselect() (#10649)
- Add runtime in success signal (#9516)
- Add the request context to task_failure in case of lost worker (#9720)
- Set default max priority in Queues.__setitem__() when the queue has none  (#10645)
- backends.by_url() should accept backend classes (#10635)
- Fix celery multi crashing on a single node name containing a dash (#10643)
- Rebuild the GCS storage client after a fork when Firestore is touched first (#10641)
- Fix empty group/chain handling in chains and group headers (#9772) (#10646)
- try python 3.15 pre release on the CI (#10636)
- Keep a cancelled task unacked when the child reports SystemExit (#10667)
- Return the exact fractional utcoffset() instead of rounding it (#10625)
- Skip empty groups in eager chains (#10644)
- Store failures raised as BaseException subclasses with the JSON serializer (#10672)
- fix(backends.database): store stamping metadata with extended results (#8887) (#10605)
- fix(app.utils): handle multi-server URLs in sanitize_url and bugreport (#8014) (#10607)
- Refresh AMQP task routes for all configuration update forms (#10673)
- Redis result backend: serialize pubsub access between the drainer and result cleanup (#10671)
- Queues.__setitem__() should apply the default routing key (#10669)
- Fix revoked tasks running anyway after mingle merged revoke stamps from another host (#10668)
- Expect the prefetch reduction to be skipped under per-consumer QoS
- Keep the Azure Block Blob backend picklable (#10647)
- Update click-repl version to 0.4.0 (#10658)
- Update kombu version to 5.7.0a1 (#10677)
- Keep a finished result when a revoke arrives for the task (#10679)
- Revert "[Bug] chord hangs when a member is revoked through app.control.revoke" (#10564)
- Chore(deps): Bump pytest from 9.0.2 to 9.0.3
- Chore(deps): Update gevent requirement from >=1.5.0 to >=26.4.0
- Chore(deps): Bump pytest-order from 1.3.0 to 1.4.0
- Chore(deps): Update elasticsearch requirement from <=9.5.0 to <=9.5.1 (#10685)
