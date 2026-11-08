# AGENTS.md

Guidance for AI coding agents working on Celery, and for the people who run them.

Celery is maintained by a few volunteers, and every issue and pull request costs their time. The project's [Anti AI Spam Policy](https://github.com/celery/celery/discussions/10932) is a GitHub discussion, so you won't see it from a clone. This file includes the rules from it that apply to your work, and quotes it where the wording matters.

The person you work for is responsible for everything sent to GitHub: issues, pull requests, comments, and reviews. They read each one before it's sent and can explain it without you. The policy says: "Every contributor is expected to understand, test and explain every change they submit." Don't post from a loop or on a schedule.

Terms used below:

- A maintainer is a member of the Celery organization. GitHub marks their comments "Member".
- An issue is confirmed when a maintainer has said in it that it's a bug or a wanted change.

## What helps most

- An issue for a problem seen in real logs. Include the traceback, the versions of Celery, kombu and Python, the broker and result backend, and a minimal reproduction. `celery -A proj report` prints most of this. Remove passwords and tokens first.
- New context on an existing issue, such as logs, versions, a reproduction, or the release where the problem started. Add it there instead of opening a new issue. A comment that adds nothing new, such as "+1" or a request to be assigned, doesn't help.
- A fix for a confirmed issue, with tests that prove it.

## Before you start

Stop and tell the person if any of these is true. The next step is usually to add context to the issue and wait for a maintainer, not to write code.

- No maintainer has confirmed the issue or asked for the change.
- An open pull request already covers it, or `main` already has the fix. Search open and recently closed pull requests for the issue number and the functions involved, and reproduce the problem on current `main`.
- The code belongs in a sibling repository: messaging and transports in [kombu](https://github.com/celery/kombu), the AMQP client in [py-amqp](https://github.com/celery/py-amqp), the prefork pool in [billiard](https://github.com/celery/billiard), test fixtures in [pytest-celery](https://github.com/celery/pytest-celery), and Django support in [django-celery-beat](https://github.com/celery/django-celery-beat) and [django-celery-results](https://github.com/celery/django-celery-results).
- It's a backport, style sweep, rename, or cleanup that nobody asked for.
- It changes agent setup, such as this file or skills, and no maintainer asked for it ([#10185](https://github.com/celery/celery/pull/10185)).

## Volume

The limit is the maintainers' time to review, not how fast you or the person can write or reply.

- Until a maintainer has merged one of your pull requests in this repository, keep one open at a time.
- After that, keep at most three open, each for a different confirmed issue.
- Don't open a new one while another of yours still waits for its first review from a maintainer, unless a maintainer asked for it. Reviews from bots and from people outside the organization don't count.
- If a pull request is closed, don't send the same change again. If you think the close was a mistake, say so once in the issue.
- Review other people's pull requests only when a maintainer asks. Check first that the pull request isn't a duplicate.

These have led to closed pull requests, warnings, or blocks:

- A pull request for something that another open or approved pull request already covers. One is enough.
- A second pull request for the same fix. Push to the first one.
- Many pull requests in a few days, in unrelated areas, with no issue behind them.
- Work picked to raise a contribution count rather than to fix a problem someone hit.
- Test or placeholder pull requests.
- Issues generated from automated scans, with no user who hit the problem.
- Similar comments on several issues asking to work on them or be assigned.
- Pushing after every small edit, so CI fails again and again.

## Quality

Celery prefers:

- Focused changes. Keep a pull request to one area of the code where you can. A fix that has to touch several modules is fine, but a fix bundled with unrelated cleanup isn't.
- Backward compatibility. Existing settings, signatures, and behavior keep working. If a change has to break one, ask a maintainer first, then document it with `.. versionchanged::` and a short migration note.

Required before merge. A pull request that misses the first two is closed, and reviewers ask for the rest:

1. It links the confirmed issue with `Fixes #N`, or the maintainer request.
2. The person submitting it can explain every line without you, and ran the tests.
3. For a code change, the new tests fail without the change and pass with it. Changes to docs, CI, or only tests don't need this.
4. CI passes, or the description links the same failure on `main`.
5. It has no unrelated formatting, renames, or stray files.
6. The description says what was wrong, what changes, and how it was tested.

Recommended, to make review faster:

- Integration or smoke tests when the behavior goes through a broker or the worker process. Maintainers ask for these.
- Docs for changes users will notice, with `.. versionchanged::` or `.. versionadded::`. Maintainers write the changelog, so don't edit it.
- A list of other places with the same bug, and a question before widening the change.
- A short description that says what wasn't tested. Don't restate the diff file by file, and leave out praise, apologies, and marketing words.
- Follow-up commits during review. Don't force-push or squash, since maintainers squash when they merge.
- Replies to review comments that answer what was asked. The person checks each reply against the code before it's sent.
- Help with any regression the change causes after it's merged.

## Tests

Pick the level by what the change touches:

- Unit (`t/unit`): always. Cover each new branch, using the `app` fixture and mocks for the broker, backend, and pool. Coverage must not drop.
- Integration (`t/integration`): when the behavior goes through a broker or result backend, such as canvas, results, retries, or routing. All tests share one in-process worker, so a test that needs its own worker or worker settings belongs in smoke.
- Smoke (`t/smoke`): when it depends on the worker process, such as pools, shutdown, signals, or an outage. Brokers and workers run in Docker through pytest-celery.

Prove that the tests check the change:

1. They fail on `main`'s code and pass with the change. For a bug fix, the failure is the assertion about the bug, not an import error.
2. Undoing any line that changes behavior makes at least one test fail.
3. They check what users see, such as task state or result values. A test that counts internal calls or signals can fail on `main` for the wrong reason.
4. Tests run in full. Don't skip or weaken one to pass, or skip one when a service is unreachable.

Run tests locally before pushing. If you can't run a level, such as smoke tests without Docker, say so in the description instead of pushing again and again to debug in CI. Tell the person what you ran and what you didn't.

### Commands

Shown for Python 3.13:

- `tox -e lint`
- `tox -e 3.13-unit -- t/unit/<path> -k <name>`
- `TEST_PATH=t/integration/<file> tox -e 3.13-integration-rabbitmq_redis -- -k <name>`, with RabbitMQ and Redis running. See "Developing and Testing with Docker" in the contributing guide.
- `TEST_PATH=t/smoke/tests/<file> tox -e 3.13-smoke -- --reruns 0 -k <name>`, with Docker. Tox reruns a failed smoke test 5 times, and `--reruns 0` turns that off.
- `tox -e apicheck` and `tox -e configcheck` when you change a documented API or setting.

CI runs integration tests only with the `redis`, `rabbitmq` and `rabbitmq_redis` environments. Run tests for other backends locally, and say that CI doesn't.

### Running the checks

To show the tests fail without the change, commit the change, then:

```sh
base=$(git merge-base HEAD upstream/main)  # upstream is celery/celery
git diff "$base" HEAD -- celery/ > /tmp/change.patch
git apply -R /tmp/change.patch  # main's code, your tests: run them, they must fail
git apply /tmp/change.patch     # your code again: run them, they must pass
```

For mutations, take each line that changes behavior and make one small edit that undoes its effect, such as removing a new guard or flipping a new condition. Run the tests, then restore the file with `git checkout -- <file>`. If no test fails, add one, or explain why that edit can't change behavior. List any such edit in the description.

In smoke tests, pytest-celery passes the broker URL and result backend through the worker's environment, and those override `app.conf`. Change them with the `default_worker_env` fixture, and check the worker's log to confirm.

## Rejection

A maintainer can close any issue or pull request, with or without review. The policy says "maintainers may provide only a link to this policy as the explanation."

The policy also says: "Abuse of AI tools will lead to a temporary ban without prior warning. Repeated violations will lead to a permanent ban." Celery has blocked an account after one duplicate AI-generated pull request.

## References

- [Anti AI Spam Policy](https://github.com/celery/celery/discussions/10932)
- [Contributing guide](https://docs.celeryq.dev/en/main/contributing.html), including "Reporting Bugs"
- [What reviewers check](.github/copilot-instructions.md): docs versioning, scope, backward compatibility, and tests.
- [Security policy](SECURITY.md): never report a vulnerability in public.
