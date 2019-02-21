---
name: Bug Report
about: Is something wrong with Celery?
---

# Checklist

- [ ] I have included the output of ``celery -A proj report`` in the issue.
    (if you are not able to do this, then at least specify the Celery
     version affected).
- [ ] I have included all related issues and possible duplicate issues in this issue.
- [ ] I have included the contents of ``pip freeze`` in the issue.
- [ ] I have verified that the issue exists against the `master` branch of Celery.
- [ ] I have tried reproducing the issue on more than one message broker and/or result backend.
- [ ] I have tried reproducing the issue on more than one workers pool.
- [ ] I have tried reproducing the issue with retries, ETA/Countdown & rate limits disabled.

## Related Issues and Possible Duplicates
<!--
Please make sure to search and mention any related issues or possible duplicates to this issue.
-->

#### Related Issues

- None

#### Possible Duplicates

- None

## Environment & Settings
<!-- Include the contents of celery --version below -->
**Celery version**:
<!-- Include the output of celery -A proj report below -->
<details>
<summary><b><code>celery report</code> Output:</b></summary>
<p>

```
```

</p>
</details>

# Steps to Reproduce

## Required Dependencies
<!-- Please fill the required dependencies to reproduce this issue -->
* **Minimal Python Version**: N/A or Unknown
* **Minimal Broker Version**: N/A or Unknown
* **Minimal Result Backend Version**: N/A or Unknown
* **Minimal OS and/or Kernel Version**: N/A or Unknown
* **Minimal Broker Client Version**: N/A or Unknown
* **Minimal Result Backend Client Version**: N/A or Unknown

### Python Packages
<!-- Please fill the contents of pip freeze below -->
<details>
<summary><b><code>pip freeze</code> Output:</b></summary>
<p>

```
```

</p>
</details>

### Other Dependencies
<!--
Please provide system dependencies, configuration files
and other dependency information if applicable
-->
<details>
<p>
N/A
</p>
</details>

## Minimally Reproducible Test Case
<!--
Please provide a reproducible test case.
Refer to the Reporting Bugs section in our contribution guide.

We prefer submitting test cases in the form of a PR to our integration test suite.
If you can provide one, please mention the PR number below.
If not, please attach the most minimal code example required to reproduce the issue below.
If there test case is too large, please include a link to a gist or a repository below.
-->

<details>
<p>

```python
```

</p>
</details>

# Expected Behavior
<!-- Describe in detail what you expect to happen -->

# Actual Behavior
<!--
Describe in detail what actually happened.
Please include a backtrace and surround it with triple backticks (```).
In addition, include the Celery daemon logs below.
-->
