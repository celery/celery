#!/bin/bash

pip --disable-pip-version-check install "$@"

if [[ "${TRAVIS_PYTHON_VERSION}" == "3.7" ]]; then
  # We have to uninstall the typing package which comes along with
  # the couchbase package in order to prevent an error on CI for Python 3.7.
  pip uninstall typing -y
fi
