#!/bin/sh
# For managing all the local python installations for testing, use pyenv
curl -L https://github.com/pyenv/pyenv-installer/raw/master/bin/pyenv-installer | bash

# To enable testing versions like 3.4.8 as 3.4 in tox, we need to alias
# pyenv python versions
git clone https://github.com/s1341/pyenv-alias.git $(pyenv root)/plugins/pyenv-alias

# Python versions to test against
VERSION_ALIAS="python3.13" pyenv install 3.13.1
VERSION_ALIAS="python3.12" pyenv install 3.12.8
VERSION_ALIAS="python3.11" pyenv install 3.11.11
VERSION_ALIAS="python3.10" pyenv install 3.10.16
VERSION_ALIAS="python3.9" pyenv install 3.9.21
VERSION_ALIAS="python3.8" pyenv install 3.8.20
