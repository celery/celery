#!/bin/sh
# For managing all the local python installations for testing, use pyenv
curl -L https://raw.githubusercontent.com/pyenv/pyenv-installer/master/bin/pyenv-installer | bash

# To enable testing versions like 3.4.8 as 3.4 in tox, we need to alias
# pyenv python versions
git clone https://github.com/s1341/pyenv-alias.git $(pyenv root)/plugins/pyenv-alias

# Python versions to test against
VERSION_ALIAS="python2.7" pyenv install 2.7.14
VERSION_ALIAS="python3.4" pyenv install 3.4.8
VERSION_ALIAS="python3.5" pyenv install 3.5.5
VERSION_ALIAS="python3.6" pyenv install 3.6.4
