#!/bin/bash

make --quiet --directory="$HOME/celery" clean-pyc

eval "$(pyenv init -)"
eval "$(pyenv virtualenv-init -)"
exec "$@"
