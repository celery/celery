#!/bin/bash -x

MOTO_DIRECTORY="$TRAVIS_BUILD_DIR/servers/moto"
mkdir -p "$MOTO_DIRECTORY"
cd $MOTO_DIRECTORY
virtualenv moto-env
git clone https://github.com/spulec/moto.git
cd moto
$MOTO_DIRECTORY/./moto-env/bin/pip install .[server]
cd .. && rm -rf moto
nohup $MOTO_DIRECTORY/./moto-env/bin/moto_server sqs -H 127.0.0.1 -p 80 &
