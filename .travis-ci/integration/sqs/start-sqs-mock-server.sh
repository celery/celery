#!/bin/bash -x

set -e -u

sudo apt-get install -y supervisor
MOTO_DIRECTORY="$TRAVIS_BUILD_DIR/servers/moto"
mkdir -p "$MOTO_DIRECTORY"
cd $MOTO_DIRECTORY
virtualenv moto-env
git clone https://github.com/spulec/moto.git moto-git
$MOTO_DIRECTORY/./moto-env/bin/pip install -U pip
cd moto-git
# Use specific working commit
git checkout df84675ae67e717449f01fafe3a022eb14984e26
$MOTO_DIRECTORY/./moto-env/bin/pip install .[server]

SUPERVISORD_CONF_PATH=/etc/supervisor/conf.d/moto-server-sqs.conf
echo '[program:moto-server-sqs]' | sudo tee "$SUPERVISORD_CONF_PATH"
echo "command=$MOTO_DIRECTORY/./moto-env/bin/moto_server sqs -H 0.0.0.0 -p 80" | sudo tee -a "$SUPERVISORD_CONF_PATH"
echo "user=root" | sudo tee -a "$SUPERVISORD_CONF_PATH"
echo 'stdout_logfile=/var/log/moto-server-sqs.log' | sudo tee -a "$SUPERVISORD_CONF_PATH"
echo 'stderr_logfile=/var/log/moto-server-sqs.log' | sudo tee -a "$SUPERVISORD_CONF_PATH"
sudo service supervisor stop
sudo service supervisor start
sleep 10
curl -v localhost
