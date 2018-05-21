#!/usr/bin/env bash

sudo apt-get update && sudo apt-get install -y default-jre supervisor
mkdir /opt/dynamodb-local
cd /opt/dynamodb-local && curl --retry 5 --retry-delay 1 -L http://dynamodb-local.s3-website-us-west-2.amazonaws.com/dynamodb_local_latest.tar.gz | tar zx
cd -
echo '[program:dynamodb-local]' | sudo tee /etc/supervisor/conf.d/dynamodb-local.conf
echo 'command=java -Djava.library.path=./DynamoDBLocal_lib -jar DynamoDBLocal.jar -inMemory' | sudo tee -a /etc/supervisor/conf.d/dynamodb-local.conf
echo 'directory=/opt/dynamodb-local' | sudo tee -a /etc/supervisor/conf.d/dynamodb-local.conf
sudo service supervisor stop
sudo service supervisor start
sleep 10
curl localhost:8000
