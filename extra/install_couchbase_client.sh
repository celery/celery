#!/usr/bin/env bash

wget -qO - https://packages.couchbase.com/ubuntu/couchbase.key | sudo apt-key add -
sudo apt-add-repository -y 'deb http://packages.couchbase.com/ubuntu trusty trusty/main'
sudo apt-get update && sudo apt-get install -y libcouchbase-dev
