#!/bin/sh
# Install Couchbase's GPG key
sudo wget -O - http://packages.couchbase.com/ubuntu/couchbase.key | sudo apt-key add -
# Adding Ubuntu 18.04 repo to apt/sources.list of 19.10 or 19.04
echo "deb http://packages.couchbase.com/ubuntu bionic bionic/main" | sudo tee /etc/apt/sources.list.d/couchbase.list
# To install or upgrade packages
apt-get update
apt-get install -y libcouchbase-dev libcouchbase2-bin build-essential
