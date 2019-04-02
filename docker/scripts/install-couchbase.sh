#!/bin/sh
wget http://packages.couchbase.com/releases/couchbase-release/couchbase-release-1.0-4-amd64.deb
dpkg -i couchbase-release-1.0-4-amd64.deb
apt-get update
apt-get install libcouchbase-dev build-essential
