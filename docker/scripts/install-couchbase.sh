#!/bin/sh
wget http://packages.couchbase.com/clients/c/libcouchbase-2.10.4_buster_amd64.tar
tar -vxf libcouchbase-2.10.4_buster_amd64.tar
dpkg -i libcouchbase-2.10.4_buster_amd64/libcouchbase2-core_2.10.3-1_amd64.deb
dpkg -i libcouchbase-2.10.4_buster_amd64/libcouchbase-dev_2.10.3-1_amd64.deb
