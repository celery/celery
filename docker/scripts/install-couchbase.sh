#!/bin/sh
wget http://packages.couchbase.com/clients/c/libcouchbase-2.8.4_jessie_amd64.tar
tar -vxf libcouchbase-2.8.4_jessie_amd64.tar
dpkg -i libcouchbase-2.8.4_jessie_amd64/libcouchbase2-core_2.8.4-1_amd64.deb
dpkg -i libcouchbase-2.8.4_jessie_amd64/libcouchbase-dev_2.8.4-1_amd64.deb
