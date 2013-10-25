#!/usr/bin/env bash

secs=${1:-30}
secs=$((secs - 1))

while true; do
    sudo rabbitmqctl start_app
    echo "sleep for ${secs}s"
    sleep $secs
    sudo rabbitmqctl stop_app
    echo "sleep for 1s"
    sleep 1
done
