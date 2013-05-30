#!/usr/bin/env bash

while true; do
    sudo rabbitmqctl stop_app
    sudo rabbitmqctl start_app
    sleep 10
done
