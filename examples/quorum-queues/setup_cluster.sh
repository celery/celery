#!/bin/bash

ERLANG_COOKIE="MYSECRETCOOKIE"

cleanup() {
    echo "Stopping and removing existing RabbitMQ containers..."
    docker stop rabbit1 rabbit2 rabbit3 2>/dev/null
    docker rm rabbit1 rabbit2 rabbit3 2>/dev/null

    echo "Removing existing Docker network..."
    docker network rm rabbitmq-cluster 2>/dev/null
}

wait_for_container() {
    local container_name=$1
    local retries=20
    local count=0

    until [ "$(docker inspect -f {{.State.Running}} $container_name)" == "true" ]; do
        sleep 1
        count=$((count + 1))
        if [ $count -ge $retries ]; then
            echo "Error: Container $container_name did not start in time."
            exit 1
        fi
    done
}

wait_for_rabbitmq() {
    local container_name=$1
    local retries=10
    local count=0

    until docker exec -it $container_name rabbitmqctl status; do
        sleep 1
        count=$((count + 1))
        if [ $count -ge $retries ]; then
            echo "Error: RabbitMQ in container $container_name did not start in time."
            exit 1
        fi
    done
}

setup_cluster() {
    echo "Creating Docker network for RabbitMQ cluster..."
    docker network create rabbitmq-cluster

    echo "Starting rabbit1 container..."
    docker run -d --rm --name rabbit1 --hostname rabbit1 --net rabbitmq-cluster \
        -e RABBITMQ_NODENAME=rabbit@rabbit1 \
        -e RABBITMQ_ERLANG_COOKIE=$ERLANG_COOKIE \
        --net-alias rabbit1 \
        -p 15672:15672 -p 5672:5672 rabbitmq:3-management

    sleep 5
    wait_for_container rabbit1
    wait_for_rabbitmq rabbit1

    # echo "Installing netcat in rabbit1 for debugging purposes..."
    # docker exec -it rabbit1 bash -c "apt-get update && apt-get install -y netcat"

    echo "Starting rabbit2 container..."
    docker run -d --rm --name rabbit2 --hostname rabbit2 --net rabbitmq-cluster \
        -e RABBITMQ_NODENAME=rabbit@rabbit2 \
        -e RABBITMQ_ERLANG_COOKIE=$ERLANG_COOKIE \
        --net-alias rabbit2 \
        -p 15673:15672 -p 5673:5672 rabbitmq:3-management

    sleep 5
    wait_for_container rabbit2
    wait_for_rabbitmq rabbit2

    # echo "Installing netcat in rabbit2 for debugging purposes..."
    # docker exec -it rabbit2 bash -c "apt-get update && apt-get install -y netcat"

    echo "Starting rabbit3 container..."
    docker run -d --rm --name rabbit3 --hostname rabbit3 --net rabbitmq-cluster \
        -e RABBITMQ_NODENAME=rabbit@rabbit3 \
        -e RABBITMQ_ERLANG_COOKIE=$ERLANG_COOKIE \
        --net-alias rabbit3 \
        -p 15674:15672 -p 5674:5672 rabbitmq:3-management

    sleep 5
    wait_for_container rabbit3
    wait_for_rabbitmq rabbit3

    # echo "Installing netcat in rabbit3 for debugging purposes..."
    # docker exec -it rabbit3 bash -c "apt-get update && apt-get install -y netcat"

    echo "Joining rabbit2 to the cluster..."
    docker exec -it rabbit2 rabbitmqctl stop_app
    docker exec -it rabbit2 rabbitmqctl reset
    docker exec -it rabbit2 rabbitmqctl join_cluster rabbit@rabbit1
    if [ $? -ne 0 ]; then
        echo "Error: Failed to join rabbit2 to the cluster."
        exit 1
    fi
    docker exec -it rabbit2 rabbitmqctl start_app

    echo "Joining rabbit3 to the cluster..."
    docker exec -it rabbit3 rabbitmqctl stop_app
    docker exec -it rabbit3 rabbitmqctl reset
    docker exec -it rabbit3 rabbitmqctl join_cluster rabbit@rabbit1
    if [ $? -ne 0 ]; then
        echo "Error: Failed to join rabbit3 to the cluster."
        exit 1
    fi
    docker exec -it rabbit3 rabbitmqctl start_app

    echo "Verifying cluster status from rabbit1..."
    docker exec -it rabbit1 rabbitmqctl cluster_status
}

cleanup
setup_cluster

echo "RabbitMQ cluster setup is complete."
