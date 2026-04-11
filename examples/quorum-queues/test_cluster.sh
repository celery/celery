#!/bin/bash

QUEUE_NAME="my-quorum-queue"
VHOST="/"

remove_existing_queue() {
  docker exec -it rabbit1 rabbitmqctl delete_queue $QUEUE_NAME
}

create_quorum_queue() {
  docker exec -it rabbit1 rabbitmqadmin declare queue name=$QUEUE_NAME durable=true arguments='{"x-queue-type":"quorum"}'
}

verify_quorum_queue() {
  docker exec -it rabbit1 rabbitmqctl list_queues name type durable auto_delete arguments | grep $QUEUE_NAME
}

send_test_message() {
  docker exec -it rabbit1 rabbitmqadmin publish exchange=amq.default routing_key=$QUEUE_NAME payload='Hello, RabbitMQ!'
}

receive_test_message() {
  docker exec -it rabbit1 rabbitmqadmin get queue=$QUEUE_NAME ackmode=ack_requeue_false
}

echo "Removing existing quorum queue if it exists..."
remove_existing_queue

echo "Creating quorum queue..."
create_quorum_queue

echo "Verifying quorum queue..."
verify_quorum_queue

echo "Sending test message..."
send_test_message

echo "Receiving test message..."
receive_test_message

echo "Quorum queue setup and message test completed successfully."
