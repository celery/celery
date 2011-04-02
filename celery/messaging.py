from celery import current_app

TaskPublisher = current_app.amqp.TaskPublisher
ConsumerSet = current_app.amqp.ConsumerSet
TaskConsumer = current_app.amqp.TaskConsumer
establish_connection = current_app.broker_connection
with_connection = current_app.with_default_connection
get_consumer_set = current_app.amqp.get_task_consumer
