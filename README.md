rabbitmq-ha-client
==================

High availability client for RabbitMQ inspired/derived from https://github.com/joshdevins/rabbitmq-ha-client

FIXME
==================
* Investigate under what conditions it is ok to not let a consumer see a shutdown signal. right now, I never let the consumer see it. the only thing I can think of is if its application initiated. But I need to investigate under what conditions an "application initiated shutdown signal" is created

TODO
==================
* add configuration options for max tries
* add configuration options for timeouts
* add configuration for reconnect delay