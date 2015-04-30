Flume-ng RabbitMQ
========

This project forked from [jcustenborder/flume-ng-rabbitmq](https://github.com/jcustenborder/flume-ng-rabbitmq). Major improvements are on RabbitMQSink class at the moment.

<pre>mvn package</pre>

and put the resulting jar file in the lib directory in your flume installation.

This project is available under the Apache License.

Configuration of RabbitMQ Source
------
The configuration of RabbitMQ sources requires that you either declare an exchange name or a queue name.

The exchange name option is helpful if you have declared an exchange in RabbitMQ, but want to use a
default named queue.  If you have a predeclared queue that you want to receive events from, then you can simply declare
the queue name and leave the exchange name out.  Another optional configuration option is the declaration of
topic routing keys that you want to listen to.  This is a comma-delimited list.

**Minimal Config Example**

	agent1.sources.rabbitmq-source1.channels = ch1
	agent1.sources.rabbitmq-source1.type = org.apache.flume.source.rabbitmq.RabbitMQSource
	agent1.sources.rabbitmq-source1.hostname = 10.10.10.173

	agent1.sources.rabbitmq-source1.queuename = log_jammin
	OR
	agent1.sources.rabbitmq-source1.exchangename = log_jammin_exchange

**Full Config Example**

	agent1.sources.rabbitmq-source1.channels = ch1
	agent1.sources.rabbitmq-source1.type = org.apache.flume.source.rabbitmq.RabbitMQSource
	agent1.sources.rabbitmq-source1.hostname = 10.10.10.173

	agent1.sources.rabbitmq-source1.queuename = log_jammin
	OR
	agent1.sources.rabbitmq-source1.exchangename = log_jammin_exchange

	agent1.sources.rabbitmq-source1.topics = topic1,topic2
	agent1.sources.rabbitmq-source1.username = rabbitmquser
	agent1.sources.rabbitmq-source1.password = p@$$w0rd!
	agent1.sources.rabbitmq-source1.port = 12345
	agent1.sources.rabbitmq-source1.virtualhost = virtualhost1

RabbitMQ Sink
------

| Name           | Default                    | Description                                                               |
|----------------|----------------------------|---------------------------------------------------------------------------|
| hostname       | -                          | hostname of server where RabbitMQ is running on |
| username       | guest                      | username for connecting to RabbitMQ |
| password       | guest                      | password for connecting to RabbitMQ |
| port           | 5672                       | port RabbitMQ uses for linstening incoming traffic |
| virutalhost    | /                          | virutal host on RabbitMQ |
| queuename      | flume-rabbitmq-queue       | the queue name that messages will be shown on. Be aware that this is a durable queue which means messages written to this queue will be persisted to disk. |
| exchangename   | flume-rabbitmq-exchange    | the exchange name that flume is going send message to |
| routingkey     | flume-rabbitmq-routing-key | used by RabbitMQ to bind queue with exchange          |
| batchsize      | 1                          | by default, messages are sent to RabbitMQ one by one. By increasing the batchsize, a batch of messages will be sent to RabbitMQ before calling commit on flume transcation. |
| confirmtimeout | 500                        | there is a latency on RabbitMQ persists messages to disk. Consider increase this number if the batchsize is big |

**Minimal Config Example**

	agent1.sinks.rabbitmq-sink1.channels = ch1
	agent1.sinks.rabbitmq-sink1.type = org.apache.flume.sink.rabbitmq.RabbitMQSink
	agent1.sinks.rabbitmq-sink1.hostname = 10.10.10.173

**Full Config Example**

	agent1.sinks.rabbitmq-sink1.channels = ch1
	agent1.sinks.rabbitmq-sink1.type = org.apache.flume.sink.rabbitmq.RabbitMQSink
	agent1.sinks.rabbitmq-sink1.hostname = 10.10.10.173
	agent1.sinks.rabbitmq-sink1.username = guest
	agent1.sinks.rabbitmq-sink1.password = guest
	agent1.sinks.rabbitmq-sink1.port = 5672
	agent1.sinks.rabbitmq-sink1.virtualhost = virtualhost1
	agent1.sinks.rabbitmq-sink1.queuename = flume-rabbitmq-queue
	agent1.sinks.rabbitmq-sink1.exchangename = flume-rabbitmq-exchange
	agent1.sinks.rabbitmq-sink1.routingkey = flume-rabbitmq-routing-key
	agent1.sinks.rabbitmq-sink1.batchsize = 1
	agent1.sinks.rabbitmq-sink1.confirmtimeout = 500

