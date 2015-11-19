SimpleHelpers.Net.RabbitMQ
=================

RabbitMQ .net work queue helper class


How to install?
--------

Use NuGet [https://www.nuget.org/packages/SimpleHelpers.RabbitMQWorkQueue/]

```
Install-Package SimpleHelpers.RabbitMQWorkQueue
```


Description and Examples
--------

SimpleHelpers.RabbitWorkQueue is a simple helper class (RabbitWorkQueue) that implements a work queue using RabbitMQ server and the official [RabbitMQ .NET Client](https://www.rabbitmq.com/dotnet.html).

This work queue implementation will distribute messages (tasks) among workers. Each message will be send only once to a worker and, in case of failure to process the message, it will safelly be returned to the queue or sent to the dead-letter queue.


### Features

+ Wrapper the RabbitMQ .net client for work queue use case.
+ *IEnumerable* methods for receiving messages.
+ Dead letter queue for failed (negative acknowledgement) messages.
+ Maximum retry count for failed (negative acknowledgement) messages.
+ Configurable delay before failed message is available again in the queue.
+ Auto queue and exchanges creation.
+ Serialization and Deserialization with Jil serializer (json high performance .net serialization lib).
+ Thread safe message acknowledgment

---

### Examples

#### Publishing messages

Publishing object that will serialized to json before publishing

```
using (var queue = new RabbitWorkQueue ("amqp://login:password@my.rabbitmq.server.address:5672", "my-test-queue")
{
	for (var i = 0; i < 250; i++)
		queue.Publish (new MyObject ());
}
```


There is also support for publishing a text (string) without serialization

```
using (var queue = new RabbitWorkQueue ("amqp://login:password@my.rabbitmq.server.address:5672", "my-test-queue")
{
	for (var i = 0; i < 250; i++)
		queue.Publish ("teste message " + i);
}
```

---

#### Receiving messages


Simple loop

```
using (var queue = new RabbitWorkQueue ("amqp://login:password@my.rabbitmq.server.address:5672", "my-test-queue")
{
	// keep consuming the queue with a 30 min timeout (in case the queue is empty for 30 minutes)
	foreach (var i in queue.Get (TimeSpan.FromMinutes (30)))
	{
		try
		{
			// let's do some processing here ...
			// ...
			
			// in case of success, let's tell rabbitMq that this message was processed (acknowledge)!
			i.Ack ();
		}
		catch (Exception ex)
		{
			// in case of error, let's signal a negative acknowledgement
			i.Nack ();
			
			// logging...
		}
	}
}
```


##### Parallel processing example.

In this example we allow a maximum of 30 local messages. Increase the `batchSize` parameter for greater concurrency.
Also we configure the messages for no delay in case of retrying and a maximum of 10 retries.

Note that the message acknowledgement is thread safe (`i.Ack ()` and `i.Nack ()`).

```
using (var queue = new RabbitWorkQueue ("amqp://login:password@my.rabbitmq.server.address:5672", "my-test-queue", 30, RabbitWorkQueueMode.Transient, 0, 10)
{
	// keep consuming the queue with a 30 min timeout (in case the queue is empty for 30 minutes)
	Parallel.ForEach<RabbitWorkMessage> (queue.Get (TimeSpan.FromMinutes (30)), i =>
	{
		try
		{
			// let's do some processing here ...
			// ...
			
			// in case of success, let's tell rabbitMq that this message was processed (acknoledge)!
			i.Ack ();
		}
		catch (Exception ex)
		{
			// in case of error, let's signal a negative acknowledgement
			i.Nack ();
			
			// logging...
		}
	});
}
```

---

### Maximum Retry and Dead Letter Queue


Every time a message receives a negative acknowledgement (Nack), it will increment a internal counter (in the message header). If this counter is greater than the `maxRetry` parameter, then this message is removed from the queue and moved to the dead letter queue.


The dead letter queue is a queue with the same name as the queue plus the suffix ".dead-letter".


---

### Configuration

RabbitWorkQueue constructor parameters:

+ **queueUri:** The queue URI address formatted as `amqp://login:password@address:port`
+ **queueName:** Queue name. If empty, a temporary queue with a random queue name will be created.
+ **batchSize:** Maximum number of unacknowledged messages for this connection. This helps to improve throughput as multiple messages are received for each request. Use '0' for ilimited.
+ **mode:** How to handle queue opening, creation and message publishing.
+ **retryDelayMilliseconds:** The retry delay milliseconds. In case of failed message (Nack) will wait the retry delay before becoming available again.
+ **maxRetry:** The max retry for failed (Nack) messages.


#### Batch Size explained

Batch size configures the maximum number of unacknowledged messages for this connection. This helps to improve throughput as multiple messages are received for each request reducing the number of round trips to the RabbitMQ server to retrieve messages. 

Use `0` for ilimited unacknowledged messages (not recommended).

This parameter affects the *QoS prefetch setting*. If you want a more in-depth view of this setting, you should read [Some queuing theory: throughput, latency and bandwidth](https://www.rabbitmq.com/blog/2012/05/11/some-queuing-theory-throughput-latency-and-bandwidth/).

As a guide line, follow these recommendations:

**0.** RabbitMQ ensures that the unacknowledged messages will be safely returned to the queue if your application fail to process them (in case of a network failure or application crash).

If the connection is dropped or closed, the messages that were not acknowledged (`Ack`) will be safely returned to que queue.


**1.** If the application use multiple threads to process the consumed messages. The `batchSize` parameter should be greater than the number of threads.

For instance, if the application is using 30 threads to process the messages, than you need more than 30 unacknowledged messages, otherwise some threads will be idle without work to do...


**2.** If each message is takes little processing time, they will be consumed fast! So a greater `batchSize` will allow less waiting for new messages to arrive.


**3.** While the usage of `batchSize` is recommended, note that these messages won't be available for other consumers, so a huge `batchSize` should be used with care...


#### RabbitMQ WorkQueue Modes

These options affects the behavior of RabbitWorkQueue in two steps:

1. **At initialization,** affecting the behavior of the queue by ensuring its existence or not.
2. **At publishing,** by publish messages as transient or persistent.


**List of Modes**

+ **OpenOrCreateInMemory:** Open or creates fast in-memory durable queue and publishes messages in-memory mode.

Also ensures that a delayed exchange exists.

Since RabbitMQ will keep the messages in-memory, this mode is faster but the messages will be lost in case of a server restart or crash.


+ **OpenOrCreatePersistent:** Open or creates a durable queue and publishes messages with persistence (slower since every message is written to disk).

Also ensures that a delayed exchange exists.

Since RabbitMQ will keep the messages in-memory, this mode is slower but safer in case of a server restart or crash.

+ **OpenOrCreateTemporary:** Open or creates a temporary queue and publishes messages as transient (in-memory) that will be deleted after all consumers disconnects.

Also ensures that a delayed exchange exists.

Since RabbitMQ will keep the messages in-memory, this mode is faster but the messages will be lost in case of a server restart or crash.

+ **OpenInMemory:** All messages will be published in-memory but the queue will not be created if not exists.

Since RabbitMQ will keep the messages in-memory, this mode is faster.

+ **OpenPersistent:** All messages will be published as persistent (disk) but the queue will not be created if not exits.

Since RabbitMQ will keep the messages in-memory, this mode is slower but safer in case of a server restart or crash.


#### Maximum Retry and Retry Delay

The work queue can also configure what to do with failed messages, i.e. messages with **Negative Acknowledgement** or **Nack**.

Whenever a Nack is received, the message is send to the dead-letter queue if the number of tries is greater than the Maximum Retry count.

If the number of tries is less than the Maximum Retry count, than the message is sent back to the queue and will be available after the Retry Delay setting.

