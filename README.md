SimpleHelpers.Net.RabbitMQ
=================

RabbitMQ .net work queue helper class


Description and Examples
--------

Simple helper class that implements a work queue using RabbitMQ.

### Features

+ Wrapper the RabbitMQ .net client for work queue use case.
+ IEnumerable methods for receiving messages.
+ Dead letter queue for failed (negative acknoledgement) messages.
+ Maximum retry count for failed (negative acknoledgement) messages.
+ Configurable delay before failed message is avalable again in the queue.
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
			
			// in case of success, let's tell rabbitMq that this message was processed (acknoledge)!
			i.Ack ();
		}
		catch (Exception ex)
		{
			// in case of error, let's signal a negative acknoledgement
			i.Nack ();
			
			// logging...
		}
	}
}
```


##### Parallel processing example.

In this example we allow a maximum of 30 local messages. Increase the `batchSize` parameter for greater concurrency.
Also we configure the messages for no delay in case of retrying and a maximum of 10 retries.

Note that the message acknoledgement is thread safe (`i.Ack ()` and `i.Nack ()`).

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
			// in case of error, let's signal a negative acknoledgement
			i.Nack ();
			
			// logging...
		}
	});
}
```

---

### Maximum Retry and Dead Letter Queue


Every time a message receives a negative acknoledgement (Nack), it will increment a internal counter (in the message header). If this counter is greater than the maxRetry parameter, then this message is removed from the queue and moved to the dead letter queue.

The dead letter queue is a queue with the same name as the queue plus the suffix ".dead-letter".

---

### Configuration

RabbitWorkQueue constructor parameters:

+ **queueUri:** The queue URI address formated as `amqp://login:password@address:port`
+ **queueName:** Queue name. If empty, a temporary queue with a random queue name will be created.
+ **batchSize:** Maximum number of unacknoledged messages for this connection. This helps to improve throughput as multiple messages are received for each request. Use '0' for ilimited.
+ **mode:** How to handle queue creation and message publishing.
+ **retryDelayMilliseconds:** The retry delay milliseconds. In case of failed message (Nack) will wait the retry delay before becoming available again.
+ **maxRetry:** The max retry for failed (Nack) messages.


