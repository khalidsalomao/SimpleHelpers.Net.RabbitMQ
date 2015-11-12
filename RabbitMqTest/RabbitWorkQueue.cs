using RabbitMQ.Client;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace RabbitMqTest
{
    /// <summary>
    /// RabbitMQ helper for a simple work queue with retry logic
    /// </summary>
    public class RabbitWorkQueue : IDisposable
    {
        internal static readonly Jil.Options DefaultJilSettings = Jil.Options.ISO8601ExcludeNullsIncludeInheritedUtc;
        
        private bool _publishPersistent = false;
        private RabbitConnectionFactory _factory;
        private ushort _batchSize;
        private int _retryDelayMilliseconds;        
        private long _maxRetry;
        private IModel _consumerChannel;
        private IModel _publishChannel;

        private string delayedExchange;
        private string deadLetterQueue;

        /// <summary>
        /// Current connection
        /// </summary>
        public IConnection Connection
        {
            get { return _factory.GetConnection (); }
        }

        /// <summary>
        /// Consumer channel. Channel where used for consumer subscription and get operations.
        /// </summary>
        public IModel ConsumerChannel
        {            
            get
            {
                if (_consumerChannel == null)
                    _consumerChannel = CreateChannel ();
                return _consumerChannel;
            }
        }

        /// <summary>
        /// Channel (IModel) used for message publishing operations
        /// </summary>
        public IModel PublishChannel
        {
            get
            {
                if (_publishChannel == null || _publishChannel.IsClosed)
                    _publishChannel = CreateChannel ();
                return _publishChannel;
            }
        }

        /// <summary>
        /// Current queue name (used as routing key for publish requests)
        /// </summary>
        public string QueueName { get; set; }
        
        /// <summary>
        ///  How to handle queue creation and message publishing.
        /// </summary>
        public RabbitWorkQueueMode Mode { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="RabbitWorkQueue" /> class.
        /// </summary>
        /// <param name="queueUri">The queue URI address formated as amqp://login:password@address:port</param>
        /// <param name="queueName">Queue name. If empty, a temporary queue with a random queue name will be created.</param>
        /// <param name="batchSize">Maximum number of unacknowledged messages for this connection. This helps to improve throughput as multiple messages are received for each request. Use '0' for ilimited.</param>
        /// <param name="mode">How to handle queue creation and message publishing.</param>
        /// <param name="retryDelayMilliseconds">The retry delay milliseconds. In case of failed message (Nack) will wait the retry delay before becoming available again.</param>
        /// <param name="maxRetry">The max retry for failed (Nack) messages.</param>
        public RabbitWorkQueue (string queueUri, string queueName, ushort batchSize = 10, RabbitWorkQueueMode mode = RabbitWorkQueueMode.OpenOrCreateInMemory, int retryDelayMilliseconds = 0, int maxRetry = 0)
        {
            if (queueUri.IndexOf ("://") < 0)
                queueUri = "amqp://" + queueUri;

            var uri = new Uri (queueUri);
            var auth = uri.UserInfo.Split (':');
            
            Initialize (uri.Host, uri.Port > 0 ? uri.Port : 5672, auth[0], auth.Length > 1 ? auth[1] : "", queueName, batchSize, mode, retryDelayMilliseconds, maxRetry);
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RabbitWorkQueue" /> class.
        /// </summary>
        /// <param name="address">RabbitMQ address.</param>
        /// <param name="port">RabbitMQ server port. Defaults to 5672.</param>
        /// <param name="username">RabbitMQ username.</param>
        /// <param name="password">RabbitMQ password.</param>
        /// <param name="queueName">Queue name. If empty, a temporary queue with a random queue name will be created.</param>
        /// <param name="batchSize">Maximum number of unacknowledged messages for this connection. This helps to improve throughput as multiple messages are received for each request. Use '0' for ilimited.</param>
        /// <param name="mode">How to handle queue creation and message publishing.</param>
        /// <param name="retryDelayMilliseconds">The retry delay milliseconds. In case of failed message (Nack) will wait the retry delay before becoming available again.</param>
        /// <param name="maxRetry">The max retry for failed (Nack) messages.</param>
        public RabbitWorkQueue (string address, int port, string username, string password, string queueName, ushort batchSize = 10, RabbitWorkQueueMode mode = RabbitWorkQueueMode.OpenOrCreateInMemory, int retryDelayMilliseconds = 0, int maxRetry = 0)
        {
            Initialize (address, port, username, password, queueName, batchSize, mode, retryDelayMilliseconds, maxRetry);
        }

        private void Initialize (string address, int port, string username, string password, string queueName, ushort batchSize, RabbitWorkQueueMode mode, int retryDelayMilliseconds, int maxRetry)
        {
            QueueName = queueName;
            Mode = mode;            
            _batchSize = batchSize;
            _retryDelayMilliseconds = retryDelayMilliseconds;
            _maxRetry = maxRetry;

            // open connection and channel
            _factory = new RabbitConnectionFactory (address, port, username, password);
            
            // create our channels
            _publishChannel = CreateChannel ();
            _consumerChannel = CreateChannel ();

            // publish mode
            _publishPersistent = mode == RabbitWorkQueueMode.OpenOrCreatePersistent || mode == RabbitWorkQueueMode.OpenPersistent;

            // create queue (only if not exists)
            if (mode != RabbitWorkQueueMode.OpenPersistent && mode != RabbitWorkQueueMode.OpenInMemory)
            {
                if (String.IsNullOrEmpty (queueName))
                    queueName = "temp_" + System.IO.Path.GetRandomFileName ().Replace (".", "") + "_" + DateTime.UtcNow.ToString ("yyyyMMdd");

                // declare delayed exchange
                if (retryDelayMilliseconds > 0)
                {
                    delayedExchange = queueName + ".delayed";
                    SafeExecute (c => c.ExchangeDeclare (delayedExchange, "x-delayed-message", true, true, CreateProperty ("x-delayed-type", "direct")));
                }

                // create dead letter queue
                // note: dead-letter queue will be create only if needed                

                // check if queue already exists  
                var args = new Dictionary<string, object> ();
                args.AddProperty ("x-dead-letter-exchange", delayedExchange ?? "");
                // args.AddProperty ("x-dead-letter-routing-key", queueName);
                
                QueueName = EnsureQueueExists (queueName, mode, args);                

                // bind together exchanges to queue                
                SafeExecute (c => c.QueueBind (QueueName, delayedExchange, QueueName));
            }
        }

        /// <summary>
        /// Creates a new channel (IModel).
        /// </summary>
        public IModel CreateChannel ()
        {
            return _factory.GetConnection ().CreateModel ();
        }

        /// <summary>
        /// Publishes a message to the queue using the QueueName as the routingKey.
        /// </summary>
        /// <typeparam name="T">The type of the message object.</typeparam>
        /// <param name="msg">The message.</param>
        /// <param name="exchange">[Optional] The exchange name.</param>
        public void Publish<T> (T msg, string exchange = "")
        {
            PublishMessage (msg, exchange, QueueName, _retryDelayMilliseconds);
        }

        /// <summary>
        /// Publishes a message to the queue using the QueueName as the routingKey.
        /// </summary>
        /// <typeparam name="T">The type of the message object.</typeparam>
        /// <param name="msg">The message.</param>
        /// <param name="delayMilliseconds">The delay milliseconds before the message is available in the queue.</param>
        public void PublishDelayed<T> (T msg, int delayMilliseconds)
        {
            PublishMessage (msg, delayedExchange, QueueName, delayMilliseconds);
        }

        /// <summary>
        /// Publishes a message to the queue using the QueueName as the routingKey.
        /// </summary>
        /// <typeparam name="T">The type of the message object.</typeparam>
        /// <param name="msg">The message.</param>
        /// <param name="exchange">[Optional] The exchange name.</param>
        /// <param name="delayMilliseconds">[Optional] The delay milliseconds before the message is available in the queue.</param>
        private void PublishMessage<T> (T msg, string exchange, string queueName, int delayMilliseconds)
        {
            // set persistent property
            IBasicProperties basicProperties = null;
            if (_publishPersistent)
            {
                if (basicProperties == null) basicProperties = ConsumerChannel.CreateBasicProperties ();
                basicProperties.Persistent = _publishPersistent;
            }

            if (_retryDelayMilliseconds > 0)
            {
                if (basicProperties == null) basicProperties = ConsumerChannel.CreateBasicProperties ();
                basicProperties.AddHeader ("x-delay", _retryDelayMilliseconds);
            }

            // publish message            
            PublishChannel.BasicPublish (exchange: exchange ?? "",
                    routingKey: queueName,
                    basicProperties: basicProperties,
                    body: GetMessageContent (msg));
        }

        /// <summary>
        /// Get messages from the queue.
        /// </summary>
        /// <param name="timeout">The wait timeout for a message to arrive.</param>
        /// <param name="noAck">If the acknowledgement will be manual (noAck == false) or automatic (true).</param>
        public IEnumerable<RabbitWorkMessage> Get (TimeSpan timeout, bool noAck = false)
        {
            return Get ((int)timeout.TotalMilliseconds, noAck);
        }

        /// <summary>
        /// Get messages from the queue.
        /// </summary>
        /// <param name="millisecondsTimeout">The wait timeout in milliseconds for a message to arrive. -1 for infinite timeout</param>
        /// <param name="noAck">If the acknowledgement will be manual (noAck == false) or automatic (true).</param>
        public IEnumerable<RabbitWorkMessage> Get (int millisecondsTimeout, bool noAck = false)
        {
            RabbitMQ.Client.Events.BasicDeliverEventArgs item;
            bool done = false;
            while (!done)
            {
                done = true;
                using (var sub = new RabbitMQ.Client.MessagePatterns.Subscription (ConsumerChannel, QueueName, noAck))
                { 
                    while (sub.Next (millisecondsTimeout, out item))
                    {
                        if (item == null)
                        {
                            // in case of a connection close, the item will be null and we should re-subscribe
                            done = false;
                            break;                            
                        }

                        // if the message is within the retry threshold, proceed...
                        var msg = new RabbitWorkMessage (this, item);
                        if (CheckRetryLimit (_maxRetry, msg))
                            yield return msg;
                    }
                }
            }
        }

        private bool CheckRetryLimit (long retryLimit, RabbitWorkMessage msg)
        {
            IBasicProperties properties = msg.BasicProperties;
            if (retryLimit <= 0)
                return true;
            
            // check dead-letter counter (number of times the message was dlx)
            long count = msg.GetRetryCount ();            

            // check dlx count against our threshold
            if (count >= retryLimit)
            {
                // move message to dead-letter queue
                if (String.IsNullOrEmpty (deadLetterQueue))
                {
                    // create dead letter queue
                    lock (QueueName)
                    {
                        deadLetterQueue = QueueName + ".dead-letter";
                        deadLetterQueue = EnsureQueueExists (deadLetterQueue, Mode);
                    }
                }
                // publish message to the deadletter queue
                PublishChannel.BasicPublish ("", deadLetterQueue, (IBasicProperties)properties.Clone (), msg.Body);
                // delete message
                Ack (msg);
                return false;
            }
            return true;
        }

        /// <summary>
        /// Request an individual message from the queue. Returns null if queue is empty.
        /// </summary>
        /// <param name="noAck">If the acknowledgement will be manual (noAck == false) or automatic (true).</param>
        public RabbitWorkMessage GetOne (bool noAck = false)
        {            
            var item = ConsumerChannel.BasicGet (QueueName, noAck);
            return item != null ? new RabbitWorkMessage (this, item) : null;
        }

        /// <summary>
        /// Acknowledge the message arrival and processing, so that RabbitMq can remove it from the queue.
        /// </summary>
        /// <param name="msg">The message instance</param>
        public void Ack (RabbitWorkMessage msg)
        {
            if (ConsumerChannel.IsOpen) ConsumerChannel.BasicAck (msg.DeliveryTag, false);
        }

        /// <summary>
        /// Negative acknowledge the message, the message will be marked as a try and send to dead letter exchange according to the maxRetry threshold.
        /// </summary>
        /// <param name="msg">The message instance</param>
        public void Nack (RabbitWorkMessage msg)
        {
            Nack (msg, false);
        }

        /// <summary>
        /// Rejects the message and returns it to the queue. This does not count against the maxRetry counter.
        /// </summary>
        /// <param name="msg">The message instance</param>
        public void Requeue (RabbitWorkMessage msg)
        {
            Nack (msg, true);
        }

        /// <summary>
        /// Negative acknowledge the message, the message will be returned to the queue or send to dead letter exchange.
        /// </summary>
        /// <param name="msg">The message instance</param>
        /// <param name="requeue">False will send to dead letter exchange, true will send back to the queue.</param>
        private void Nack (RabbitWorkMessage msg, bool requeue = false)
        {
            if (ConsumerChannel.IsOpen)
            {
                // check max retry limit
                if (!requeue && !CheckRetryLimit (_maxRetry - 1, msg))
                    return;
                ConsumerChannel.BasicNack (msg.DeliveryTag, false, requeue);
            }
        }

        /// <summary>
        /// Closes this instance channel to rabbitmq.
        /// </summary>
        public void Dispose ()
        {
            Close ();
        }

        /// <summary>
        /// Closes this instance channel to rabbitmq.
        /// </summary>
        public void Close ()
        {
            if (ConsumerChannel.IsOpen)
            {
                // set auto close to true, so that it will close if no other channel is active
                if (Connection.IsOpen)
                    Connection.AutoClose = true;
                ConsumerChannel.Close ();
            }
        }

        public static byte[] GetMessageContent<T> (T msg)
        {
            string str = msg is string ? msg as string : Jil.JSON.Serialize (msg, DefaultJilSettings);
            return Encoding.UTF8.GetBytes (str);
        }        

        private string EnsureQueueExists (string queueName, RabbitWorkQueueMode mode, IDictionary<string, object> queueArgs = null)
        {
            queueName = queueName ?? "";

            using (var ch = CreateChannel ())
            {                
                // try to create queue
                try
                {
                    var res = ch.QueueDeclare (queue: queueName ?? "",
                        durable: mode == RabbitWorkQueueMode.OpenOrCreatePersistent,
                        exclusive: false,
                        autoDelete: mode == RabbitWorkQueueMode.OpenOrCreateTemporary,
                        arguments: queueArgs);

                    // set queue name if a random one was generated
                    if (String.IsNullOrEmpty (queueName) && res != null)
                        queueName = res.QueueName;
                }
                catch { }
            }
            return queueName;
        }

        private IDictionary<string, object> CreateProperty (string key, object value)
        {
            var headers = new Dictionary<string, object> (StringComparer.Ordinal);
            headers.Add (key, value);
            return headers;
        }

        private bool SafeExecute (Action<IModel> action, ushort retry = 1, int delayMilliseconds = 0, bool silentExceptions = true)
        {
            if (retry < 0)
                retry = 1;
            for (var i = 0; i < retry; i++)
            {
                try
                {
                    using (var channel = CreateChannel ())
                        action (channel);
                    return true;
                }
                catch
                {                    
                    if (i == retry - 1)
                    {
                        if (!silentExceptions)
                            throw;
                        return false;
                    }
                    Task.Delay (delayMilliseconds).Wait ();
                }
            }
            return true;
        }
    }

    /// <summary>
    /// How to handle queue creation and message publishing
    /// </summary>
    public enum RabbitWorkQueueMode
    {
        /// <summary>
        /// Open or creates fast in-memory durable queue and publishes messages in-memory mode.<para/>
        /// Also ensures that a delayed exchange exists.<para/>
        /// Since RabbitMQ will keep the messages in-memory, this mode is faster but the messages will be lost in case of a server restart or crash.
        /// </summary>
        OpenOrCreateInMemory,
        /// <summary>
        /// Open or creates a durable queue and publishes messages with persistence (slower since every message is written to disk).<para/>
        /// Also ensures that a delayed exchange exists.<para/>
        /// Since RabbitMQ will keep the messages in-memory, this mode is slower but safer in case of a server restart or crash.
        /// </summary>
        OpenOrCreatePersistent,
        /// <summary>
        /// Open or creates a temporary queue and publishes messages as transient (in-memory) that will be deleted after all consumers disconnects.<para/>
        /// Also ensures that a delayed exchange exists.<para/>
        /// Since RabbitMQ will keep the messages in-memory, this mode is faster but the messages will be lost in case of a server restart or crash.
        /// </summary>
        OpenOrCreateTemporary,        
        /// <summary>
        /// All messages will be published in-memory but the queue will not be created if not exists.<para/>
        /// Since RabbitMQ will keep the messages in-memory, this mode is faster.
        /// </summary>
        OpenInMemory,
        /// <summary>
        /// All messages will be published as persistent (disk) but the queue will not be created if not exits.<para/>
        /// Since RabbitMQ will keep the messages in-memory, this mode is slower but safer in case of a server restart or crash.
        /// </summary>
        OpenPersistent
    }

    public class RabbitConnectionFactory :  IDisposable
    {        
        static ConcurrentDictionary<string, Tuple<ConnectionFactory, IConnection>> ConnectionMap = new ConcurrentDictionary<string, Tuple<ConnectionFactory, IConnection>> (StringComparer.OrdinalIgnoreCase);
        
        public string Address { get; private set; }
        public int Port { get; private set; }
        public string Login { get; private set; }
        public string Password { get; private set; }

        IConnection _connection;

        public RabbitConnectionFactory (string address, int port, string login, string password)
        {
            Address = address;
            Port = port;
            Login = login;
            Password = password;            
        }

        public IConnection GetConnection ()
        {
            if (_connection == null || !_connection.IsOpen)
            {
                try
                {
                    _connection.Dispose ();
                } catch {}
                _connection = GetConnection (Address, Port, Login, Password);
            }
            return _connection;
        }

        public void Close()
        {            
            if (_connection != null)
            {
                // set auto close to true, so that it will close if no other channel is active
                if (_connection.IsOpen)            
                    _connection.AutoClose = true;
            }
            _connection = null;
        }

        public void Dispose ()
        {
            Close ();
        }

        /// <summary>
        /// Get a lightweight connection (channel) with a RabbitMq server
        /// </summary>
        /// <param name="address">rabbitMq address or dns</param>
        /// <param name="port"><rabbitMq server port/param>
        /// <param name="login"></param>
        /// <param name="password"></param>
        /// <returns></returns>
        public IConnection GetConnection (string address, int port, string login, string password, string vhost = null)
        {
            // get connection or create
            string connKey = address + port + login;
            Tuple<ConnectionFactory, IConnection> tuple;
            IConnection connection = null;
            // get from cache
            if (ConnectionMap.TryGetValue (connKey, out tuple))
                connection = tuple.Item2;
            // check connection status
            if (connection == null || !connection.IsOpen)
            {
                int tryCount = 0;
                // lock to avoid creating multiple connection to the same host
                lock (ConnectionMap)
                {
                    ConnectionFactory factory = null;
                    // check again if we have a connection
                    if (ConnectionMap.TryGetValue (connKey, out tuple))
                    {
                        factory = tuple.Item1;
                        connection = tuple.Item2;
                    }
                    // try to connect
                    while (connection == null || !connection.IsOpen)
                    {
                        try
                        {
                            if (factory == null)
                            {
                                factory = new ConnectionFactory ();
                                factory.AutomaticRecoveryEnabled = true;
                                factory.HostName = address;
                                factory.Port = port <= 0 ? 5672 : port;
                                factory.UserName = login;
                                factory.Password = password;
                                factory.NetworkRecoveryInterval = TimeSpan.FromSeconds (15); // defaults to 5s
                                factory.RequestedHeartbeat = 60;
                                factory.VirtualHost = vhost ?? factory.VirtualHost;
                            }

                            // open connection
                            connection = factory.CreateConnection ();

                            // add it to connection map
                            ConnectionMap[connKey] = Tuple.Create (factory, connection);
                        }
                        catch (Exception ex)
                        {
                            connection = null;
                            // check for authentication errors
                            if (ex is RabbitMQ.Client.Exceptions.AuthenticationFailureException ||
                                ex.InnerException is RabbitMQ.Client.Exceptions.AuthenticationFailureException)
                                throw;
                            // try 5 times before giving up...
                            if (tryCount++ > 3)
                                throw;
                            // wait a while before trying again
                            if (ex is RabbitMQ.Client.Exceptions.ConnectFailureException ||
                                ex.InnerException is RabbitMQ.Client.Exceptions.ConnectFailureException)
                                Task.Delay (250).Wait ();
                        }
                    }
                }
            }

            // auto close must be called after channel creation...
            connection.AutoClose = false;
            return connection;
        }
    }

    /// <summary>
    /// RabbitMq message wrapper
    /// </summary>
    public class RabbitWorkMessage
    {
        RabbitWorkQueue _queue;
        string _content;
        RabbitMQ.Client.Events.BasicDeliverEventArgs _msg1; 
        RabbitMQ.Client.BasicGetResult _msg2;

        /// <summary>The content header of the message.</summary>
        public IBasicProperties BasicProperties { get { return _msg1 != null ? _msg1.BasicProperties : _msg2.BasicProperties; } }

        /// <summary>
        /// Retrieves the body of this message. Use Get () or GetAs () to get the converted message content.
        /// </summary>
        public byte[] Body { get { return _msg1 != null ? _msg1.Body : _msg2.Body; } }

        /// <summary>
        /// The consumer tag of the consumer that the message was delivered to. RabbitWorkQueue.Get call.
        /// </summary>
        public string ConsumerTag { get { return _msg1 != null ? _msg1.ConsumerTag : null; } }

        /// <summary>
        /// Retrieve the delivery tag for this message. Used to acknowledge a message delivery.
        /// </summary>
        public ulong DeliveryTag
        {
            get
            {
                // since the delivery tag may change after a connection reset
                // we must acquire it from the original message object
                return _msg1 != null ? _msg1.DeliveryTag : _msg2.DeliveryTag;
            }
        }

        /// <summary>
        /// Retrieve the exchange this message was published to.
        /// </summary>
        public string Exchange { get { return _msg1 != null ? _msg1.Exchange : _msg2.Exchange; } }

        /// <summary>
        /// Retrieve the redelivered flag for this message.
        /// </summary>
        public bool Redelivered
        {
            get { return _msg1 != null ? _msg1.Redelivered : _msg2.Redelivered; }
        }
        
        /// <summary>
        /// Retrieve the routing key with which this message was published.
        /// </summary>
        public string RoutingKey
        {
            get { return _msg1 != null ? _msg1.RoutingKey : _msg2.RoutingKey; }
        }
        
        /// <summary>
        /// Number of messages pending on the queue. Only populated on RabbitWorkQueue.GetOne call.
        /// </summary>
        public uint MessageCount
        {
            get { return _msg1 != null ? 0 : _msg2.MessageCount; }
        }

        /// <summary>
        /// The message body as string.
        /// </summary>
        public string Content
        {
            get { return Get (); }
        }

        public long RetryCount
        {
            get { return GetRetryCount ();  }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RabbitWorkMessage" /> class.
        /// </summary>
        public RabbitWorkMessage ()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RabbitWorkMessage" /> class.
        /// </summary>
        /// <param name="queue">The queue.</param>
        /// <param name="evt">The <see cref="RabbitMQ.Client.Events.BasicDeliverEventArgs" /> instance containing the event data.</param>
        public RabbitWorkMessage (RabbitWorkQueue queue, RabbitMQ.Client.Events.BasicDeliverEventArgs evt)
        {
            _queue = queue;
            _msg1 = evt;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="RabbitWorkMessage" /> class.
        /// </summary>
        /// <param name="queue">The queue.</param>
        /// <param name="evt">The evt.</param>
        public RabbitWorkMessage (RabbitWorkQueue queue, RabbitMQ.Client.BasicGetResult evt)
        {
            _queue = queue;
            _msg2 = evt;
        }

        /// <summary>
        /// Gets the message body as string. Same as the property Content.
        /// </summary>
        public string Get ()
        {
            if (_content == null)
                _content = Encoding.UTF8.GetString (Body);
            return _content;
        }
        
        /// <summary>
        /// Deserialize the message to the desired type.
        /// </summary>
        /// <typeparam name="T">The type to deserialize the json formated message</typeparam>
        public T GetAs<T> ()
        {
            return Jil.JSON.Deserialize<T> (Get (), RabbitWorkQueue.DefaultJilSettings);
        }

        /// <summary>
        /// Acknowledge the message arrival and processing, so that RabbitMq can remove it from the queue.
        /// </summary>
        public void Ack ()
        {
            _queue.Ack (this);
        }

        /// <summary>
        /// Negative acknowledge the message, the message will be marked as a try and send to dead letter exchange according to the maxRetry threshold.
        /// </summary>
        public void Nack ()
        {
            _queue.Nack (this);
        }

        /// <summary>
        /// Rejects the message and returns it to the queue. This does not count against the maxRetry counter.
        /// </summary>
        public void Requeue ()
        {
            _queue.Requeue (this);
        }

        /// <summary>
        /// Gets the number of times this message were negative acknowledged.
        /// </summary>
        /// <returns></returns>
        public long GetRetryCount ()
        {
            IBasicProperties properties = BasicProperties;
            if (properties == null)
                return 0;
            // try get dead-letter header info
            var headers = properties.Headers;
            object value;
            if (headers == null || !headers.TryGetValue ("x-death", out value))
                return 0;

            // check dead-letter counter (number of times the message was dlx)
            long count = 0;
            IList<object> deadLetter = value as IList<object>;
            if (deadLetter != null && deadLetter.Count > 0)
            {
                var map = deadLetter[0] as IDictionary<string, object>;
                if (map != null && map.TryGetValue ("count", out value))
                    count = (long)value;
            }
            return count;
        }
    }

    public static class RabbitExtensions
    {
        public static void AddHeader (this IBasicProperties property, string key, object value)
        {
            if (property != null)
            {
                if (property.Headers == null)
                    property.Headers = new Dictionary<string, object> (StringComparer.Ordinal);
                property.Headers[key] = value;                
            }
        }

        public static IDictionary<string, object> AddProperty (this IDictionary<string, object> headers, string key, object value)
        {
            if (headers == null) headers = new Dictionary<string, object> (StringComparer.Ordinal);
            headers[key] = value;
            return headers;
        }
    }
}
