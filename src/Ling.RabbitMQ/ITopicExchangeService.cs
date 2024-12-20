using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Ling.RabbitMQ;

/// <summary>
/// Interface for a service that handles topic exchanges in RabbitMQ.
/// </summary>
public interface ITopicExchangeService
{
    /// <summary>
    /// Publishes a message to the specified topic exchange with the given topic.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="exchange">The exchange name.</param>
    /// <param name="topic">The topic.</param>
    /// <param name="message">The message to publish.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous publish operation.</returns>
    Task PublishAsync<T>(
        string exchange,
        string topic,
        T message,
        CancellationToken cancellationToken = default)
        where T : class;

    /// <summary>
    /// Subscribes to the specified topic exchange and queue, and processes messages using the given handler.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="exchange">The exchange name.</param>
    /// <param name="queue">The queue name.</param>
    /// <param name="topicPattern">The topic pattern.</param>
    /// <param name="handler">The handler to process the message.</param>
    /// <param name="requeue">Whether to requeue the message in case of an error.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous subscribe operation.</returns>
    Task SubscribeAsync<T>(
        string exchange,
        string queue,
        string topicPattern,
        Func<T?, CancellationToken, Task> handler,
        bool requeue = false,
        CancellationToken cancellationToken = default)
        where T : class;
}

/// <summary>
/// Implementation of <see cref="ITopicExchangeService"/> that handles topic exchanges in RabbitMQ.
/// </summary>
public class TopicExchangeService : RabbitMQServiceBase, ITopicExchangeService
{
    /// <summary>
    /// Initializes a new instance of the <see cref="TopicExchangeService"/> class with the specified connection.
    /// </summary>
    /// <param name="connection">The RabbitMQ connection.</param>
    public TopicExchangeService(RabbitMQConnection connection) : base(connection)
    {
        DefaultProperties.Persistent = true;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="TopicExchangeService"/> class with the specified connection and logger factory.
    /// </summary>
    /// <param name="connection">The RabbitMQ connection.</param>
    /// <param name="loggerFactory">The logger factory.</param>
    public TopicExchangeService(RabbitMQConnection connection, ILoggerFactory loggerFactory)
        : base(connection, loggerFactory)
    {
        DefaultProperties.Persistent = true;
    }

    /// <summary>
    /// Publishes a message to the specified topic exchange with the given topic.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="exchange">The exchange name.</param>
    /// <param name="topic">The topic.</param>
    /// <param name="message">The message to publish.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous publish operation.</returns>
    public async Task PublishAsync<T>(
        string exchange,
        string topic,
        T message,
        CancellationToken cancellationToken = default)
        where T : class
    {
        try
        {
            await ConnectAsync(cancellationToken);
            await DeclareExchangeAsync(exchange, ExchangeType.Topic, true, cancellationToken);

            var body = Serialize(message);
            await Channel.BasicPublishAsync(
                exchange: exchange,
                routingKey: topic,
                mandatory: true,
                basicProperties: DefaultProperties,
                body: body,
                cancellationToken: cancellationToken);

            Logger.LogInformation("Message published to topic exchange {Exchange} with topic {Topic}",
                exchange, topic);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to publish message to topic exchange {Exchange} with topic {Topic}",
                exchange, topic);
            throw;
        }
    }

    /// <summary>
    /// Subscribes to the specified topic exchange and queue, and processes messages using the given handler.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="exchange">The exchange name.</param>
    /// <param name="queue">The queue name.</param>
    /// <param name="topicPattern">The topic pattern.</param>
    /// <param name="handler">The handler to process the message.</param>
    /// <param name="requeue">Whether to requeue the message in case of an error.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous subscribe operation.</returns>
    public async Task SubscribeAsync<T>(
        string exchange,
        string queue,
        string topicPattern,
        Func<T?, CancellationToken, Task> handler,
        bool requeue = false,
        CancellationToken cancellationToken = default)
        where T : class
    {
        try
        {
            await ConnectAsync(cancellationToken);

            await DeclareExchangeAsync(exchange, ExchangeType.Topic, true, cancellationToken);

            await DeclareQueueAsync(queue, true, null, cancellationToken);

            await Channel.QueueBindAsync(
                queue: queue,
                exchange: exchange,
                routingKey: topicPattern,
                cancellationToken: cancellationToken);

            var consumer = CreateConsumer(handler, requeue);

            await Channel.BasicConsumeAsync(queue, false, consumer, cancellationToken);

            Logger.LogInformation(
                "Subscribed to topic exchange {Exchange} with queue {Queue} and pattern {Pattern}",
                exchange, queue, topicPattern);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex,
                "Failed to subscribe to topic exchange {Exchange} with queue {Queue} and pattern {Pattern}",
                exchange, queue, topicPattern);
            throw;
        }
    }
}
