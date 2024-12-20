using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Ling.RabbitMQ;

/// <summary>
/// Interface for a service that handles publish-subscribe pattern in RabbitMQ.
/// </summary>
public interface IPubSubService
{
    /// <summary>
    /// Publishes a message to the specified exchange.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="exchange">The exchange name.</param>
    /// <param name="message">The message to publish.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous publish operation.</returns>
    Task PublishAsync<T>(
        string exchange,
        T message,
        CancellationToken cancellationToken = default)
        where T : class;

    /// <summary>
    /// Subscribes to the specified exchange and queue, and processes messages using the given handler.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="exchange">The exchange name.</param>
    /// <param name="queue">The queue name.</param>
    /// <param name="handler">The handler to process the message.</param>
    /// <param name="requeue">Whether to requeue the message in case of an error.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous subscribe operation.</returns>
    Task SubscribeAsync<T>(
        string exchange,
        string queue,
        Func<T?, CancellationToken, Task> handler,
        bool requeue = false,
        CancellationToken cancellationToken = default)
        where T : class;
}

/// <summary>
/// Implementation of <see cref="IPubSubService"/> that handles publish-subscribe pattern in RabbitMQ.
/// </summary>
public class PubSubService : RabbitMQServiceBase, IPubSubService
{
    /// <summary>
    /// Initializes a new instance of the <see cref="PubSubService"/> class with the specified connection.
    /// </summary>
    /// <param name="connection">The RabbitMQ connection.</param>
    public PubSubService(RabbitMQConnection connection) : base(connection)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="PubSubService"/> class with the specified connection and logger factory.
    /// </summary>
    /// <param name="connection">The RabbitMQ connection.</param>
    /// <param name="loggerFactory">The logger factory.</param>
    public PubSubService(RabbitMQConnection connection, ILoggerFactory loggerFactory) : base(connection, loggerFactory)
    {
    }

    /// <summary>
    /// Publishes a message to the specified exchange.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="exchange">The exchange name.</param>
    /// <param name="message">The message to publish.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous publish operation.</returns>
    public async Task PublishAsync<T>(
        string exchange,
        T message,
        CancellationToken cancellationToken = default)
        where T : class
    {
        try
        {
            await ConnectAsync(cancellationToken);

            await DeclareExchangeAsync(exchange, ExchangeType.Fanout, true, cancellationToken);

            var body = Serialize(message);
            await Channel.BasicPublishAsync(
                exchange: exchange,
                routingKey: string.Empty,
                mandatory: false,
                DefaultProperties,
                body,
                cancellationToken);

            Logger.LogInformation("Message published to {Exchange} with message: {@message}", exchange, message);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to publish message to {Exchange} with message: {@message}", exchange, message);
            throw;
        }
    }

    /// <summary>
    /// Subscribes to the specified exchange and queue, and processes messages using the given handler.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="exchange">The exchange name.</param>
    /// <param name="queue">The queue name.</param>
    /// <param name="handler">The handler to process the message.</param>
    /// <param name="requeue">Whether to requeue the message in case of an error.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous subscribe operation.</returns>
    public async Task SubscribeAsync<T>(
        string exchange,
        string queue,
        Func<T?, CancellationToken, Task> handler,
        bool requeue = false,
        CancellationToken cancellationToken = default)
        where T : class
    {
        try
        {
            await ConnectAsync(cancellationToken);

            await DeclareExchangeAsync(exchange, ExchangeType.Fanout, true, cancellationToken);
            await DeclareQueueAsync(queue, true, null, cancellationToken);

            await Channel.QueueBindAsync(queue, exchange, string.Empty, cancellationToken: cancellationToken);

            var consumer = CreateConsumer(handler, requeue);

            await Channel.BasicConsumeAsync(queue, false, consumer, cancellationToken);
            Logger.LogInformation("Subscribed to {Exchange} with queue {Queue}", exchange, queue);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to subscribe to {Exchange}", exchange);
            throw;
        }
    }
}
