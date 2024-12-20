using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Ling.RabbitMQ;

/// <summary>
/// Interface for a service that handles dead letter queues in RabbitMQ.
/// </summary>
public interface IDeadLetterService
{
    /// <summary>
    /// Sets up a dead letter queue for the specified original queue.
    /// </summary>
    /// <param name="originalQueue">The original queue name.</param>
    /// <param name="deadLetterExchange">The dead letter exchange name.</param>
    /// <param name="deadLetterQueue">The dead letter queue name.</param>
    /// <param name="deadLetterRoutingKey">The dead letter routing key.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous setup operation.</returns>
    Task SetupDeadLetterQueueAsync(
        string originalQueue,
        string deadLetterExchange,
        string deadLetterQueue,
        string deadLetterRoutingKey,
        CancellationToken cancellationToken = default);

    /// <summary>
    /// Subscribes to the dead letter queue and processes messages using the specified handler.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="deadLetterQueue">The dead letter queue name.</param>
    /// <param name="handler">The handler to process the message.</param>
    /// <param name="requeue">Whether to requeue the message in case of an error.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous subscribe operation.</returns>
    Task SubscribeDeadLetterQueueAsync<T>(
        string deadLetterQueue,
        Func<T?, string, CancellationToken, Task> handler,
        bool requeue = false,
        CancellationToken cancellationToken = default)
        where T : class;
}

/// <summary>
/// Implementation of <see cref="IDeadLetterService"/> that handles dead letter queues in RabbitMQ.
/// </summary>
public class DeadLetterService : RabbitMQServiceBase, IDeadLetterService
{
    /// <summary>
    /// Initializes a new instance of the <see cref="DeadLetterService"/> class with the specified connection.
    /// </summary>
    /// <param name="connection">The RabbitMQ connection.</param>
    public DeadLetterService(RabbitMQConnection connection) : base(connection) { }

    /// <summary>
    /// Initializes a new instance of the <see cref="DeadLetterService"/> class with the specified connection and logger factory.
    /// </summary>
    /// <param name="connection">The RabbitMQ connection.</param>
    /// <param name="loggerFactory">The logger factory.</param>
    public DeadLetterService(RabbitMQConnection connection, ILoggerFactory loggerFactory)
        : base(connection, loggerFactory) { }

    /// <summary>
    /// Sets up a dead letter queue for the specified original queue.
    /// </summary>
    /// <param name="originalQueue">The original queue name.</param>
    /// <param name="deadLetterExchange">The dead letter exchange name.</param>
    /// <param name="deadLetterQueue">The dead letter queue name.</param>
    /// <param name="deadLetterRoutingKey">The dead letter routing key.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous setup operation.</returns>
    public async Task SetupDeadLetterQueueAsync(
        string originalQueue,
        string deadLetterExchange,
        string deadLetterQueue,
        string deadLetterRoutingKey,
        CancellationToken cancellationToken = default)
    {
        try
        {
            await ConnectAsync(cancellationToken);

            await DeclareExchangeAsync(deadLetterExchange, ExchangeType.Direct, true, cancellationToken);

            await DeclareQueueAsync(deadLetterQueue, true, null, cancellationToken);

            await Channel.QueueBindAsync(
                deadLetterQueue,
                deadLetterExchange,
                deadLetterRoutingKey,
                cancellationToken: cancellationToken);

            var arguments = new Dictionary<string, object?>
            {
                { "x-dead-letter-exchange", deadLetterExchange },
                { "x-dead-letter-routing-key", deadLetterRoutingKey }
            };

            await DeclareQueueAsync(originalQueue, true, arguments, cancellationToken);

            Logger.LogInformation(
                "Dead letter queue setup completed. Original queue: {OriginalQueue}, " +
                "DLX: {DeadLetterExchange}, DLQ: {DeadLetterQueue}",
                originalQueue, deadLetterExchange, deadLetterQueue);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to setup dead letter queue");
            throw;
        }
    }

    /// <summary>
    /// Subscribes to the dead letter queue and processes messages using the specified handler.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="deadLetterQueue">The dead letter queue name.</param>
    /// <param name="handler">The handler to process the message.</param>
    /// <param name="requeue">Whether to requeue the message in case of an error.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous subscribe operation.</returns>
    public async Task SubscribeDeadLetterQueueAsync<T>(
        string deadLetterQueue,
        Func<T?, string, CancellationToken, Task> handler,
        bool requeue = false,
        CancellationToken cancellationToken = default)
        where T : class
    {
        try
        {
            await ConnectAsync(cancellationToken);

            var consumer = new AsyncEventingBasicConsumer(Channel);
            consumer.ReceivedAsync += async (_, ea) =>
            {
                try
                {
                    var message = Deserialize<T>(ea.Body);
                    var originalRoutingKey = ea.BasicProperties.Headers?
                        .TryGetValue("x-death", out var xDeath) == true
                        ? (xDeath as List<object>)?.FirstOrDefault() is Dictionary<string, object> death
                            ? death.TryGetValue("routing-keys", out var routingKeys)
                                ? (routingKeys as List<object>)?.FirstOrDefault()?.ToString()
                                : null
                            : null
                        : null;

                    Logger.LogInformation(
                        "Received dead letter message from queue {Queue} with original routing key {RoutingKey}, message {message}",
                        deadLetterQueue, originalRoutingKey, message);

                    await handler(message, originalRoutingKey ?? string.Empty, ea.CancellationToken);
                    await Channel.BasicAckAsync(ea.DeliveryTag, false, ea.CancellationToken);

                    Logger.LogInformation(
                        "Successfully processed dead letter message from queue {Queue}",
                        deadLetterQueue);
                }
                catch (Exception ex)
                {
                    Logger.LogError(ex,
                        "Error processing dead letter message from queue {Queue}",
                        deadLetterQueue);
                    await Channel.BasicNackAsync(ea.DeliveryTag, false, requeue, ea.CancellationToken);
                }
            };

            await Channel.BasicConsumeAsync(deadLetterQueue, false, consumer, cancellationToken);

            Logger.LogInformation("Subscribed to dead letter queue {Queue}", deadLetterQueue);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to subscribe to dead letter queue {Queue}", deadLetterQueue);
            throw;
        }
    }
}
