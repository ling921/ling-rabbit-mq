using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Ling.RabbitMQ.Producers;

/// <summary>
/// Interface for a service that handles work queues in RabbitMQ.
/// </summary>
public interface IWorkQueueProducer
{
    /// <summary>
    /// Enqueues a message to the specified queue.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="queue">The queue name.</param>
    /// <param name="message">The message to enqueue.</param>
    /// <param name="configureProperties">Optional action to configure message properties.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous enqueue operation.</returns>
    Task InvokeAsync<T>(
        string queue,
        T message,
        bool durable = true,
        Action<IBasicProperties>? configureProperties = null,
        CancellationToken cancellationToken = default)
        where T : class;
}

/// <summary>
/// Implementation of <see cref="IWorkQueueProducer"/> that handles work queues in RabbitMQ.
/// </summary>
public class WorkQueueProducer : ProducerBase, IWorkQueueProducer
{
    public WorkQueueProducer(IConnection connection) : base(connection)
    {
    }

    public WorkQueueProducer(RabbitMQOptions connectionConfig) : base(connectionConfig)
    {
    }

    public WorkQueueProducer(RabbitMQOptions connectionConfig, IMessageSerializer serializer) : base(connectionConfig, serializer)
    {
    }

    public WorkQueueProducer(ILoggerFactory loggerFactory, RabbitMQOptions connectionConfig, IMessageSerializer serializer) : base(loggerFactory, connectionConfig, serializer)
    {
    }

    /// <inheritdoc />
    public virtual async Task InvokeAsync<T>(
        string queue,
        T message,
        bool durable = true,
        Action<IBasicProperties>? configureProperties = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        await InitializeAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false);

        try
        {
            await Channel.QueueDeclareAsync(
                queue: queue,
                durable: durable,
                exclusive: false,
                autoDelete: false,
                arguments: null,
                passive: false,
                noWait: false,
                cancellationToken: cancellationToken);

            var properties = CreateBasicProperties<T>();
            configureProperties?.Invoke(properties);

            var body = Serializer.Serialize(message);
            await Channel.BasicPublishAsync(
                exchange: string.Empty,
                routingKey: queue,
                mandatory: true,
                basicProperties: properties,
                body: body,
                cancellationToken: cancellationToken);

            Logger.LogInformation("Message enqueued to {Queue} with message: {@message}", queue, message);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to enqueue message to {Queue} with message: {@message}", queue, message);
            throw;
        }
    }
}
