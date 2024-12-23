using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using RabbitMQ.Client;

namespace Ling.RabbitMQ.Producers;

/// <summary>
/// Provides a base class for RabbitMQ producers, handling connection and message publishing.
/// </summary>
public abstract class ProducerBase : RabbitMQServiceBase
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ProducerBase"/> class with the specified connection.
    /// </summary>
    /// <param name="connection">The RabbitMQ connection.</param>
    protected ProducerBase(IConnection connection) : base(connection)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ProducerBase"/> class with the specified connection configuration.
    /// </summary>
    /// <param name="connectionConfig">The RabbitMQ connection configuration.</param>
    protected ProducerBase(RabbitMQOptions connectionConfig) : base(connectionConfig)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ProducerBase"/> class with the specified connection configuration and message serializer.
    /// </summary>
    /// <param name="connectionConfig">The RabbitMQ connection configuration.</param>
    /// <param name="serializer">The message serializer.</param>
    protected ProducerBase(RabbitMQOptions connectionConfig, IMessageSerializer serializer)
        : base(NullLoggerFactory.Instance, connectionConfig, serializer)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ProducerBase"/> class with the specified logger factory, connection configuration, and message serializer.
    /// </summary>
    /// <param name="loggerFactory">The logger factory.</param>
    /// <param name="connectionConfig">The RabbitMQ connection configuration.</param>
    /// <param name="serializer">The message serializer.</param>
    protected ProducerBase(ILoggerFactory loggerFactory, RabbitMQOptions connectionConfig, IMessageSerializer serializer)
        : base(loggerFactory, connectionConfig, serializer)
    {
    }

    /// <summary>
    /// Creates basic properties for messages.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <returns>The created basic properties.</returns>
    protected virtual BasicProperties CreateBasicProperties<T>() => new()
    {
        Persistent = true,
        MessageId = Guid.NewGuid().ToString("N"),
        Timestamp = new AmqpTimestamp(DateTimeOffset.UtcNow.ToUnixTimeSeconds()),
        Type = typeof(T).FullName
    };
}
