using Microsoft.Extensions.Logging;
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
    /// Initializes a new instance of the <see cref="ProducerBase"/> class with the specified connection, message serializer, and logger factory.
    /// </summary>
    /// <param name="connection">The RabbitMQ connection.</param>
    /// <param name="serializer">The message serializer.</param>
    /// <param name="loggerFactory">The logger factory.</param>
    protected ProducerBase(IConnection connection, IMessageSerializer serializer, ILoggerFactory loggerFactory)
        : base(connection, serializer, loggerFactory)
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
