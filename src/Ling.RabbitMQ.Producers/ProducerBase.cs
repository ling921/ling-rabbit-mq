using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Ling.RabbitMQ.Producers;

public abstract class ProducerBase : RabbitMQServiceBase
{
    protected ProducerBase(IConnection connection) : base(connection)
    {
    }

    protected ProducerBase(RabbitMQOptions connectionConfig) : base(connectionConfig)
    {
    }

    protected ProducerBase(RabbitMQOptions connectionConfig, IMessageSerializer serializer)
        : base(connectionConfig, serializer)
    {
    }

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
