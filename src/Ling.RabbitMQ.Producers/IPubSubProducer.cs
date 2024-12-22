using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Ling.RabbitMQ.Producers;

/// <summary>
/// Interface for a service that handles publish-subscribe pattern in RabbitMQ.
/// </summary>
public interface IPubSubProducer
{
    /// <summary>
    /// Publishes a message to the specified exchange.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="exchange">The exchange name.</param>
    /// <param name="message">The message to publish.</param>
    /// <param name="configureProperties">Optional action to configure message properties.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous publish operation.</returns>
    Task InvokeAsync<T>(
        string exchange,
        T message,
        bool durable = true,
        Action<IBasicProperties>? configureProperties = null,
        CancellationToken cancellationToken = default)
        where T : class;
}

/// <summary>
/// Implementation of <see cref="IPubSubProducer"/> that handles publish-subscribe pattern in RabbitMQ.
/// </summary>
public class PubSubProducer : ProducerBase, IPubSubProducer
{
    public PubSubProducer(IConnection connection) : base(connection)
    {
    }

    public PubSubProducer(RabbitMQOptions connectionConfig) : base(connectionConfig)
    {
    }

    public PubSubProducer(RabbitMQOptions connectionConfig, IMessageSerializer serializer) : base(connectionConfig, serializer)
    {
    }

    public PubSubProducer(ILoggerFactory loggerFactory, RabbitMQOptions connectionConfig, IMessageSerializer serializer) : base(loggerFactory, connectionConfig, serializer)
    {
    }

    /// <inheritdoc />
    public virtual async Task InvokeAsync<T>(
        string exchange,
        T message,
        bool durable = true,
        Action<IBasicProperties>? configureProperties = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        await InitializeAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false);

        try
        {
            await Channel.ExchangeDeclareAsync(
                exchange: exchange,
                type: ExchangeType.Fanout,
                durable: true,
                autoDelete: false,
                arguments: null,
                noWait: false,
                cancellationToken: cancellationToken);

            var properties = CreateBasicProperties<T>();
            configureProperties?.Invoke(properties);

            var body = Serializer.Serialize(message);
            await Channel.BasicPublishAsync(
                exchange: exchange,
                routingKey: string.Empty,
                mandatory: false,
                basicProperties: properties,
                body: body,
                cancellationToken: cancellationToken);

            Logger.LogInformation("Message published to {Exchange} with message: {@message}", exchange, message);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to publish message to {Exchange} with message: {@message}", exchange, message);
            throw;
        }
    }
}
