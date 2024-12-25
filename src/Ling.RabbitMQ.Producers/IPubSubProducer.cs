using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Ling.RabbitMQ.Producers;

/// <summary>
/// Interface for a service that handles the publish-subscribe pattern in RabbitMQ.
/// </summary>
public interface IPubSubProducer
{
    /// <summary>
    /// Publishes a message to the specified exchange.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="exchange">The exchange name.</param>
    /// <param name="message">The message to publish.</param>
    /// <param name="durable">Indicates whether the message should be durable.</param>
    /// <param name="configureProperties">Optional action to configure message properties.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous publish operation.</returns>
    Task InvokeAsync<T>(
        string exchange,
        T message,
        bool durable = true,
        Action<IBasicProperties>? configureProperties = null,
        CancellationToken cancellationToken = default);
}

/// <summary>
/// Implementation of <see cref="IPubSubProducer"/> that handles the publish-subscribe pattern in RabbitMQ.
/// </summary>
public class PubSubProducer : ProducerBase, IPubSubProducer
{
    /// <summary>
    /// Initializes a new instance of the <see cref="PubSubProducer"/> class with the specified connection.
    /// </summary>
    /// <param name="connection">The RabbitMQ connection.</param>
    public PubSubProducer(IConnection connection) : base(connection)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="PubSubProducer"/> class with the specified connection, message serializer, and logger factory.
    /// </summary>
    /// <param name="connection">The RabbitMQ connection.</param>
    /// <param name="serializer">The message serializer.</param>
    /// <param name="loggerFactory">The logger factory.</param>
    public PubSubProducer(IConnection connection, IMessageSerializer serializer, ILoggerFactory loggerFactory) : base(connection, serializer, loggerFactory)
    {
    }

    /// <inheritdoc />
    public virtual async Task InvokeAsync<T>(
        string exchange,
        T message,
        bool durable = true,
        Action<IBasicProperties>? configureProperties = null,
        CancellationToken cancellationToken = default)
    {
        await InitializeAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false);

        try
        {
            await Channel.ExchangeDeclareAsync(
                exchange: exchange,
                type: ExchangeType.Fanout,
                durable: durable,
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

            Logger.LogInformation("Message published to exchange '{Exchange}' with message: {@Message}", exchange, message);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to publish message to exchange '{Exchange}' with message: {@Message}", exchange, message);
            throw;
        }
    }
}
