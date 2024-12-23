using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace Ling.RabbitMQ.Consumers;

/// <summary>
/// Represents a base class for a RabbitMQ consumer that handles the publish-subscribe pattern.
/// </summary>
/// <typeparam name="TMessage">The type of the message.</typeparam>
public abstract class PubSubConsumer<TMessage> : RabbitMQConsumerBase<TMessage>
{
    /// <summary>
    /// Gets the name of the exchange.
    /// </summary>
    protected abstract string ExchangeName { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="PubSubConsumer{TMessage}"/> class.
    /// </summary>
    /// <param name="loggerFactory">The logger factory.</param>
    /// <param name="serializer">The message serializer.</param>
    /// <param name="options">The RabbitMQ options.</param>
    protected PubSubConsumer(ILoggerFactory loggerFactory, IMessageSerializer serializer, IOptions<RabbitMQOptions> options)
        : base(loggerFactory, serializer, options)
    {
    }

    /// <summary>
    /// Sets up the consumer asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous setup operation.</returns>
    protected override async Task SetupAsync(CancellationToken cancellationToken)
    {
        await InitializeAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false);

        try
        {
            await Channel.ExchangeDeclareAsync(
                exchange: ExchangeName,
                type: ExchangeType.Fanout,
                durable: true,
                autoDelete: false,
                arguments: null,
                noWait: false,
                cancellationToken: cancellationToken);

            // Declare a server-named queue
            var queueDeclareResult = await Channel.QueueDeclareAsync(cancellationToken: cancellationToken);
            var queueName = queueDeclareResult.QueueName;

            await ApplyQosAsync(Channel, cancellationToken);

            await Channel.QueueBindAsync(
                queue: queueName,
                exchange: ExchangeName,
                routingKey: string.Empty,
                arguments: null,
                noWait: false,
                cancellationToken: cancellationToken);

            var consumer = CreateConsumer(Channel);

            await Channel.BasicConsumeAsync(
                queue: queueName,
                autoAck: AutoAck,
                consumer: consumer,
                cancellationToken: cancellationToken);

            Logger.LogInformation("Successfully subscribed to exchange '{Exchange}' with queue '{Queue}'", ExchangeName, queueName);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to subscribe to exchange '{Exchange}'", ExchangeName);
            throw;
        }
    }
}
