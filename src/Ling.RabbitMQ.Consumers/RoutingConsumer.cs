using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace Ling.RabbitMQ.Consumers;

/// <summary>
/// Represents a base class for a RabbitMQ consumer that handles the routing pattern.
/// </summary>
/// <typeparam name="TMessage">The type of the message.</typeparam>
public abstract class RoutingConsumer<TMessage> : RabbitMQConsumerBase<TMessage>
{
    /// <summary>
    /// Gets the name of the exchange.
    /// </summary>
    protected abstract string ExchangeName { get; }

    /// <summary>
    /// Gets the routing keys.
    /// </summary>
    protected abstract string[] RoutingKeys { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="RoutingConsumer{TMessage}"/> class.
    /// </summary>
    /// <param name="loggerFactory">The logger factory.</param>
    /// <param name="serializer">The message serializer.</param>
    /// <param name="options">The RabbitMQ options.</param>
    protected RoutingConsumer(ILoggerFactory loggerFactory, IMessageSerializer serializer, IOptions<RabbitMQOptions> options)
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
        if (RoutingKeys is not { Length: > 0 })
        {
            Logger.LogError("Routing keys are null or empty.");
            throw new InvalidOperationException("Routing keys cannot be null or empty.");
        }

        await InitializeAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false);

        try
        {
            await Channel.ExchangeDeclareAsync(
                exchange: ExchangeName,
                type: ExchangeType.Direct,
                durable: Durable,
                autoDelete: false,
                arguments: null,
                noWait: false,
                cancellationToken: cancellationToken);

            // Declare a server-named queue
            var queueDeclareResult = await Channel.QueueDeclareAsync(cancellationToken: cancellationToken);
            QueueName = queueDeclareResult.QueueName;

            await ApplyQosAsync(Channel, cancellationToken);

            foreach (var routingKey in RoutingKeys.Distinct())
            {
                await Channel.QueueBindAsync(
                    queue: QueueName,
                    exchange: ExchangeName,
                    routingKey: routingKey,
                    arguments: null,
                    noWait: false,
                    cancellationToken: cancellationToken);
            }

            var consumer = CreateConsumer(Channel);

            ConsumerTag = await Channel.BasicConsumeAsync(
                queue: QueueName,
                autoAck: AutoAck,
                consumer: consumer,
                cancellationToken: cancellationToken);

            Logger.LogInformation("Successfully subscribed to exchange '{Exchange}' with queue '{Queue}' and routing keys '{RoutingKeys}'", ExchangeName, QueueName, string.Join(", ", RoutingKeys));
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to subscribe to exchange '{Exchange}' with routing keys '{RoutingKeys}'", ExchangeName, string.Join(", ", RoutingKeys));
            throw;
        }
    }
}
