using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace Ling.RabbitMQ.Consumers;

/// <summary>
/// Represents a base class for a RabbitMQ consumer that handles work queues.
/// </summary>
/// <typeparam name="TMessage">The type of the message.</typeparam>
public abstract class WorkQueueConsumer<TMessage> : RabbitMQConsumerBase<TMessage>
{
    /// <summary>
    /// Gets the name of the queue.
    /// </summary>
    protected override abstract string QueueName { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="WorkQueueConsumer{TMessage}"/> class.
    /// </summary>
    /// <param name="loggerFactory">The logger factory.</param>
    /// <param name="serializer">The message serializer.</param>
    /// <param name="options">The RabbitMQ options.</param>
    protected WorkQueueConsumer(
        ILoggerFactory loggerFactory,
        IMessageSerializer serializer,
        IOptions<RabbitMQOptions> options)
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
            await Channel.QueueDeclareAsync(
                queue: QueueName,
                durable: Durable,
                exclusive: false,
                autoDelete: false,
                arguments: Arguments,
                passive: false,
                noWait: false,
                cancellationToken: cancellationToken);

            await ApplyQosAsync(Channel, cancellationToken);

            var consumer = CreateConsumer(Channel);

            ConsumerTag = await Channel.BasicConsumeAsync(
                queue: QueueName,
                autoAck: AutoAck,
                consumer: consumer,
                cancellationToken: cancellationToken);

            Logger.LogInformation("Successfully subscribed to queue '{Queue}'", QueueName);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to subscribe to queue '{Queue}'", QueueName);
            throw;
        }
    }
}
