using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace Ling.RabbitMQ.Consumers;

/// <summary>
/// Represents a consumer that reads messages from a work queue in RabbitMQ.
/// </summary>
/// <typeparam name="TMessage"></typeparam>
public abstract class WorkQueueConsumer<TMessage> : RabbitMQConsumerBase<TMessage>
    where TMessage : class
{
    /// <summary>
    /// Gets or sets the queue name.
    /// </summary>
    protected abstract string QueueName { get; }

    protected override bool AutoAck => false;

    protected WorkQueueConsumer(
        ILoggerFactory loggerFactory,
        IMessageSerializer serializer,
        IOptions<RabbitMQOptions> options)
        : base(loggerFactory, serializer, options)
    {
    }

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

            await Channel.BasicConsumeAsync(
                queue: QueueName,
                autoAck: AutoAck,
                consumer: consumer,
                cancellationToken: cancellationToken);

            Logger.LogInformation("Subscribed to queue {Queue}", QueueName);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to subscribe to {Queue}", QueueName);
            throw;
        }
    }
}
