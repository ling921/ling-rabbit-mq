using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace Ling.RabbitMQ.Consumers;

/// <summary>
/// Implementation of <see cref="IPubSubConsumer"/> that handles publish-subscribe pattern in RabbitMQ.
/// </summary>
public abstract class PubSubConsumer<TMessage> : RabbitMQConsumerBase<TMessage>
    where TMessage : class
{
    protected abstract string ExchangeName { get; }

    protected PubSubConsumer(ILoggerFactory loggerFactory, IMessageSerializer serializer, IOptions<RabbitMQOptions> options) : base(loggerFactory, serializer, options)
    {
    }

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

            // declare a server-named queue
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

            Logger.LogInformation("Subscribed to {Exchange} with queue {Queue}", ExchangeName, queueName);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to subscribe to {Exchange}", ExchangeName);
            throw;
        }
    }
}
