using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace Ling.RabbitMQ.Consumers;

public abstract class RoutingConsumer<TMessage> : RabbitMQConsumerBase<TMessage>
    where TMessage : class
{
    public abstract string ExchangeName { get; }
    public abstract string[] RoutingKeys { get; }

    protected RoutingConsumer(ILoggerFactory loggerFactory, IMessageSerializer serializer, IOptions<RabbitMQOptions> options) : base(loggerFactory, serializer, options)
    {
    }

    protected override async Task SetupAsync(CancellationToken cancellationToken)
    {
        if (RoutingKeys is not { Length: > 0 })
        {
            Logger.LogError("'RoutingKeys' is null or empty.");
            throw new InvalidOperationException("'RoutingKeys' cannot be null or empty.");
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

            // declare a server-named queue
            var queueDeclareResult = await Channel.QueueDeclareAsync(cancellationToken: cancellationToken);
            var queueName = queueDeclareResult.QueueName;

            await ApplyQosAsync(Channel, cancellationToken);

            foreach (var routingKey in RoutingKeys.Distinct())
            {
                await Channel.QueueBindAsync(
                    queue: queueName,
                    exchange: ExchangeName,
                    routingKey: routingKey,
                    arguments: null,
                    noWait: false,
                    cancellationToken: cancellationToken);
            }

            var consumer = CreateConsumer(Channel);

            await Channel.BasicConsumeAsync(
                queue: queueName,
                autoAck: AutoAck,
                consumer: consumer,
                cancellationToken: cancellationToken);

            Logger.LogInformation("Subscribed to {Exchange} with queue {Queue} and routing key {RoutingKey}", ExchangeName, queueName, string.Join(", ", RoutingKeys));
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to subscribe to {Exchange} and routing key {RoutingKey}", ExchangeName, string.Join(", ", RoutingKeys));
            throw;
        }
    }
}
