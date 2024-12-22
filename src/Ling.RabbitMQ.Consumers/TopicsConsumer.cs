using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace Ling.RabbitMQ.Consumers;

public abstract class TopicsConsumer<TMessage> : RabbitMQConsumerBase<TMessage>
    where TMessage : class
{
    protected ITopicPatternVerifier TopicPatternVerifier { get; }

    protected abstract string ExchangeName { get; }
    protected abstract string[] BindingKeys { get; }

    protected TopicsConsumer(
        ILoggerFactory loggerFactory,
        IMessageSerializer serializer,
        IOptions<RabbitMQOptions> options,
        ITopicPatternVerifier topicPatternVerifier)
        : base(loggerFactory, serializer, options)
    {
        TopicPatternVerifier = topicPatternVerifier;
    }

    protected override async Task SetupAsync(CancellationToken cancellationToken)
    {
        if (BindingKeys is not { Length: > 0 })
        {
            Logger.LogError("'BindingKeys' is null or empty.");
            throw new InvalidOperationException("'BindingKeys' cannot be null or empty.");
        }
        foreach (var bindingKey in BindingKeys)
        {
            if (!TopicPatternVerifier.IsValid(bindingKey, out var errorMessage))
            {
                Logger.LogError("Invalid topic pattern for consumer: {ErrorMessage}, Exchange: {Exchange}, Topic: {Topic}",
                    errorMessage, ExchangeName, bindingKey);
                throw new InvalidOperationException($"Topic '{bindingKey}' in 'BindingKeys' is not a valid pattern");
            }
        }

        await InitializeAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false);

        try
        {
            await Channel.ExchangeDeclareAsync(
                exchange: ExchangeName,
                type: ExchangeType.Topic,
                durable: Durable,
                autoDelete: false,
                arguments: null,
                noWait: false,
                cancellationToken: cancellationToken);

            // declare a server-named queue
            var queueDeclareResult = await Channel.QueueDeclareAsync(cancellationToken: cancellationToken);
            var queueName = queueDeclareResult.QueueName;

            await ApplyQosAsync(Channel, cancellationToken);

            foreach (var bindingKey in BindingKeys.Distinct())
            {
                await Channel.QueueBindAsync(
                    queue: queueName,
                    exchange: ExchangeName,
                    routingKey: bindingKey,
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

            Logger.LogInformation("Subscribed to {Exchange} with queue {Queue} and routing key {RoutingKey}", ExchangeName, queueName, string.Join(", ", BindingKeys));
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to subscribe to {Exchange} and routing key {RoutingKey}", ExchangeName, string.Join(", ", BindingKeys));
            throw;
        }
    }
}
