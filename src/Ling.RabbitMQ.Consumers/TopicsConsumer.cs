using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;

namespace Ling.RabbitMQ.Consumers;

/// <summary>
/// Represents a base class for a RabbitMQ consumer that handles the topic exchange pattern.
/// </summary>
/// <typeparam name="TMessage">The type of the message.</typeparam>
public abstract class TopicsConsumer<TMessage> : RabbitMQConsumerBase<TMessage>
{
    /// <summary>
    /// Gets the topic pattern verifier.
    /// </summary>
    protected ITopicPatternVerifier TopicPatternVerifier { get; }

    /// <summary>
    /// Gets the name of the exchange.
    /// </summary>
    protected abstract string ExchangeName { get; }

    /// <summary>
    /// Gets the binding keys.
    /// </summary>
    protected abstract string[] BindingKeys { get; }

    /// <summary>
    /// Initializes a new instance of the <see cref="TopicsConsumer{TMessage}"/> class.
    /// </summary>
    /// <param name="loggerFactory">The logger factory.</param>
    /// <param name="serializer">The message serializer.</param>
    /// <param name="options">The RabbitMQ options.</param>
    /// <param name="topicPatternVerifier">The topic pattern verifier.</param>
    protected TopicsConsumer(
        ILoggerFactory loggerFactory,
        IMessageSerializer serializer,
        IOptions<RabbitMQOptions> options,
        ITopicPatternVerifier topicPatternVerifier)
        : base(loggerFactory, serializer, options)
    {
        TopicPatternVerifier = topicPatternVerifier;
    }

    /// <summary>
    /// Sets up the consumer asynchronously.
    /// </summary>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous setup operation.</returns>
    protected override async Task SetupAsync(CancellationToken cancellationToken)
    {
        if (BindingKeys is not { Length: > 0 })
        {
            Logger.LogError("Binding keys are null or empty.");
            throw new InvalidOperationException("Binding keys cannot be null or empty.");
        }

        foreach (var bindingKey in BindingKeys)
        {
            if (!TopicPatternVerifier.IsValid(bindingKey, out var errorMessage))
            {
                Logger.LogError("Invalid topic pattern for consumer: {ErrorMessage}, Exchange: {Exchange}, Topic: {Topic}",
                    errorMessage, ExchangeName, bindingKey);
                throw new InvalidOperationException($"Topic '{bindingKey}' in 'BindingKeys' is not a valid pattern.");
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

            // Declare a server-named queue
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

            Logger.LogInformation("Successfully subscribed to exchange '{Exchange}' with queue '{Queue}' and binding keys '{BindingKeys}'", ExchangeName, queueName, string.Join(", ", BindingKeys));
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to subscribe to exchange '{Exchange}' with binding keys '{BindingKeys}'", ExchangeName, string.Join(", ", BindingKeys));
            throw;
        }
    }
}
