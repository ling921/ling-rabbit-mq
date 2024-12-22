using Microsoft.Extensions.Logging;
using RabbitMQ.Client;

namespace Ling.RabbitMQ.Producers;

public interface ITopicsProducer
{
    /// <summary>
    /// Publishes a message to the specified topic exchange with the given topic.
    /// </summary>
    /// <typeparam name="T">The type of the message.</typeparam>
    /// <param name="exchange">The exchange name.</param>
    /// <param name="topic">The topic.</param>
    /// <param name="message">The message to publish.</param>
    /// <param name="configureProperties">Optional action to configure message properties.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous publish operation.</returns>
    Task InvokeAsync<T>(
        string exchange,
        string topic,
        T message,
        bool durable = true,
        Action<IBasicProperties>? configureProperties = null,
        CancellationToken cancellationToken = default)
        where T : class;
}

/// <summary>
/// Implementation of <see cref="ITopicsProducer"/> that handles topic exchanges in RabbitMQ.
/// </summary>
public class TopicsProducer : ProducerBase, ITopicsProducer
{
    protected virtual ITopicPatternVerifier TopicPatternVerifier { get; }

    public TopicsProducer(IConnection connection) : base(connection)
    {
        TopicPatternVerifier = new DefaultTopicPatternVerifier();
    }

    public TopicsProducer(RabbitMQOptions connectionConfig) : base(connectionConfig)
    {
        TopicPatternVerifier = new DefaultTopicPatternVerifier();
    }

    public TopicsProducer(RabbitMQOptions connectionConfig, IMessageSerializer serializer) : base(connectionConfig, serializer)
    {
        TopicPatternVerifier = new DefaultTopicPatternVerifier();
    }

    public TopicsProducer(ILoggerFactory loggerFactory, RabbitMQOptions connectionConfig, IMessageSerializer serializer) : base(loggerFactory, connectionConfig, serializer)
    {
        TopicPatternVerifier = new DefaultTopicPatternVerifier();
    }

    public TopicsProducer(ILoggerFactory loggerFactory, RabbitMQOptions connectionConfig, IMessageSerializer serializer, ITopicPatternVerifier topicPatternVerifier) : base(loggerFactory, connectionConfig, serializer)
    {
        TopicPatternVerifier = topicPatternVerifier;
    }

    /// <inheritdoc />
    public virtual async Task InvokeAsync<T>(
        string exchange,
        string topic,
        T message,
        bool durable = true,
        Action<IBasicProperties>? configureProperties = null,
        CancellationToken cancellationToken = default)
        where T : class
    {
        if (!TopicPatternVerifier.IsValid(topic, out var errorMessage))
        {
            Logger.LogWarning("Invalid topic pattern when publishing: {ErrorMessage}, Exchange: {Exchange}, Topic: {Topic}",
                errorMessage, exchange, topic);
            throw new ArgumentException(errorMessage, nameof(topic));
        }

        await InitializeAsync(cancellationToken).ConfigureAwait(continueOnCapturedContext: false);

        try
        {
            await Channel.ExchangeDeclareAsync(
                exchange: exchange,
                type: ExchangeType.Topic,
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
                routingKey: topic,
                mandatory: true,
                basicProperties: properties,
                body: body,
                cancellationToken: cancellationToken);

            Logger.LogInformation("Message published to topic exchange {Exchange} with topic {Topic}",
                exchange, topic);
        }
        catch (Exception ex)
        {
            Logger.LogError(ex, "Failed to publish message to topic exchange {Exchange} with topic {Topic}",
                exchange, topic);
            throw;
        }
    }
}
